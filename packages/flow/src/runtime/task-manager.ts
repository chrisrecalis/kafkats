/**
 * The tasks of one Run (KS `TaskManager`): the only map from assignments to tasks. It creates, resumes, suspends,
 * closes and revives tasks (4.3, 4.7.5), completes their restore pipelines (4.7.1), routes consumed records to their
 * task (4.5.1) and tells the Run when every assigned task is RUNNING (4.3.5).
 *
 * `handleAssignment`, `handleRevocation`, `handleLost`, `revive` and `closeAll` run inside the scheduler's exclusive
 * section (the caller wraps them; `resetOutput` calls back into `closeAll`). A failed run acknowledges nothing more:
 * its hooks, revivals and restore completions do nothing (fail-stop, runtime§6.1).
 */
import type { ConsumerGroupMetadata, Logger, Message, TopicPartition } from '@kafkats/client'
import { TaskMigratedError } from '@/api/errors.js'
import type { TaskId } from '@/processor/api.js'
import type { TaskOutput } from '@/processor/output.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import { planAssignment } from '@/runtime/assignment.js'
import type { CommitOutcome, CommitReason } from '@/runtime/commit.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { classify, errorMessage, type FailureSite } from '@/runtime/errors.js'
import type { AssignedPartition, Clock, GroupConsumerPort } from '@/runtime/io.js'
import {
	RestorePipelines,
	type Restored,
	type StartPositionSettler,
	type TaskRestorer,
} from '@/runtime/restore-pipelines.js'
import { cap, raceSignal } from '@/runtime/signals.js'
import { TaskIdRegistry, tpKey } from '@/runtime/task-id.js'
import { StreamTask, type TaskConfig, type TaskState } from '@/runtime/task.js'
import type { StateStore } from '@/state.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { BuiltTopology, Subtopology } from '@/topology/built-topology.js'
import type { PartitionCounts } from '@/topology/nodes.js'

export type { StartPositionSettler, TaskRestorer } from '@/runtime/restore-pipelines.js'

export interface TaskManagerDeps {
	readonly topology: BuiltTopology
	readonly counts: PartitionCounts
	readonly ids: TaskIdRegistry
	readonly config: ResolvedFlowConfig
	readonly stateDirectory: StateDirectory
	readonly checkpoints: Checkpoints
	/** The read-only view of a global store, which every processor may read (TaskDeps.globalStore). */
	readonly globalStore: (name: string) => StateStore
	readonly consumer: Pick<GroupConsumerPort, 'pause' | 'resume' | 'seek' | 'groupMetadata' | 'nextOffset'>
	/** The Run's current collector: per-task outputs, and (EOS) an open transaction before a task's init() (4.4). */
	readonly outputs: { forTask(task: StreamTask): TaskOutput; ensureTransaction(signal: AbortSignal): Promise<void> }
	readonly restorer: TaskRestorer
	readonly startPositions: StartPositionSettler
	readonly exclusive: <T>(fn: () => Promise<T>) => Promise<T>
	readonly commit: (reason: CommitReason, signal: AbortSignal) => Promise<CommitOutcome>
	/** Run.resetOutput: abort or discard, `closeAll('dirty')` of this manager, a new collector (4.3.1, 4.3.3). */
	readonly resetOutput: () => Promise<void>
	readonly onWork: (task: StreamTask) => void
	/** scheduler.requestCommit; becomes TaskDeps.onCommitRequested. */
	readonly requestCommit: () => void
	/** false at every assignment, true once every assigned task is RUNNING: lifecycle and prune timer (4.3.5). */
	readonly onAllRunningChanged: (allRunning: boolean) => void
	/** The Run's `failed` flag. */
	readonly failed: () => boolean
	readonly onFailure: (e: unknown, site: FailureSite) => void
	/** The Run's hard stop: bounds the revocation commit (3.8), which a close must not cut short. */
	readonly hardStop: AbortSignal
	/** AbortSignal.any([run.hardStop, runtime.shutdown]): aborts every restore pipeline. */
	readonly signal: AbortSignal
	readonly clock: Clock
	readonly logger: Logger
}

/** Bound of the transaction a task's init() may write into (EOS). */
const TRANSACTION_TIMEOUT_MS = 10_000
const REVOCATION_COMMIT_MAX_MS = 30_000
/** A task corrupted again within this window escalates to a migration (4.7.5). */
const CORRUPTION_WINDOW_MS = 60_000

interface Waiter {
	resolve(): void
	reject(error: unknown): void
}

export class TaskManager {
	private readonly byId = new Map<TaskId, StreamTask>()
	private readonly byPartition = new Map<string, StreamTask>()
	private ordered: readonly StreamTask[] | undefined
	private readonly pipelines: RestorePipelines
	private readonly revivedAt = new Map<TaskId, number>()
	private readonly waiters = new Set<Waiter>()
	private readonly taskConfig: TaskConfig
	private metadata: ConsumerGroupMetadata | undefined
	private running = false
	private assigning = false
	/** Assignments that completed without a failure: only they count for allRunning (CR14). */
	private assignments = 0
	private failure: { readonly error: unknown } | undefined

	constructor(private readonly deps: TaskManagerDeps) {
		this.taskConfig = taskConfig(deps.config, deps.topology)
		this.pipelines = new RestorePipelines({
			restorer: deps.restorer,
			startPositions: deps.startPositions,
			exclusive: deps.exclusive,
			signal: deps.signal,
			complete: (task, signal, restored) => this.complete(task, signal, restored),
			onFailure: error => this.fail(error, 'restore'),
		})
	}

	/** TaskId order. */
	tasks(): readonly StreamTask[] {
		this.ordered ??= [...this.byId.values()].sort((a, b) => TaskIdRegistry.compare(a.id, b.id))
		return this.ordered
	}

	taskFor(tp: TopicPartition): StreamTask | undefined {
		return this.byPartition.get(tpKey(tp))
	}

	/** The generation the current assignment belongs to (EOS offsets are fenced by it, CR1). */
	get assignmentMetadata(): ConsumerGroupMetadata | undefined {
		return this.metadata
	}

	get allRunning(): boolean {
		return this.running
	}

	/**
	 * The consumer's handler (4.5.1): queues the records in their task and pauses a partition whose queue just grew past
	 * `bufferedRecordsPerPartition` (the scheduler resumes it). Records of a partition without a task are dropped.
	 */
	enqueue(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): void {
		const task = this.taskFor(tp)
		if (task === undefined) {
			this.deps.logger.debug('dropped records of a partition without a task', { ...tp, count: messages.length })
			return
		}
		const size = task.addRecords(tp, messages)
		const limit = this.taskConfig.bufferedRecordsPerPartition
		if (size > limit && size - messages.length <= limit) {
			this.deps.consumer.pause([tp])
		}
		this.deps.onWork(task)
	}

	/**
	 * Eager assignment (4.3.2): closes the tasks it does not keep, pauses the partitions of every task that is not
	 * RUNNING, resumes suspended tasks with the same partitions, creates the others and starts their restore pipelines
	 * (resumed tasks too, KF3). A task RUNNING or restoring with the same partitions keeps going (the TestDriver's
	 * cumulative assignments). A failure is reported, never thrown.
	 */
	async handleAssignment(assigned: readonly AssignedPartition[]): Promise<void> {
		if (this.deps.failed()) {
			return
		}
		this.running = false
		this.deps.onAllRunningChanged(false)
		this.metadata = this.deps.consumer.groupMetadata()
		this.assigning = true
		try {
			const plan = planAssignment(assigned, this.deps.topology, this.deps.counts, this.deps.ids)
			for (const task of this.tasks()) {
				const next = plan.get(task.id)
				if (next === undefined || !samePartitions(task.inputPartitions, next.partitions)) {
					await this.closeTask(task, task.state === 'SUSPENDED' ? 'clean' : 'dirty')
				}
			}
			// Before any fetch: the client holds new partitions until this hook returns (runtime§6.30).
			const held = [...plan].filter(([id]) => this.byId.get(id)?.state !== 'RUNNING')
			if (held.length > 0) {
				this.deps.consumer.pause(held.flatMap(([, next]) => next.partitions))
			}
			for (const [id, next] of plan) {
				const task = this.byId.get(id) ?? (await this.create(id, next.subtopology, next.partitions))
				if (task.state === 'SUSPENDED') {
					task.resume()
				}
				if (task.state === 'RESTORING' && !this.pipelines.running(task)) {
					task.setStarts(next.starts)
					this.pipelines.start(task)
				}
			}
			this.assignments++
		} catch (error) {
			this.fail(error, 'assignment')
		} finally {
			this.assigning = false
		}
		this.maybeAllRunning()
	}

	/**
	 * Revocation (4.3.1): commits every task (the coordinator answers 'nothing' when none needs it), then suspends the
	 * tasks of the revoked partitions. A failed commit resets the output (every task closes dirty) and lets the
	 * rebalance proceed: the next assignment re-creates the tasks from the last commit. Only a fatal one is reported.
	 */
	async handleRevocation(revoked: readonly TopicPartition[]): Promise<void> {
		if (this.deps.failed()) {
			return
		}
		const bound = Math.min(this.deps.config.rebalanceTimeoutMs / 2, REVOCATION_COMMIT_MAX_MS)
		try {
			await this.deps.commit('revocation', cap(this.deps.hardStop, bound))
		} catch (error) {
			await this.deps.resetOutput()
			if (classify(error, 'commit') === 'fatal') {
				this.fail(error, 'commit')
			} else {
				this.deps.logger.warn('the revocation commit failed: the tasks restart from the last commit', {
					error: errorMessage(error),
				})
			}
			return
		}
		const keys = new Set(revoked.map(tpKey))
		const failures: unknown[] = []
		for (const task of this.tasks()) {
			if (task.state !== 'SUSPENDED' && holds(task, keys)) {
				await this.pipelines.stop(task)
				await task.suspend().catch((error: unknown) => failures.push(error))
			}
		}
		this.changeAllRunning(false)
		if (failures.length > 0) {
			this.fail(failures[0], 'assignment')
		}
	}

	/** Synchronous part of the lost hook: no further record, punctuation or send of these tasks (W5). */
	markLost(lost: readonly TopicPartition[]): void {
		const keys = new Set(lost.map(tpKey))
		for (const task of this.tasks()) {
			if (holds(task, keys)) {
				task.markLost()
			}
		}
	}

	/** Loss (4.3.3, KS handleLostAll): the output is reset, which closes every task dirty. Idempotent. */
	async handleLost(lost: readonly TopicPartition[]): Promise<void> {
		const keys = new Set(lost.map(tpKey))
		if (!this.deps.failed() && this.tasks().some(task => task.lost || holds(task, keys))) {
			await this.deps.resetOutput()
		}
	}

	/**
	 * Closes the tasks dirty and re-creates them in place without leaving the group (4.7.5, KS closeDirtyAndRevive):
	 * from their last commit, partitions paused and rewound to it, through the restore pipeline. A task corrupted again
	 * within 60 s escalates to a migration. Tasks replaced or suspended meanwhile are skipped.
	 */
	async revive(tasks: readonly StreamTask[], options: { wipe: boolean }): Promise<void> {
		if (this.deps.failed()) {
			return
		}
		const now = this.deps.clock.now()
		const current = tasks.filter(task => this.byId.get(task.id) === task && task.state !== 'SUSPENDED')
		const again = current.find(task => now - (this.revivedAt.get(task.id) ?? -Infinity) < CORRUPTION_WINDOW_MS)
		if (again !== undefined) {
			const window = CORRUPTION_WINDOW_MS / 1000
			this.fail(
				new TaskMigratedError(`task ${again.id.key} was corrupted twice within ${window} s`),
				'assignment'
			)
			return
		}
		try {
			for (const task of current) {
				this.revivedAt.set(task.id, now)
				await this.reviveTask(task, options.wipe)
			}
		} catch (error) {
			this.fail(error, 'assignment')
		}
	}

	/**
	 * Closes every task: 'clean' suspends a task first (a lost one closes dirty); a task whose clean close fails closes
	 * dirty, and the first such failure is thrown once all are closed. 'dirty' never throws. Waits for each close until
	 * `signal` aborts; the rest then close in the background.
	 */
	async closeAll(mode: 'clean' | 'dirty', signal: AbortSignal): Promise<void> {
		const tasks = this.tasks()
		this.changeAllRunning(false)
		const failures: unknown[] = []
		for (const task of tasks) {
			await raceSignal(this.closeTask(task, mode), signal).catch((error: unknown) => {
				if (!signal.aborted) failures.push(error)
			})
		}
		if (signal.aborted) {
			this.deps.logger.warn('closing the tasks did not finish in time: the rest close in the background')
		}
		if (failures.length > 0) {
			throw failures[0]
		}
	}

	/**
	 * Resolves once at least one assignment completed and every task is RUNNING. Rejects with the first failure this
	 * manager reported that fails the run (assignment, restore, revocation, revival), or with the signal's reason when
	 * it aborts (the runtime aborts it with its own failures and the start timeout).
	 */
	whenAllRunning(signal: AbortSignal): Promise<void> {
		if (this.failure !== undefined) {
			return Promise.reject(this.failure.error)
		}
		if (this.running) {
			return Promise.resolve()
		}
		if (signal.aborted) {
			return Promise.reject(signal.reason)
		}
		let waiter!: Waiter
		const running = new Promise<void>((resolve, reject) => {
			waiter = { resolve, reject }
		})
		const onAbort = () => waiter.reject(signal.reason)
		signal.addEventListener('abort', onAbort, { once: true })
		this.waiters.add(waiter)
		return running.finally(() => {
			this.waiters.delete(waiter)
			signal.removeEventListener('abort', onAbort)
		})
	}

	// ---------------------------------------------------------------------------------------------------------

	/** A new task, owned from now on, initialized (CREATED -> RESTORING: its stores are leased). */
	private async create(
		id: TaskId,
		subtopology: Subtopology,
		inputPartitions: readonly TopicPartition[]
	): Promise<StreamTask> {
		const { deps } = this
		const task = new StreamTask(
			{
				subtopology,
				config: this.taskConfig,
				stateDirectory: deps.stateDirectory,
				checkpoints: deps.checkpoints,
				globalStore: deps.globalStore,
				outputFor: owner => deps.outputs.forTask(owner),
				clock: deps.clock,
				logger: deps.logger,
				onCommitRequested: deps.requestCommit,
			},
			id,
			inputPartitions
		)
		this.byId.set(id, task)
		for (const tp of inputPartitions) {
			this.byPartition.set(tpKey(tp), task)
		}
		this.ordered = undefined
		await task.initialize()
		return task
	}

	private forget(task: StreamTask): void {
		if (this.byId.get(task.id) !== task) {
			return
		}
		this.byId.delete(task.id)
		for (const tp of task.inputPartitions) {
			this.byPartition.delete(tpKey(tp))
		}
		this.ordered = undefined
	}

	/** Forgets the task, stops its pipeline and closes it. A failed clean close closes dirty and is thrown. */
	private async closeTask(task: StreamTask, mode: 'clean' | 'dirty'): Promise<void> {
		this.forget(task)
		await this.pipelines.stop(task)
		if (mode === 'dirty' || task.lost) {
			await task.close('dirty')
			return
		}
		if (task.state !== 'SUSPENDED') {
			try {
				await task.suspend()
			} catch (error) {
				await task.close('dirty')
				throw error
			}
		}
		await task.close('clean')
	}

	private async reviveTask(task: StreamTask, wipe: boolean): Promise<void> {
		const { consumer } = this.deps
		// First: nothing more of these partitions reaches either incarnation (C5).
		consumer.pause(task.inputPartitions)
		this.changeAllRunning(false)
		await this.pipelines.stop(task)
		const starts = task.restartStarts()
		this.forget(task)
		await task.close('dirty', { wipe })
		const revived = await this.create(task.id, task.subtopology, task.inputPartitions)
		revived.setStarts(starts)
		for (const tp of revived.inputPartitions) {
			const start = starts.get(tpKey(tp))
			if (start !== undefined) {
				consumer.seek(tp, start.resolved)
			}
		}
		this.pipelines.start(revived)
	}

	/** A pipeline's completion op: RESTORING -> RUNNING, then its partitions resume (4.7.1). */
	private async complete(task: StreamTask, signal: AbortSignal, restored: Restored): Promise<void> {
		if (!this.completes(task, signal, 'RESTORING')) {
			return
		}
		task.setSettledPositions(restored.positions)
		if (this.taskConfig.eos) {
			// init() may write logged stores (4.4).
			await this.deps.outputs.ensureTransaction(cap(signal, TRANSACTION_TIMEOUT_MS))
			if (!this.completes(task, signal, 'RESTORING')) {
				return
			}
		}
		await task.completeRestoration(restored.storeTimes)
		if (!this.completes(task, signal, 'RUNNING')) {
			return
		}
		this.deps.consumer.resume(task.inputPartitions)
		this.deps.onWork(task)
		this.maybeAllRunning()
	}

	/** Same incarnation in the expected state, pipeline not stopped, run neither failed nor stopping (runtime§6.30). */
	private completes(task: StreamTask, signal: AbortSignal, state: TaskState): boolean {
		return !signal.aborted && !this.deps.failed() && this.byId.get(task.id) === task && task.state === state
	}

	private maybeAllRunning(): void {
		if (!this.assigning && this.assignments > 0 && this.tasks().every(task => task.state === 'RUNNING')) {
			this.changeAllRunning(true)
		}
	}

	private changeAllRunning(running: boolean): void {
		if (this.running === running) {
			return
		}
		this.running = running
		this.deps.onAllRunningChanged(running)
		if (running) {
			for (const waiter of this.waiters) waiter.resolve()
		}
	}

	/** Reports a failure. The first one that fails the run (a corruption is repaired in place) rejects whenAllRunning. */
	private fail(error: unknown, site: FailureSite): void {
		if (this.failure === undefined && classify(error, site) !== 'corrupted') {
			this.failure = { error }
			for (const waiter of this.waiters) waiter.reject(error)
		}
		this.deps.onFailure(error, site)
	}
}

/** The tasks' view of the config: repartition sources always extract with failOnInvalid (6.17). */
function taskConfig(config: ResolvedFlowConfig, topology: BuiltTopology): TaskConfig {
	return {
		applicationId: config.applicationId,
		eos: config.eos,
		extractor: topic => {
			const source = topology.subtopologyOf(topic)?.sourcesByTopic.get(topic)?.[0]
			const fallback =
				source?.internal === 'repartition'
					? TimestampExtractors.failOnInvalid
					: config.defaultTimestampExtractor
			return source?.timestampExtractor ?? fallback
		},
		maxTaskIdleMs: config.maxTaskIdleMs,
		bufferedRecordsPerPartition: config.bufferedRecordsPerPartition,
		emitIntervalMs: config.emitIntervalMs,
		handlers: config.handlers,
	}
}

function holds(task: StreamTask, keys: ReadonlySet<string>): boolean {
	return task.inputPartitions.some(tp => keys.has(tpKey(tp)))
}

function samePartitions(a: readonly TopicPartition[], b: readonly TopicPartition[]): boolean {
	const keys = new Set(a.map(tpKey))
	return a.length === b.length && b.every(tp => keys.has(tpKey(tp)))
}
