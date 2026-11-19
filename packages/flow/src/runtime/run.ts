/**
 * One group membership of the app (3.11, SI1): a group consumer, a producer and its RecordCollector, the
 * CommitCoordinator, the TaskManager and the Scheduler, wired to each other and to the consumer's rebalance hooks
 * (4.3). Created by FlowRuntime.start and by every recovery; never revived: a Run ends once, gracefully (close) or
 * abandoned (failure). Corrupted tasks are revived in place within it (4.7.5).
 */
import { NoOffsetForPartitionError, type Logger } from '@kafkats/client'
import { NoOffsetResetError, StreamsError, type TaskCorruptedError } from '@/api/errors.js'
import type { AppLifecycle } from '@/app/lifecycle.js'
import { CommitCoordinator } from '@/runtime/commit.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { errorMessage, type FailureSite } from '@/runtime/errors.js'
import type { GroupConsumerHooks, GroupConsumerPort, ProducerPort, RuntimeIO } from '@/runtime/io.js'
import { SinkPartitionCounts } from '@/runtime/partition-count-cache.js'
import { PruneTimer } from '@/runtime/prune-timer.js'
import { RepartitionPurger } from '@/runtime/purge.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import { RestorationPool } from '@/runtime/restoration.js'
import { Scheduler } from '@/runtime/scheduler.js'
import { cap, raceSignal } from '@/runtime/signals.js'
import type { StartPositions } from '@/runtime/start-positions.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import { TaskManager } from '@/runtime/task-manager.js'
import type { StateStore } from '@/state.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { PartitionCounts } from '@/topology/nodes.js'

/** How long an exactly-once abort may take (a reset, a dirty close). */
const ABORT_TIMEOUT_MS = 5000
/** How long the end of a run waits for the slices in flight, and then for its turn among the exclusive ops. */
const SETTLE_TIMEOUT_MS = 5000
const EXCLUSIVE_WAIT_MS = 5000
const CLOSE_COMMIT_TIMEOUT_MS = 10_000
/** The client leaves the group within its own bound; past this the run stops waiting for it. */
const CONSUMER_STOP_TIMEOUT_MS = 7000
const PRODUCER_CLOSE_TIMEOUT_MS = 5000
const PRELOAD_TIMEOUT_MS = 10_000

export interface RunDeps {
	readonly topology: BuiltTopology
	readonly counts: PartitionCounts
	readonly config: ResolvedFlowConfig
	readonly io: RuntimeIO
	readonly stateDirectory: StateDirectory
	readonly checkpoints: Checkpoints
	/** The start's 'latest' pins (4.7.4): they outlive the run. */
	readonly startPositions: StartPositions
	/** The app state follows rebalances and restores (4.3.5). */
	readonly lifecycle: Pick<AppLifecycle, 'setRunning' | 'setRebalancing'>
	/** The read-only view of a global store (GlobalStateManager.store). */
	readonly globalStore: (name: string) => StateStore
	/** Stream processing waits for a global table loaded again (FlowRuntime.processingPaused, 4.12). */
	readonly paused: () => boolean
	/** FlowRuntime.fail for this run. */
	readonly onFailure: (error: unknown, site: FailureSite) => void
	/** The runtime's shutdown: aborts the restore pipelines with the close. */
	readonly shutdown: AbortSignal
	readonly logger: Logger
}

export class Run {
	readonly consumer: GroupConsumerPort
	readonly tasks: TaskManager
	readonly scheduler: Scheduler
	readonly commits: CommitCoordinator
	/** Aborted at the close deadline: every network await of this run rejects (CR2, SI7). */
	readonly hardStop = new AbortController()
	private producer: ProducerPort
	private current: RecordCollector
	private readonly partitionCounts: SinkPartitionCounts
	private readonly pruneTimer: PruneTimer
	private isClosed = false
	private isFailed = false
	private hasAssignment = false
	/** The run asked its consumer to stop: the end of its run is expected. */
	private stopping = false
	private ending: Promise<void> | undefined

	constructor(private readonly deps: RunDeps) {
		const { io, config, logger } = deps
		this.consumer = io.consumer()
		this.producer = io.producer()
		this.partitionCounts = new SinkPartitionCounts({
			producer: () => this.producer,
			clock: io.clock,
			signal: this.hardStop.signal,
			logger,
		})
		this.current = this.newCollector()
		this.commits = new CommitCoordinator({
			consumer: this.consumer,
			collector: () => this.current,
			tasks: () => this.tasks.tasks(),
			assignmentMetadata: () => this.tasks.assignmentMetadata,
			eos: config.eos,
			checkpoints: deps.checkpoints,
			onCommitted: new RepartitionPurger({
				admin: io.admin,
				topics: new Set(deps.topology.subtopologies.flatMap(s => [...s.repartitionSourceTopics])),
				intervalMs: config.repartitionPurgeIntervalMs,
				clock: io.clock,
				signal: this.hardStop.signal,
				logger,
			}),
			failed: () => this.isFailed,
			hardStop: this.hardStop.signal,
			config,
			clock: io.clock,
			logger,
		})
		this.tasks = new TaskManager({
			topology: deps.topology,
			counts: deps.counts,
			ids: new TaskIdRegistry(),
			config,
			stateDirectory: deps.stateDirectory,
			checkpoints: deps.checkpoints,
			globalStore: deps.globalStore,
			consumer: this.consumer,
			outputs: {
				forTask: task => this.current.forTask(task),
				initialize: signal => this.current.initialize(signal),
				ensureTransaction: signal => this.current.ensureTransaction(signal),
			},
			restorer: new RestorationPool({
				io,
				checkpoints: deps.checkpoints,
				options: config.restoration,
				applicationId: config.applicationId,
				commitIntervalMs: config.commitIntervalMs,
				logger,
			}),
			startPositions: deps.startPositions.settler(this.consumer),
			exclusive: fn => this.scheduler.exclusive(fn),
			commit: (reason, signal) => this.commits.commit(reason, signal),
			resetOutput: () => this.resetOutput(),
			onWork: task => this.scheduler.markWork(task),
			requestCommit: () => this.scheduler.requestCommit(),
			onAllRunningChanged: running => (running ? this.allRunning() : this.rebalancing()),
			failed: () => this.isFailed,
			onFailure: (error, site) => this.fail(error, site),
			hardStop: this.hardStop.signal,
			signal: AbortSignal.any([this.hardStop.signal, deps.shutdown]),
			clock: io.clock,
			logger,
		})
		this.scheduler = new Scheduler({
			tasks: this.tasks,
			commits: this.commits,
			collector: () => this.current,
			consumer: this.consumer,
			clock: io.clock,
			config,
			paused: deps.paused,
			failed: () => this.isFailed,
			hardStop: this.hardStop.signal,
			onFailure: (error, site) => this.fail(error, site),
			logger,
		})
		this.pruneTimer = new PruneTimer({
			stateDirectory: deps.stateDirectory,
			clock: io.clock,
			signal: this.hardStop.signal,
			logger,
		})
	}

	/** The output of the current producer incarnation; replaced only by resetOutput(). */
	get collector(): RecordCollector {
		return this.current
	}

	/** Its hooks return at once: abandoned, or its tasks closed by the close. */
	get closed(): boolean {
		return this.isClosed
	}

	/** Set by FlowRuntime.fail: nothing of this run is processed or committed any more (fail-stop, runtime§6.1). */
	get failed(): boolean {
		return this.isFailed
	}

	/** An assignment arrived (the start timeout counts until then). */
	get assigned(): boolean {
		return this.hasAssignment
	}

	markFailed(): void {
		this.isFailed = true
		this.scheduler.wake()
	}

	/**
	 * Closes the corrupted tasks dirty and re-creates them from their last commit, without a rebalance (4.7.5, KS
	 * `closeDirtyAndRevive`); `rebuild`: their logged stores are rebuilt from the changelogs (a restore that found its
	 * records gone). The other tasks go on.
	 */
	revive(error: TaskCorruptedError, options: { rebuild: boolean }): void {
		const ids = new Set(error.tasks)
		const tasks = this.tasks.tasks().filter(task => ids.has(task.id))
		if (tasks.length === 0) {
			return
		}
		this.deps.logger.warn('tasks are revived from their last commit', {
			tasks: tasks.map(task => task.id.key),
			rebuild: options.rebuild,
			...errorFields(error),
		})
		this.scheduler
			.exclusive(() => this.tasks.revive(tasks, options))
			.catch((failure: unknown) => this.fail(failure, 'assignment'))
	}

	/**
	 * The gate of a global table loaded again (4.12 step 3): inside exclusive, a commit of the RUNNING tasks (skipped
	 * while a rebalance is pending; a failure fails the run), then `pause()`, so that no slice starts in between.
	 */
	commitAndPause(pause: () => void): Promise<void> {
		return this.scheduler.exclusive(async () => {
			if (this.acknowledges) {
				await this.commits
					.commit('user', cap(this.hardStop.signal, CLOSE_COMMIT_TIMEOUT_MS))
					.catch((error: unknown) => this.fail(error, 'commit'))
			}
			pause()
		})
	}

	/** Joins the group: the consumer's handler enqueues, its hooks run the TaskManager inside exclusive (4.3). */
	start(): void {
		const { consumer, deps } = this
		consumer.on('rebalancePending', () => this.rebalancing())
		consumer.on('error', error => deps.logger.warn('the group consumer reported an error', errorFields(error)))
		consumer.on('offsetReset', ({ reason, topic, partition, from, to }) => {
			// Offsets as strings: the client's logger writes JSON, which has no bigint.
			deps.logger[reason === 'out-of-range' ? 'warn' : 'info'](
				`a partition starts at its reset position (${reason})`,
				{ topic, partition, from: from?.toString() ?? null, to: to.toString() }
			)
		})
		consumer.on('offsetOutOfRange', ({ topic, partition, offset }) => {
			const message = `'${topic}' partition ${partition} has no record at offset ${offset} and its reset policy is 'none'`
			this.fail(new NoOffsetResetError(message), 'consumer-run')
		})
		this.preloadPartitionCounts()
		this.scheduler.start()
		const topics = [...deps.topology.groupTopics]
		consumer
			.run(topics, (tp, messages) => this.tasks.enqueue(tp, messages), this.hooks())
			.then(
				() => this.consumerEnded(new StreamsError('the group consumer stopped')),
				(error: unknown) => this.consumerEnded(error)
			)
	}

	/**
	 * Inside exclusive: abort (EOS) or discard (ALOS) the output, close every task dirty, then continue on a new
	 * collector (EOS: on a new producer with the same transactional id, whose first transaction fences the old one;
	 * the old producer closes in the background). After a loss or a failed revocation commit (4.3.1, 4.3.3). A run
	 * that failed or closed meanwhile keeps its output: its end closes it.
	 */
	async resetOutput(): Promise<void> {
		await this.closeDirty(this.hardStop.signal)
		if (!this.acknowledges) {
			return
		}
		this.current.close()
		if (this.deps.config.eos) {
			const replaced = this.producer
			this.producer = this.deps.io.producer()
			void this.closeProducer(replaced, this.hardStop.signal)
		}
		this.current = this.newCollector()
	}

	/** Ends the run after a failure: every task closed dirty, consumer and producer closed. Memoized. */
	abandon(signal: AbortSignal): Promise<void> {
		this.isClosed = true
		this.ending ??= this.end('dirty', signal)
		return this.ending
	}

	/**
	 * close() (4.11 step 4): commit and close every task clean unless the run failed (then dirty), then stop the
	 * consumer and close the producer, bounded by `signal`. On an abandoned run it awaits the abandon.
	 */
	closeGracefully(signal: AbortSignal): Promise<void> {
		this.ending ??= this.end('clean', signal)
		return this.ending
	}

	// ---------------------------------------------------------------------------------------------------------

	private hooks(): GroupConsumerHooks {
		return {
			onPartitionsRevoked: partitions => this.inHook(() => this.tasks.handleRevocation(partitions)),
			onPartitionsAssigned: partitions => {
				this.hasAssignment = true
				return this.inHook(() => this.tasks.handleAssignment(partitions))
			},
			onPartitionsLost: partitions => {
				// Synchronously, before any other code: no record, punctuation or send of these tasks any more (W5).
				this.tasks.markLost(partitions)
				return this.inHook(() => this.tasks.handleLost(partitions))
			},
		}
	}

	/** A rebalance step inside exclusive; a closed or failed run acknowledges nothing more (4.3, CR11). */
	private async inHook(step: () => Promise<void>): Promise<void> {
		if (!this.acknowledges) {
			return
		}
		try {
			await this.scheduler.exclusive(async () => {
				if (this.acknowledges) await step()
			})
		} catch (error) {
			this.fail(error, 'assignment')
		}
	}

	private get acknowledges(): boolean {
		return !this.isClosed && !this.isFailed
	}

	/** Every assigned task is RUNNING (4.3.5): the app state follows, and the unowned state is pruned later. */
	private allRunning(): void {
		this.deps.lifecycle.setRunning()
		this.pruneTimer.arm()
	}

	/** A rebalance is pending or a task left RUNNING (assignment, revocation, revival). */
	private rebalancing(): void {
		this.deps.lifecycle.setRebalancing()
		this.pruneTimer.disarm()
	}

	/**
	 * A failed run reports nothing more: what fails in it later (an op that outlived its end, a commit the end voided)
	 * follows from the first failure or from the end.
	 */
	private fail(error: unknown, site: FailureSite): void {
		if (this.isFailed) {
			this.deps.logger.debug('a later failure of a failed run is ignored', { site, ...errorFields(error) })
			return
		}
		this.deps.onFailure(error, site)
	}

	/**
	 * The consumer's run ended or failed; expected only once the run stops it (4.10.1). A partition without a committed
	 * offset under reset policy 'none' is flow's NoOffsetResetError, like an out-of-range position (KS wraps both
	 * halves of `InvalidOffsetException` in one StreamsException).
	 */
	private consumerEnded(error: unknown): void {
		if (this.stopping) {
			return
		}
		if (error instanceof NoOffsetForPartitionError) {
			const message = `'${error.topic}' partition ${error.partition} has no committed offset and its reset policy is 'none'`
			this.fail(new NoOffsetResetError(message, { cause: error }), 'consumer-run')
			return
		}
		this.fail(error, 'consumer-run')
	}

	/** Static sink topics, so that partitioners read their counts synchronously from the first record (best effort). */
	private preloadPartitionCounts(): void {
		for (const topic of this.deps.topology.staticSinkTopics) {
			this.partitionCounts.load(topic, cap(this.hardStop.signal, PRELOAD_TIMEOUT_MS)).catch((error: unknown) => {
				this.deps.logger.debug('preloading a partition count failed: loaded on first use', {
					topic,
					...errorFields(error),
				})
			})
		}
	}

	/**
	 * The end of the run (4.10.2 step 2, 4.11 step 4): no new slice; once the slices in flight settled, the tasks
	 * close inside exclusive (clean after the close commit, or dirty); a slice that did not finish its record in time
	 * fails the run and the tasks close dirty without waiting for it (its later sends are dropped, CR9). Then,
	 * outside exclusive (the client's stop awaits the hooks, 4.3), the consumer stops and the producer closes.
	 */
	private async end(mode: 'clean' | 'dirty', signal: AbortSignal): Promise<void> {
		this.stopping = true
		this.pruneTimer.disarm()
		const settle = cap(signal, SETTLE_TIMEOUT_MS)
		await this.scheduler.stop(settle)
		let closing: Promise<void> | undefined
		const closeTasks = () => (closing ??= this.closeTasks(mode, signal))
		if (!settle.aborted) {
			// Bounds the wait for its turn: an op that hangs must not hold the close up (it runs once, either way).
			await raceSignal(this.scheduler.exclusive(closeTasks), cap(signal, EXCLUSIVE_WAIT_MS)).catch(() => {})
		}
		if (closing === undefined) {
			// A slice or an op did not finish in time: nothing is committed beside it, the tasks close dirty (4.11 a).
			this.isFailed = true
		}
		await closeTasks()
		await this.consumer.stop({ signal: cap(signal, CONSUMER_STOP_TIMEOUT_MS) }).catch((error: unknown) => {
			this.deps.logger.warn('the group consumer did not stop in time', errorFields(error))
		})
		this.current.close()
		await this.closeProducer(this.producer, signal)
	}

	/** Clean: the close commit, then every task closed clean; if that fails, or the run failed, every task dirty. */
	private async closeTasks(mode: 'clean' | 'dirty', signal: AbortSignal): Promise<void> {
		this.isClosed = true
		if (mode === 'clean' && !this.isFailed) {
			try {
				await this.commits.commit('close', cap(signal, CLOSE_COMMIT_TIMEOUT_MS))
				await this.tasks.closeAll('clean', signal)
				return
			} catch (error) {
				this.deps.logger.warn(
					'closing the tasks cleanly failed: their uncommitted work is redone',
					errorFields(error)
				)
			}
		}
		await this.closeDirty(signal)
	}

	/** Abort (EOS: buffered store writes dropped, stores keep committed state) or discard (ALOS), then close all dirty. */
	private async closeDirty(signal: AbortSignal): Promise<void> {
		if (this.deps.config.eos) {
			await this.current.abortTransaction(cap(signal, ABORT_TIMEOUT_MS))
		} else {
			this.current.discard()
		}
		await this.tasks.closeAll('dirty', signal)
	}

	private closeProducer(producer: ProducerPort, signal: AbortSignal): Promise<void> {
		return producer.close({ signal: cap(signal, PRODUCER_CLOSE_TIMEOUT_MS) }).catch((error: unknown) => {
			this.deps.logger.warn('the producer did not close in time', errorFields(error))
		})
	}

	/**
	 * The collector of the current producer. Exactly-once, its initialization starts at once, in the background, while
	 * the consumer joins (KS initializes the producer at task initialization, before it reads any committed offset):
	 * its epoch bump aborts a transaction the previous incarnation left open, whose pending offsets would hold the
	 * assignment's offset fetch (requireStable) until the broker timed it out. Restore pipelines await it.
	 */
	private newCollector(): RecordCollector {
		const { config, logger } = this.deps
		const collector = new RecordCollector(this.producer, {
			eos: config.eos,
			handlers: config.handlers,
			counts: this.partitionCounts,
			taskTimeoutMs: config.taskTimeoutMs,
			clock: this.deps.io.clock,
			logger,
		})
		// A failure is the pipelines' to report: their initialize() tries again.
		collector.initialize(this.hardStop.signal).catch(() => {})
		return collector
	}
}

function errorFields(error: unknown): { error: string } {
	return { error: errorMessage(error) }
}
