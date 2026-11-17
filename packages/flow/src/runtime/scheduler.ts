/**
 * The processing loop of one Run (4.13; KS `StreamThread.runOnce`, whose poll the consumer's callbacks replace): task
 * slices, interval and user commits, exclusive ops. Commits and exclusive ops are barriers: each starts once every
 * slice in flight settled, and no slice starts while it runs, so the sends of a record and its offset always land in
 * the same transaction. Between barriers the slices of different tasks overlap (a task whose records await slow I/O
 * does not hold the others up); the records of one task stay sequential, one slice per task at a time. Timers wake the
 * loop for the commit interval, wall-clock punctuations and idle deadlines.
 */
import type { Logger, TopicPartition } from '@kafkats/client'
import { isThenable } from '@/processor/node.js'
import type { CommitCoordinator, CommitOutcome, CommitReason } from '@/runtime/commit.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import type { FailureSite } from '@/runtime/errors.js'
import type { Clock, GroupConsumerPort } from '@/runtime/io.js'
import type { RecordCollector } from '@/runtime/record-collector.js'
import { cap, raceSignal, yieldIfDue } from '@/runtime/signals.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import type { TaskManager } from '@/runtime/task-manager.js'
import type { StreamTask } from '@/runtime/task.js'

/** A slice stops after the record that crosses this much time since the slice started. */
export const ROUND_SLICE_MS = 10
/** An interval or user commit skipped for a pending rebalance is not retried sooner (CR12). */
const COMMIT_RETRY_MS = 100
/** A task whose readiness waits for a fetch is checked again after this (KS re-checks at every poll, poll.ms). */
const FETCH_RECHECK_MS = 100
/** Bound of an interval or user commit (CR2) and of beginning a transaction. */
const COMMIT_TIMEOUT_MS = 10_000

export interface SchedulerDeps {
	readonly tasks: Pick<TaskManager, 'tasks'>
	readonly commits: Pick<CommitCoordinator, 'commit' | 'lastCommitAt'>
	/** The Run's current collector. */
	readonly collector: () => Pick<RecordCollector, 'transactionOpen' | 'ensureTransaction'>
	readonly consumer: Pick<GroupConsumerPort, 'currentLag' | 'resume' | 'on'>
	readonly clock: Clock
	readonly config: Pick<ResolvedFlowConfig, 'eos' | 'commitIntervalMs' | 'transactionTimeoutMs'>
	/** Processing is paused (global re-bootstrap, 4.12): no slice starts, commits go on. wake() once it is not. */
	readonly paused: () => boolean
	/** The Run's `failed` flag (4.6.2): nothing new starts once it is set. */
	readonly failed: () => boolean
	/** Aborted at the close deadline: bounds the commits and the transaction begins. */
	readonly hardStop: AbortSignal
	readonly onFailure: (error: unknown, site: FailureSite) => void
	readonly logger: Logger
}

type Op = () => Promise<void>

const byTaskId = (a: StreamTask, b: StreamTask): number => TaskIdRegistry.compare(a.id, b.id)

/** When the task's next wall-clock punctuation or idle deadline is due (Infinity: none). */
const dueAt = (task: StreamTask): number =>
	Math.min(task.nextWallClockDue() ?? Infinity, task.nextIdleDeadline() ?? Infinity)

export class Scheduler {
	private state: 'created' | 'running' | 'stopped' = 'created'
	/** Tasks that may have work: records, a due punctuation or idle deadline (enqueue, RUNNING, timers). */
	private readonly work = new Set<StreamTask>()
	private readonly inFlight = new Map<StreamTask, Promise<void>>()
	/** Exclusive ops not started yet, in FIFO order. */
	private readonly ops: Op[] = []
	private draining: Promise<void> | undefined
	/** The loop's commit in progress: an exclusive op waits for it as for a slice. */
	private committing: Promise<void> | undefined
	/** Partitions paused for backpressure whose queue dropped to the limit, with the slice's task. */
	private resumes: { readonly task: StreamTask; readonly partitions: readonly TopicPartition[] }[] = []
	private commitRequested = false
	/** A skipped commit waits until then; a completed rebalance lifts it. */
	private commitRetryAt = -Infinity
	private timer: { readonly at: number; readonly cancel: () => void } | undefined
	private woken = false
	private wakeUp: (() => void) | undefined
	/** A readiness check met a partition whose lag is unknown or positive: its answer may change without a delivery. */
	private awaitingFetch = false

	constructor(private readonly deps: SchedulerDeps) {}

	start(): void {
		if (this.state !== 'created') {
			return
		}
		this.state = 'running'
		this.deps.consumer.on('rebalanceComplete', () => {
			this.commitRetryAt = -Infinity
			this.wake()
		})
		void this.loop()
	}

	/** The loop looks at everything again: records enqueued, a slice settled, an op, a timer, a rebalance completed. */
	wake(): void {
		this.woken = true
		const wakeUp = this.wakeUp
		this.wakeUp = undefined
		wakeUp?.()
	}

	/** The task has records, a due punctuation or a due idle deadline (enqueue, RUNNING transition, timers). */
	markWork(task: StreamTask): void {
		this.work.add(task)
		this.wake()
	}

	/**
	 * A barrier op: it starts once every slice in flight and the commit in progress settled, after the ops queued
	 * before it, and no slice starts until it settled. Its failure is its caller's only. Ops still run once the loop
	 * stopped or the run failed.
	 */
	exclusive<T>(fn: () => Promise<T>): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			this.ops.push(async () => {
				try {
					resolve(await fn())
				} catch (error) {
					reject(error)
				}
			})
			if (this.state === 'running') {
				this.wake()
			} else {
				void this.drainOps()
			}
		})
	}

	/** A commit of every task at the next barrier (`context.commit()`). */
	requestCommit(): void {
		this.commitRequested = true
		this.wake()
	}

	/**
	 * Starts no slice any more; a slice in flight stops after its current record. Resolves once the slices in flight
	 * settled or `signal` aborted. Later exclusive() calls still run.
	 */
	async stop(signal: AbortSignal): Promise<void> {
		this.state = 'stopped'
		this.wake()
		await raceSignal(Promise.allSettled(this.inFlight.values()), signal).catch(() => {})
	}

	// ---------------------------------------------------------------------------------------------------------

	private get running(): boolean {
		return this.state === 'running' && !this.deps.failed()
	}

	private async loop(): Promise<void> {
		try {
			while (this.running) {
				this.woken = false
				this.applyResumes()
				if (this.ops.length > 0 || this.draining) {
					await this.drainOps()
					continue
				}
				const now = this.deps.clock.now()
				if (now >= this.commitDueAt()) {
					this.committing = this.commit(now)
					await this.committing
					this.committing = undefined
					continue
				}
				this.markDue(now)
				const awaitingFetch = !this.deps.paused() && (await this.startSlices())
				await this.idle(awaitingFetch)
				await yieldIfDue()
			}
		} catch (error) {
			// A bug: KS hands an unexpected exception of its stream thread to the uncaught exception handler.
			this.deps.onFailure(error, 'processing')
		}
		this.state = 'stopped'
		this.arm(Infinity)
		await this.drainOps()
	}

	/**
	 * Starts a slice of every task with work and none in flight, in TaskId order, and yields to the event loop between
	 * two starts when due (slices that ran synchronously). Stops at an op, a failure, a stop or a pause. Returns whether
	 * a task waits for a fetch: the client tells nothing when a fetch delivers no record (an empty partition, markers).
	 */
	private async startSlices(): Promise<boolean> {
		let awaitingFetch = false
		for (const task of [...this.work].sort(byTaskId)) {
			if (!this.mayStartSlice()) {
				break
			}
			if (this.inFlight.has(task)) {
				continue
			}
			this.awaitingFetch = false
			if (!task.hasWork(this.deps.clock.now(), this.lag)) {
				if (this.awaitingFetch) awaitingFetch = true
				else this.work.delete(task)
				continue
			}
			if (this.deps.config.eos && !this.deps.collector().transactionOpen) {
				if (!(await this.beginTransaction()) || !this.mayStartSlice()) {
					break
				}
			}
			const slice = this.runSlice(task, this.deps.clock.now() + ROUND_SLICE_MS)
			this.inFlight.set(
				task,
				slice.finally(() => {
					this.inFlight.delete(task)
					this.wake()
				})
			)
			await yieldIfDue()
		}
		return awaitingFetch
	}

	private mayStartSlice(): boolean {
		return this.running && !this.deps.paused() && this.ops.length === 0 && this.draining === undefined
	}

	/** Exactly-once: the transaction the slices send in, begun once before the first of them (SI13). */
	private async beginTransaction(): Promise<boolean> {
		try {
			await this.deps.collector().ensureTransaction(cap(this.deps.hardStop, COMMIT_TIMEOUT_MS))
			return true
		} catch (error) {
			this.deps.onFailure(error, 'commit')
			return false
		}
	}

	/**
	 * The task's records until `deadline` (the slice stops after the record that crosses it), then its due
	 * punctuations, also without records (KF13), then the backpressure resumes it earned. Catches every error itself
	 * (CR4): a failure fails the run, after which no record or punctuation starts.
	 */
	private async runSlice(task: StreamTask, deadline: number): Promise<void> {
		const { clock } = this.deps
		let site: FailureSite = 'processing'
		try {
			const now = clock.now()
			let ready = task.isProcessable(now, this.lag)
			while (ready && this.continues(task)) {
				let processed = task.processOne(now)
				if (isThenable(processed)) {
					processed = await processed
				}
				if (!processed) {
					break
				}
				if (task.queueDrained) {
					ready = task.isProcessable(now, this.lag)
				}
				if (clock.now() >= deadline) {
					break
				}
			}
			site = 'punctuation'
			if (this.continues(task)) {
				await task.maybePunctuateStreamTime()
			}
			if (this.continues(task)) {
				await task.maybePunctuateSystemTime(clock.now())
			}
			const partitions = task.partitionsToResume()
			if (partitions.length > 0) {
				this.resumes.push({ task, partitions })
			}
		} catch (error) {
			this.deps.onFailure(error, site)
		}
	}

	/** Not stopped, the run not failed and the task not lost. */
	private continues(task: StreamTask): boolean {
		return this.running && !task.lost
	}

	/** The consumer's lag for readiness (KIP-695), noting a partition that still waits for a fetch. */
	private readonly lag = (tp: TopicPartition): bigint | undefined => {
		const lag = this.deps.consumer.currentLag(tp)
		if (lag === undefined || lag > 0n) {
			this.awaitingFetch = true
		}
		return lag
	}

	/** Backpressure (KS): partitions whose queue dropped to the limit fetch again, unless their task left RUNNING. */
	private applyResumes(): void {
		if (this.resumes.length === 0) {
			return
		}
		const partitions = this.resumes.flatMap(({ task, partitions }) =>
			task.state === 'RUNNING' && !task.lost ? partitions : []
		)
		this.resumes = []
		if (partitions.length === 0 || !this.running) {
			return
		}
		try {
			this.deps.consumer.resume(partitions)
		} catch (error) {
			this.deps.onFailure(error, 'consumer-run')
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Commits
	// ---------------------------------------------------------------------------------------------------------

	/** When the next interval or user commit is due; a skipped one waits for its retry time. */
	private commitDueAt(): number {
		const interval = this.deps.commits.lastCommitAt + this.deps.config.commitIntervalMs
		return Math.max(this.commitRetryAt, this.commitRequested ? -Infinity : interval)
	}

	/** An interval or user commit (4.8) once every slice settled. Failures go to onFailure. */
	private async commit(now: number): Promise<void> {
		const { commits, config, clock } = this.deps
		const reason: CommitReason = now >= commits.lastCommitAt + config.commitIntervalMs ? 'interval' : 'user'
		const requested = this.commitRequested
		this.commitRequested = false
		await Promise.allSettled(this.inFlight.values())
		if (!this.running) {
			return
		}
		const timeout = config.eos ? Math.min(config.transactionTimeoutMs / 2, COMMIT_TIMEOUT_MS) : COMMIT_TIMEOUT_MS
		let outcome: CommitOutcome
		try {
			outcome = await commits.commit(reason, cap(this.deps.hardStop, timeout))
		} catch (error) {
			this.deps.onFailure(error, 'commit')
			return
		}
		// Skipped: a rebalance is pending and the revocation commits instead. Lost: the lost hook's op cleans up first.
		if (outcome === 'skipped' || outcome === 'lost') {
			this.deps.logger.debug(`${reason} commit ${outcome}: retried later`)
			this.commitRetryAt = clock.now() + Math.min(config.commitIntervalMs, COMMIT_RETRY_MS)
			this.commitRequested ||= requested
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Exclusive ops
	// ---------------------------------------------------------------------------------------------------------

	/** Single-flight. */
	private drainOps(): Promise<void> {
		this.draining ??= this.drain()
		return this.draining
	}

	/** The queued ops in FIFO order, those queued meanwhile too, once every slice and the commit in progress settled. */
	private async drain(): Promise<void> {
		try {
			await Promise.allSettled([...this.inFlight.values(), this.committing])
			this.applyResumes()
			for (let op = this.ops.shift(); op !== undefined; op = this.ops.shift()) {
				await op()
			}
		} finally {
			this.draining = undefined
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Waiting
	// ---------------------------------------------------------------------------------------------------------

	/** Until woken or the next due time: a commit, a fetch re-check, a punctuation or an idle deadline of a task. */
	private async idle(awaitingFetch: boolean): Promise<void> {
		if (this.woken) {
			return
		}
		const now = this.deps.clock.now()
		const at = Math.min(awaitingFetch ? now + FETCH_RECHECK_MS : Infinity, this.nextDue())
		if (this.woken || at <= now) {
			return
		}
		this.arm(at)
		await new Promise<void>(resolve => {
			this.wakeUp = resolve
		})
	}

	/**
	 * Every loop iteration, woken or not (KS punctuates and checks the readiness of every task in each `runOnce`): the
	 * tasks outside the work set whose wall-clock punctuation or idle deadline is due join it (4.9: what the timer
	 * marks). A busy loop never idles, so the timer alone would leave them waiting until the others ran out of work.
	 */
	private markDue(now: number): void {
		for (const task of this.deps.tasks.tasks()) {
			if (!this.work.has(task) && dueAt(task) <= now) {
				this.work.add(task)
			}
		}
	}

	/** The earliest due time: the next commit, and the wall-clock punctuations and idle deadlines outside the work set. */
	private nextDue(): number {
		let next = this.commitDueAt()
		for (const task of this.deps.tasks.tasks()) {
			if (!this.work.has(task)) {
				next = Math.min(next, dueAt(task))
			}
		}
		return next
	}

	/** One timer, kept while its time stays the next due time. */
	private arm(at: number): void {
		if (this.timer?.at === at) {
			return
		}
		this.timer?.cancel()
		this.timer = undefined
		if (at !== Infinity) {
			const cancel = this.deps.clock.setTimer(at, () => {
				this.timer = undefined
				this.wake()
			})
			this.timer = { at, cancel }
		}
	}
}
