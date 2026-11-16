/**
 * Commits of every RUNNING task of a run (4.8, KS `TaskExecutor.commitTasksAndMaybeUpdateCommittableOffsets`):
 * at-least-once flushes the sends and commits offsets with the commit metadata through the consumer; exactly-once
 * commits one transaction for all tasks, then applies the buffered store writes. Checkpoints follow a commit.
 */
import type { ConsumerGroupMetadata, Logger, TopicPartition } from '@kafkats/client'
import { CommitFailedError, RebalanceInProgressError, TimeoutError } from '@kafkats/client'
import { TaskCorruptedError, TaskMigratedError } from '@/api/errors.js'
import type { CommitListener, ResolvedFlowConfig } from '@/runtime/config.js'
import { errorMessage } from '@/runtime/errors.js'
import type { Clock, GroupConsumerPort, OffsetAndMetadata } from '@/runtime/io.js'
import type { RecordCollector } from '@/runtime/record-collector.js'
import { cap } from '@/runtime/signals.js'
import type { StreamTask } from '@/runtime/task.js'

export type CommitReason = 'interval' | 'user' | 'revocation' | 'close'

/**
 * nothing: no task needed a commit. skipped: a rebalance is pending (retry soon). deferred: an at-least-once commit
 * timed out or met a rebalance (retry at the next interval). lost: a task was lost meanwhile (the lost hook cleans up).
 */
export type CommitOutcome = 'nothing' | 'committed' | 'skipped' | 'deferred' | 'lost'

/** How long an exactly-once abort after a failed commit may take. */
const ABORT_TIMEOUT_MS = 5000

export interface CommitCoordinatorDeps {
	readonly consumer: Pick<GroupConsumerPort, 'commit' | 'nextOffset' | 'groupMetadata' | 'rebalancePending'>
	/** The run's current collector. */
	readonly collector: () => RecordCollector
	readonly tasks: () => readonly StreamTask[]
	/** The group metadata captured at the last assignment (offsets of a transaction are fenced by it). */
	readonly assignmentMetadata: () => ConsumerGroupMetadata | undefined
	readonly eos: boolean
	readonly checkpoints: { flush(): Promise<void> }
	readonly onCommitted: CommitListener
	/** The run failed: nothing is committed any more. */
	readonly failed: () => boolean
	/** Aborted at the close deadline: bounds the abort after a failed exactly-once commit. */
	readonly hardStop: AbortSignal
	readonly config: Pick<ResolvedFlowConfig, 'taskTimeoutMs'>
	readonly clock: Clock
	readonly logger: Logger
}

export class CommitCoordinator {
	private lastAttempt: number
	private lastSuccess: number
	private committed = false

	constructor(private readonly deps: CommitCoordinatorDeps) {
		this.lastAttempt = this.lastSuccess = deps.clock.now()
	}

	get lastCommitAt(): number {
		return this.lastAttempt
	}

	get lastSuccessAt(): number {
		return this.lastSuccess
	}

	get committedOnce(): boolean {
		return this.committed
	}

	/**
	 * Commits every RUNNING task that is not lost. MUST run at a scheduler barrier or inside exclusive. Rethrows the
	 * original error after the cleanup; throws TaskCorruptedError for tasks whose at-least-once sends failed.
	 */
	async commit(reason: CommitReason, signal: AbortSignal): Promise<CommitOutcome> {
		if (this.deps.failed()) {
			return 'nothing'
		}
		if ((reason === 'interval' || reason === 'user') && this.deps.consumer.rebalancePending) {
			return 'skipped'
		}
		const tasks = this.deps.tasks().filter(task => task.state === 'RUNNING' && !task.lost)
		return this.deps.eos ? this.commitTransaction(tasks, signal) : this.commitOffsets(reason, tasks, signal)
	}

	/** At-least-once (4.8.1). */
	private async commitOffsets(
		reason: CommitReason,
		tasks: readonly StreamTask[],
		signal: AbortSignal
	): Promise<CommitOutcome> {
		const { consumer } = this.deps
		const collector = this.deps.collector()
		const { corrupted } = await collector.flush(signal)
		const next = this.next
		const committing = tasks.filter(task => !corrupted.has(task) && task.needsCommit(next))
		const offsets = committing.flatMap(task => task.committableOffsets(next))
		let acknowledged = offsets
		if (offsets.length > 0) {
			try {
				await consumer.commit(offsets, { signal })
			} catch (error) {
				const outcome = this.offsetCommitFailed(reason, error, signal, committing)
				if (outcome !== undefined) {
					return outcome
				}
				acknowledged = []
			}
		}
		for (const task of corrupted) {
			collector.takeChangelogOffsets(task)
		}
		await this.postCommit(committing, task => collector.takeChangelogOffsets(task), acknowledged)
		this.succeeded(acknowledged)
		if (corrupted.size > 0) {
			const ids = [...corrupted].map(task => task.id)
			throw new TaskCorruptedError(`sends of tasks ${ids.map(id => id.key).join(', ')} failed`, ids)
		}
		return committing.length > 0 ? 'committed' : 'nothing'
	}

	/**
	 * A failed at-least-once offset commit: an outcome to return, undefined to go on without the offsets (a
	 * rebalance at a revocation or close commit: the next owner reprocesses them, the checkpoints still describe the
	 * local state), or a thrown error.
	 */
	private offsetCommitFailed(
		reason: CommitReason,
		error: unknown,
		signal: AbortSignal,
		committing: readonly StreamTask[]
	): CommitOutcome | undefined {
		const rebalancing = error instanceof RebalanceInProgressError
		if (reason === 'interval' || reason === 'user') {
			if (rebalancing || error instanceof TimeoutError || timedOut(signal)) {
				// KS: retried at the next interval, so a stalled coordinator does not turn every loop into a barrier.
				const now = this.deps.clock.now()
				this.lastAttempt = now
				if (now - this.lastSuccess > this.deps.config.taskTimeoutMs) {
					throw new TaskMigratedError(`offset commits failed for more than taskTimeoutMs`, { cause: error })
				}
				this.deps.logger.debug('offset commit deferred to the next interval', { error: errorMessage(error) })
				return 'deferred'
			}
		} else if (rebalancing) {
			this.deps.logger.warn(`a rebalance prevented the ${reason} offset commit: the next owner reprocesses`, {
				error: errorMessage(error),
			})
			return undefined
		}
		if (error instanceof CommitFailedError && committing.some(task => task.lost)) {
			return 'lost'
		}
		throw error
	}

	/** Exactly-once: one transaction for all tasks (4.8.2). */
	private async commitTransaction(tasks: readonly StreamTask[], signal: AbortSignal): Promise<CommitOutcome> {
		const collector = this.deps.collector()
		const next = this.next
		const committing = tasks.filter(task => task.needsCommit(next))
		if (committing.length === 0 && !collector.transactionOpen) {
			this.succeeded([])
			return 'nothing'
		}
		if ((await this.checkFencing()) === 'lost') {
			return 'lost'
		}
		const group = this.deps.assignmentMetadata()!
		const positions = new Map<StreamTask, Map<string, bigint>>()
		let offsets: OffsetAndMetadata[]
		try {
			await collector.flush(signal)
			for (const task of committing) {
				positions.set(task, collector.takeChangelogOffsets(task))
			}
			offsets = committing.flatMap(task => task.committableOffsets(next))
		} catch (error) {
			await this.abort()
			throw error
		}
		// A loss during the flush: nothing of this transaction may commit.
		if ((await this.checkFencing()) === 'lost') {
			return 'lost'
		}
		try {
			await collector.commitTransaction(offsets, group, signal)
		} catch (error) {
			await this.abort()
			throw error
		}
		const applied = await Promise.allSettled(tasks.map(task => task.commitBuffers()))
		const failure = applied.find(result => result.status === 'rejected')
		if (failure) {
			// The store is behind the committed changelog: only a restore repairs it, so every buffer goes.
			for (const task of tasks) {
				task.discardBuffers()
			}
			throw failure.reason
		}
		await this.postCommit(committing, task => positions.get(task)!, offsets)
		this.succeeded(offsets)
		return 'committed'
	}

	/**
	 * The transaction's offsets are fenced by the assignment's generation (CR1): if a task was lost, or that generation
	 * is unknown or no longer the consumer's, aborts and answers 'lost' or throws TaskMigratedError.
	 */
	private async checkFencing(): Promise<'lost' | undefined> {
		const lost = this.deps.tasks().some(task => task.lost)
		const assignment = this.deps.assignmentMetadata()
		const current = this.deps.consumer.groupMetadata()
		const fenced =
			assignment === undefined || assignment.generationId < 0 || current?.generationId !== assignment.generationId
		if (!lost && !fenced) {
			return undefined
		}
		await this.abort()
		if (lost) {
			return 'lost'
		}
		throw new TaskMigratedError(
			`the consumer group generation moved from ${assignment?.generationId} to ${current?.generationId}`
		)
	}

	/** Aborts the open transaction and drops every buffered store write. */
	private async abort(): Promise<void> {
		await this.deps.collector().abortTransaction(cap(this.deps.hardStop, ABORT_TIMEOUT_MS))
		for (const task of this.deps.tasks()) {
			task.discardBuffers()
		}
	}

	private async postCommit(
		tasks: readonly StreamTask[],
		positions: (task: StreamTask) => ReadonlyMap<string, bigint>,
		offsets: readonly OffsetAndMetadata[]
	): Promise<void> {
		for (const task of tasks) {
			await task.postCommit(positions(task), offsets)
		}
		await this.deps.checkpoints.flush()
	}

	private succeeded(offsets: readonly OffsetAndMetadata[]): void {
		this.lastAttempt = this.lastSuccess = this.deps.clock.now()
		if (offsets.length > 0) {
			this.committed = true
			this.deps.onCommitted.noteCommitted(offsets)
		}
	}

	private readonly next = (tp: TopicPartition): bigint | undefined => this.deps.consumer.nextOffset(tp)
}

/** The commit's own deadline passed (not the run's hard stop). */
function timedOut(signal: AbortSignal): boolean {
	return signal.aborted && signal.reason instanceof DOMException && signal.reason.name === 'TimeoutError'
}
