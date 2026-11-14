import type { ConsumeContext, Message, Producer } from '@kafkats/client'
import type { ActiveTransaction, TopicPartitionKey, WorkerContext } from '@/processors/index.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
import { isInfrastructureRecoverable, isRecoverable, toError } from '@/runtime/errors.js'

/** Default transaction commit interval for exactly_once mode (100ms) */
export const DEFAULT_COMMIT_INTERVAL_MS = 100

/** A producer transaction held open across records until the batch commits or aborts. */
type OpenTransaction = {
	tx: ActiveTransaction
	/** Let the producer commit; resolves once it has. */
	commit(): Promise<void>
	/** Make the producer abort; resolves once it has. */
	abort(error: Error): Promise<void>
}

/**
 * The exactly-once transaction batch of one worker.
 *
 * Records are processed inside a producer transaction that stays open across records and commits
 * when the commit interval elapses. All transactional work of the worker (processing, commits,
 * aborts) runs through one serialized task queue. The parts of the batch that
 * processors and state stores read (the transaction, dirty stores, pending checkpoints) live on
 * the WorkerContext; the rest is private to this class.
 */
export class WorkerTransactions {
	private open: OpenTransaction | null = null
	/** Offsets (next to read) of the records processed in the open transaction. */
	private readonly pendingOffsets = new Map<TopicPartitionKey, { topic: string; partition: number; offset: bigint }>()
	/** Consume context of the batch's first record, needed to commit offsets in the transaction. */
	private offsetCommitContext: ConsumeContext | null = null
	/** Serializes all transactional work of the worker. */
	private queue: Promise<void> = Promise.resolve()
	private commitTimer: ReturnType<typeof setTimeout> | null = null
	private lastCommitTime = 0

	constructor(
		private readonly worker: WorkerContext,
		private readonly commitIntervalMs: number,
		private readonly checkpoints: Checkpoints,
		/** Fail-stop the worker (see StreamWorker.fail). */
		private readonly fail: (err: unknown, recoverable: boolean) => void
	) {}

	/** Run a task after every transactional task queued before it, whether that succeeded or not. */
	enqueue<T>(fn: () => Promise<T>): Promise<T> {
		const run = this.queue.then(fn, fn)
		this.queue = run.then(
			() => {},
			() => {}
		)
		return run
	}

	/** Wait for all queued transactional work. */
	idle(): Promise<void> {
		return this.queue
	}

	get active(): boolean {
		return this.open !== null
	}

	/**
	 * Process a message within a batched transaction.
	 * Multiple messages are batched into a single transaction and committed together
	 * when the commit interval elapses.
	 */
	async process(message: Message<Buffer>, ctx: ConsumeContext, handle: () => Promise<void>): Promise<void> {
		const worker = this.worker
		// A commit queued ahead of this record may have failed in the meantime.
		if (worker.failure) throw worker.failure.error

		try {
			if (!this.open) {
				await this.begin()
				this.offsetCommitContext = ctx
			}

			await handle()

			// Track this offset for the batch commit
			const key: TopicPartitionKey = `${message.topic}:${message.partition}`
			const existingOffset = this.pendingOffsets.get(key)
			if (!existingOffset || message.offset + 1n > existingOffset.offset) {
				this.pendingOffsets.set(key, {
					topic: message.topic,
					partition: message.partition,
					offset: message.offset + 1n, // Commit the next offset
				})
			}
		} catch (err) {
			this.fail(err, isRecoverable(err))
			await this.abort(err)
			throw err
		}

		// Commit if the interval has elapsed, otherwise make sure a commit is scheduled.
		if (Date.now() - this.lastCommitTime >= this.commitIntervalMs) {
			await this.commit()
		} else if (!this.commitTimer) {
			this.scheduleCommit()
		}
	}

	/**
	 * Commit the current transaction batch: offsets + outputs + changelog records atomically in
	 * Kafka, then apply the buffered state updates locally and advance the checkpoints.
	 *
	 * On failure the transaction is aborted, buffered state is dropped and the worker is failed,
	 * so nothing after the last successful commit is ever acknowledged or kept.
	 */
	async commit(): Promise<void> {
		const worker = this.worker
		this.cancelCommitTimer()

		const open = this.open
		if (!open) return

		const offsets = Array.from(this.pendingOffsets.values())
		const dirtyStores = [...worker.dirtyStores]
		let changelogPositions: ReturnType<typeof worker.pendingCheckpoints.list>

		try {
			// Everything the batch produced must be acknowledged before the transaction commits.
			// The acknowledgements also report the changelog positions to checkpoint.
			await worker.sends.flush()
			changelogPositions = worker.pendingCheckpoints.list()
			if (offsets.length > 0) {
				if (!this.offsetCommitContext) {
					throw new Error('exactly_once requires a consume context for transactional offset commits')
				}
				await open.tx.sendOffsets(this.offsetCommitContext, offsets)
			}
			await open.commit()
		} catch (err) {
			await open.abort(toError(err))
			for (const store of dirtyStores) store.discardBuffer(worker.dirtyStores)
			this.clearState()
			// The consumer's position is already past the records of this batch: it must not
			// continue, or the next commit would acknowledge records whose effects were aborted.
			this.fail(err, isInfrastructureRecoverable(err))
			throw err
		}

		// The transaction is committed. Make the buffered state visible locally, then checkpoint.
		// A crash in between is safe: the store is at most behind the changelog and the checkpoint
		// still points at or before it, so restoration replays the difference.
		this.clearState()
		worker.processedSinceRestart = true
		try {
			for (const store of dirtyStores) {
				await store.commitBuffer(worker.dirtyStores)
			}
		} catch (err) {
			// Local state is now behind the committed changelog; only a restore can repair it.
			this.fail(err, true)
			throw err
		}
		await this.checkpoints.flush(changelogPositions)
	}

	async abort(error: unknown): Promise<void> {
		const worker = this.worker
		this.cancelCommitTimer()

		const open = this.open

		for (const store of worker.dirtyStores) store.discardBuffer(worker.dirtyStores)
		worker.sends.discard()
		this.clearState()
		// Stream time advanced by the aborted records must not make their replay look late.
		worker.streamClocks.clear()

		await open?.abort(toError(error))
	}

	cancelCommitTimer(): void {
		if (this.commitTimer) {
			clearTimeout(this.commitTimer)
			this.commitTimer = null
		}
	}

	private async begin(): Promise<void> {
		const worker = this.worker
		this.open = await openTransaction(worker.producer)
		worker.activeTransaction = this.open.tx
		this.lastCommitTime = Date.now()
	}

	private clearState(): void {
		const worker = this.worker
		this.pendingOffsets.clear()
		worker.pendingCheckpoints.clear()
		worker.dirtyStores.clear()
		this.open = null
		worker.activeTransaction = null
		this.offsetCommitContext = null
		this.lastCommitTime = Date.now()
	}

	private scheduleCommit(): void {
		if (this.commitTimer) {
			return
		}

		const timeUntilCommit = Math.max(0, this.commitIntervalMs - (Date.now() - this.lastCommitTime))
		this.commitTimer = setTimeout(() => {
			this.commitTimer = null
			// A failed commit fails the worker inside commit() (fail-stop).
			this.enqueue(() => this.commit()).catch(() => {})
		}, timeUntilCommit)
	}
}

/**
 * Open a producer transaction and keep it open: the callback given to the producer waits on a
 * gate, so the producer commits when the gate resolves and aborts when it rejects.
 */
function openTransaction(producer: Producer): Promise<OpenTransaction> {
	return new Promise((resolve, reject) => {
		let opened = false
		let release!: () => void
		let cancel!: (error: Error) => void
		const gate = new Promise<void>((res, rej) => {
			release = res
			cancel = rej
		})

		const completion = producer.transaction(tx => {
			opened = true
			// `completion` is assigned once producer.transaction() has returned.
			queueMicrotask(() =>
				resolve({
					tx: tx as ActiveTransaction,
					commit: () => {
						release()
						return completion
					},
					abort: async error => {
						cancel(error)
						await completion.catch(() => {})
					},
				})
			)
			return gate
		})

		completion.catch(err => {
			if (!opened) reject(toError(err))
		})
	})
}
