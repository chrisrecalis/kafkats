import type { ConsumeContext, Message, Producer } from '@kafkats/client'
import type { ActiveTransaction, TopicPartitionKey, WorkerContext } from '@/processors/index.js'
import type { Checkpoints } from '@/runtime/checkpoints.js'
import { isInfrastructureRecoverable, isRecoverable, toError } from '@/runtime/errors.js'

/** Default transaction commit interval for exactly_once mode (100ms) */
export const DEFAULT_COMMIT_INTERVAL_MS = 100

/** A producer transaction held open until the batch commits or aborts. */
type GatedTransaction = ActiveTransaction & { _resolve?: () => void; _reject?: (err: Error) => void }

/**
 * The exactly-once transaction batch of one worker.
 *
 * Records are processed inside a producer transaction that stays open across records and commits
 * when the commit interval elapses. All transactional work of the worker (processing, commits,
 * aborts) runs through one serialized task queue. The batch state itself lives on the
 * WorkerContext, where processors and state stores read it.
 */
export class WorkerTransactions {
	constructor(
		private readonly worker: WorkerContext,
		private readonly commitIntervalMs: number,
		private readonly checkpoints: Checkpoints,
		/** Fail-stop the worker (see StreamWorker.fail). */
		private readonly fail: (err: unknown, recoverable: boolean) => void
	) {}

	/** Run a task after every transactional task queued before it, whether that succeeded or not. */
	enqueue<T>(fn: () => Promise<T>): Promise<T> {
		const run = this.worker.eosQueue.then(fn, fn)
		this.worker.eosQueue = run.then(
			() => {},
			() => {}
		)
		return run
	}

	/** Wait for all queued transactional work. */
	idle(): Promise<void> {
		return this.worker.eosQueue
	}

	get active(): boolean {
		return this.worker.transactionActive
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
			if (!worker.transactionActive) {
				await this.begin()
				worker.offsetCommitContext = ctx
			}

			await handle()

			// Track this offset for the batch commit
			const key: TopicPartitionKey = `${message.topic}:${message.partition}`
			const existingOffset = worker.pendingOffsets.get(key)
			if (!existingOffset || message.offset + 1n > existingOffset.offset) {
				worker.pendingOffsets.set(key, {
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
		if (Date.now() - worker.lastCommitTime >= this.commitIntervalMs) {
			await this.commit()
		} else if (!worker.commitTimer) {
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

		if (!worker.transactionActive || !worker.activeTransaction || !worker.activeTransactionPromise) {
			return
		}

		const tx = worker.activeTransaction as GatedTransaction
		const txPromise = worker.activeTransactionPromise
		const offsets = Array.from(worker.pendingOffsets.values())
		const changelogPositions = worker.pendingCheckpoints.list()
		const dirtyStores = [...worker.dirtyStores]

		try {
			if (offsets.length > 0) {
				if (!worker.offsetCommitContext) {
					throw new Error('exactly_once requires a consume context for transactional offset commits')
				}
				await tx.sendOffsets(worker.offsetCommitContext, offsets)
			}

			// Complete the transaction by resolving the gate promise (commit happens inside producer.transaction()).
			tx._resolve?.()
			await txPromise
		} catch (err) {
			tx._reject?.(toError(err))
			await txPromise.catch(() => {})
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

		const tx = worker.activeTransaction as GatedTransaction | null
		const txPromise = worker.activeTransactionPromise

		for (const store of worker.dirtyStores) store.discardBuffer(worker.dirtyStores)
		this.clearState()
		// Stream time advanced by the aborted records must not make their replay look late.
		worker.streamClocks.clear()

		if (tx && txPromise) {
			tx._reject?.(toError(error))
			await txPromise.catch(() => {})
		}
	}

	cancelCommitTimer(): void {
		if (this.worker.commitTimer) {
			clearTimeout(this.worker.commitTimer)
			this.worker.commitTimer = null
		}
	}

	private async begin(): Promise<void> {
		const worker = this.worker
		const started = await startProducerTransaction(worker.producer)
		worker.activeTransaction = started.tx
		worker.activeTransactionPromise = started.completion
		worker.transactionActive = true
		worker.lastCommitTime = Date.now()
	}

	private clearState(): void {
		const worker = this.worker
		worker.pendingOffsets.clear()
		worker.pendingCheckpoints.clear()
		worker.dirtyStores.clear()
		worker.activeTransaction = null
		worker.activeTransactionPromise = null
		worker.offsetCommitContext = null
		worker.transactionActive = false
		worker.lastCommitTime = Date.now()
	}

	private scheduleCommit(): void {
		const worker = this.worker
		if (worker.commitTimer) {
			return
		}

		const timeUntilCommit = Math.max(0, this.commitIntervalMs - (Date.now() - worker.lastCommitTime))
		worker.commitTimer = setTimeout(() => {
			worker.commitTimer = null
			// A failed commit fails the worker inside commit() (fail-stop).
			this.enqueue(() => this.commit()).catch(() => {})
		}, timeUntilCommit)
	}
}

/**
 * Open a producer transaction and hand back its handle while keeping it open: the transaction
 * callback waits on a gate that commit() resolves and abort() rejects.
 */
function startProducerTransaction(producer: Producer): Promise<{ tx: ActiveTransaction; completion: Promise<void> }> {
	return new Promise((resolve, reject) => {
		let txResolved = false

		const completion = producer.transaction(async tx => {
			const txHandle = tx as GatedTransaction

			const gate = new Promise<void>((resolveInner, rejectInner) => {
				txHandle._resolve = resolveInner
				txHandle._reject = (err: Error) => rejectInner(err)
			})

			txResolved = true
			queueMicrotask(() => resolve({ tx: txHandle, completion }))
			return gate
		})

		completion.catch(err => {
			if (!txResolved) {
				reject(toError(err))
			}
		})
	})
}
