import { AsyncLocalStorage } from 'node:async_hooks'
import type { WorkerContext } from '@/processors/base.js'

/**
 * Identifies the record currently being processed.
 *
 * State is partitioned the same way the input is: the record's source partition selects the
 * state store partition it reads/writes and the changelog partition its mutations are logged to.
 */
export interface RecordContext {
	worker: WorkerContext
	topic: string
	partition: number
}

/** Tracks the record being processed across the async processor chain. */
export const recordContextStorage = new AsyncLocalStorage<RecordContext>()

/**
 * Identity of a worker's open transaction: the set of stores it has buffered writes in.
 * Each worker has its own, so buffers of different workers never mix.
 */
export type TransactionScope = Set<TransactionalBuffer>

/** A store whose writes are buffered until the surrounding transaction commits. */
export interface TransactionalBuffer {
	/** Apply the writes buffered by the given transaction scope to the underlying store (it committed). */
	commitBuffer(scope: TransactionScope): Promise<void>
	/** Drop the writes buffered by the given transaction scope (it aborted), or by all scopes. */
	discardBuffer(scope?: TransactionScope): void
}

/**
 * Returns the set collecting stores written by the current transaction, or null when the current
 * record is not being processed inside a transaction (at-least-once mode, restoration, tests).
 */
export function currentTransactionScope(): TransactionScope | null {
	const ctx = recordContextStorage.getStore()
	if (!ctx || !ctx.worker.activeTransaction) {
		return null
	}
	return ctx.worker.dirtyStores
}

/** The transaction scope of the worker processing the current record (even if none is open). */
export function currentWorkerScope(): TransactionScope | null {
	return recordContextStorage.getStore()?.worker.dirtyStores ?? null
}
