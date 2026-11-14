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

/** A store whose writes are buffered until the surrounding transaction commits. */
export interface TransactionalBuffer {
	/** Apply buffered writes to the underlying store (the transaction committed). */
	commitBuffer(): Promise<void>
	/** Drop buffered writes (the transaction aborted). */
	discardBuffer(): void
}

/**
 * Returns the set collecting stores written by the current transaction, or null when the current
 * record is not being processed inside a transaction (at-least-once mode, restoration, tests).
 */
export function currentTransactionScope(): Set<TransactionalBuffer> | null {
	const ctx = recordContextStorage.getStore()
	if (!ctx || !ctx.worker.activeTransaction) {
		return null
	}
	return ctx.worker.dirtyStores
}
