import type { GlobalKTable } from '@/api/ktable.js'
import type { DslContext } from '@/dsl/context.js'

/** A global table of an app: its store, which stream joins of the same app look up (6.19). */
export class GlobalKTableImpl<K, V> implements GlobalKTable<K, V> {
	declare readonly __global?: { readonly key: K; readonly value: V }

	constructor(
		readonly ctx: DslContext,
		readonly storeName: string
	) {}
}
