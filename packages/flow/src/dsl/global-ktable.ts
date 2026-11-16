import type { GlobalKTable } from '@/api/ktable.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'

export class GlobalKTableImpl<K, V> implements GlobalKTable<K, V> {
	declare readonly __global?: { readonly key: K; readonly value: V }

	constructor(
		readonly ctx: DslContext,
		readonly storeName: string,
		readonly format: StreamFormat
	) {}
}
