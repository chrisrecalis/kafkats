/**
 * Table filter, filterNot and mapValues (6.12), and what every derived table shares with table-table joins: its codecs
 * (Materialized first, then the parent's; the value codec only where values are unchanged) and its store, registered
 * only when Materialized names one (6.11).
 */
import type { ValueMapperWithKey } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { Materialized, TableOptions } from '@/api/options.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec, type StreamFormat } from '@/dsl/format.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import { ktableFilter } from '@/operators/ktable-filter.js'
import { KTableMapValues } from '@/operators/ktable-map-values.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import { timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'

export function filter<K, V>(
	table: KTableImpl<K, V>,
	predicate: (key: K, value: V) => boolean,
	options?: TableOptions<K, V>
): KTable<K, V> {
	return filtered(table, predicate, false, options)
}

export function filterNot<K, V>(
	table: KTableImpl<K, V>,
	predicate: (key: K, value: V) => boolean,
	options?: TableOptions<K, V>
): KTable<K, V> {
	return filtered(table, predicate, true, options)
}

/** mapValues() and mapValuesWithKey(): the mapper as a function of the key and the value. */
export function mapValues<K, V, VR>(
	table: KTableImpl<K, V>,
	mapper: ValueMapperWithKey<K, V, VR | null>,
	options: TableOptions<K, VR> = {}
): KTable<K, VR> {
	const format = derivedFormat(table.format, options.materialized, false)
	const storeName = materializedStore(table.ctx, options.materialized, format, 'mapValues()')
	const node = new KTableMapValues(table.table, mapper, { storeName })
	return derived(table, NodePrefix.TABLE_MAP_VALUES, options.name, node, format, storeName)
}

function filtered<K, V>(
	table: KTableImpl<K, V>,
	predicate: (key: K, value: V) => boolean,
	negate: boolean,
	options: TableOptions<K, V> = {}
): KTable<K, V> {
	const operator = negate ? 'filterNot()' : 'filter()'
	const format = derivedFormat(table.format, options.materialized, true)
	const storeName = materializedStore(table.ctx, options.materialized, format, operator)
	const node = ktableFilter(table.table, predicate, negate, storeName)
	return derived(table, NodePrefix.TABLE_FILTER, options.name, node, format, storeName)
}

/** Registers the processor of a table derived from `table` (connected to its own store, if any). */
function derived<K, V, VR>(
	table: KTableImpl<K, V>,
	prefix: string,
	name: string | undefined,
	node: KTableNode<K, VR>,
	format: StreamFormat,
	storeName: string | undefined
): KTable<K, VR> {
	const stores = storeName === undefined ? [] : [storeName]
	const processor = table.ctx.processor(prefix, name, [table.node], node.supplier, stores)
	return new KTableImpl(table.ctx, processor, format, node)
}

/** A derived table's codecs: Materialized first, then the parent's (its value codec only if values are unchanged). */
export function derivedFormat<K, V>(
	format: StreamFormat,
	materialized: Materialized<K, V> | undefined,
	sameValues: boolean
): StreamFormat {
	return {
		keyCodec: materialized?.key ?? format.keyCodec,
		valueCodec: materialized?.value ?? (sameValues ? format.valueCodec : undefined),
		windowed: materialized?.key === undefined ? format.windowed : undefined,
	}
}

/** Registers the timestamped store a Materialized with a store name asks for; returns its name. */
export function materializedStore<K, V>(
	ctx: DslContext,
	materialized: Materialized<K, V> | undefined,
	format: StreamFormat,
	operator: string
): string | undefined {
	const name = materialized?.storeName
	if (name === undefined) {
		return undefined
	}
	const keyCodec = requireCodec<K>(undefined, format.keyCodec, 'key', operator)
	const valueCodec = requireCodec<V>(undefined, format.valueCodec, 'value', operator)
	const logging = materialized?.changelog
	ctx.builder.addStateStore(timestampedKeyValueStoreBuilder(ctx.changelog, { name, keyCodec, valueCodec, logging }))
	return name
}
