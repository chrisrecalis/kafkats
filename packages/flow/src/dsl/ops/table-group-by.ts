/**
 * KTable.groupBy and the KGroupedTable aggregations (6.15). groupBy forces old values on its table and registers the
 * repartition map, which turns every update of a row into the changes of the groups it leaves and joins. Each
 * aggregation reads these changes back from a repartition topic, `<applicationId>-<Grouped.name |
 * Materialized.storeName | repartition-<n>>-repartition`, in Kafka Streams' Change format: every aggregation of a named
 * grouping shares one (KF6), every aggregation of an unnamed one has its own (Kafka Streams).
 */
import type { KGroupedTable } from '@/api/grouped.js'
import type { KeyValue } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions, Grouped } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import { requireCodec } from '@/dsl/format.js'
import { KGroupedTableImpl } from '@/dsl/kgrouped-table.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { COUNT_CODEC, requireAggregateCodec, type AggregationKind } from '@/dsl/ops/aggregate.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { changeCodec, type Change } from '@/operators/change.js'
import {
	KTableAggregate,
	tableAggregate,
	tableCount,
	tableReduce,
	type TableAggregator,
} from '@/operators/ktable-aggregate.js'
import { repartitionMapProcessor } from '@/operators/ktable-repartition-map.js'
import { timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'

/** The grouped key codec must be given (the selector changes keys); the value codec defaults to the table's. */
export function groupBy<K, V, KR, VR>(
	table: KTableImpl<K, V>,
	selector: (key: K, value: V) => KeyValue<KR, VR>,
	options: Grouped<KR, VR> = {}
): KGroupedTable<KR, VR> {
	const operator = 'groupBy()'
	const { ctx } = table
	const keyCodec = requireCodec(options.key, undefined, 'key', operator)
	const valueCodec = requireCodec(options.value, table.format.valueCodec, 'value', operator)
	table.table.enableSendingOldValues(true)
	const supplier = repartitionMapProcessor(selector, keyCodec)
	const node = ctx.processor(NodePrefix.TABLE_SELECT, options.name, [table.node], supplier)
	return new KGroupedTableImpl<KR, VR>(ctx, node, { keyCodec, valueCodec }, options.name)
}

export function count<K, V>(
	grouped: KGroupedTableImpl<K, V>,
	options: AggregateOptions<K, number> = {}
): KTable<K, number> {
	return aggregateGroups<K, V, number>(grouped, 'count', options, options.value ?? COUNT_CODEC, tableCount)
}

export function reduce<K, V>(
	grouped: KGroupedTableImpl<K, V>,
	adder: (aggregate: V, value: V) => V,
	subtractor: (aggregate: V, value: V) => V,
	options: AggregateOptions<K, V> = {}
): KTable<K, V> {
	const valueCodec = options.value ?? grouped.codecs.valueCodec
	return aggregateGroups<K, V, V>(grouped, 'reduce', options, valueCodec, tableReduce(adder, subtractor))
}

export function aggregate<K, V, A>(
	grouped: KGroupedTableImpl<K, V>,
	initializer: () => A,
	adder: (key: K, value: V, aggregate: A) => A,
	subtractor: (key: K, value: V, aggregate: A) => A,
	options: AggregateOptions<K, A> = {}
): KTable<K, A> {
	const valueCodec = requireAggregateCodec(options.value, 'aggregate()')
	const aggregator = tableAggregate(initializer, adder, subtractor)
	return aggregateGroups(grouped, 'aggregate', options, valueCodec, aggregator)
}

/** Store-name and node-name prefixes of each kind. */
const PREFIXES: Readonly<Record<AggregationKind, readonly [store: string, node: string]>> = {
	count: [StorePrefix.TABLE_COUNT, NodePrefix.TABLE_AGGREGATE],
	reduce: [StorePrefix.TABLE_REDUCE, NodePrefix.TABLE_REDUCE],
	aggregate: [StorePrefix.TABLE_AGGREGATE, NodePrefix.TABLE_AGGREGATE],
}

/**
 * Registers the aggregation's timestamped store, then the repartitioning of the groups' changes (whose topic may be
 * named after the store: a duplicate store name is reported as such, B5) and its processor.
 */
function aggregateGroups<K, V, A>(
	grouped: KGroupedTableImpl<K, V>,
	kind: AggregationKind,
	options: AggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregator: TableAggregator<K, V, A>
): KTable<K, A> {
	const { ctx } = grouped
	const keyCodec = options.key ?? grouped.codecs.keyCodec
	const [storePrefix, nodePrefix] = PREFIXES[kind]
	const store = storeName(ctx.builder, storePrefix, options.storeName)
	const logging = options.changelog
	ctx.builder.addStateStore(
		timestampedKeyValueStoreBuilder(ctx.changelog, { name: store, keyCodec, valueCodec, logging })
	)
	const input = groupChanges(grouped, options.storeName, `${kind}()`)
	const node = new KTableAggregate<K, V, A>(store, aggregator)
	const processor = ctx.processor(nodePrefix, options.name, [input.node], node.supplier, [store])
	return new KTableImpl<K, A>(ctx, processor, { keyCodec, valueCodec }, node)
}

/**
 * The changes an aggregation reads: written to the grouping's repartition topic (named after the grouping, else after
 * the aggregation's store) and read back, so each group is aggregated in the partition its key hashes to.
 */
function groupChanges<K, V>(
	grouped: KGroupedTableImpl<K, V>,
	store: string | undefined,
	operator: string
): KStreamImpl<K, Change<V>> {
	if (grouped.repartitioned !== undefined) {
		return grouped.repartitioned
	}
	const { keyCodec, valueCodec } = grouped.codecs
	const format = { keyCodec, valueCodec: changeCodec(valueCodec) }
	const changes = new KStreamImpl<K, Change<V>>(grouped.ctx, grouped.node, format, true)
	const named = grouped.name !== undefined
	const name = grouped.name ?? store
	const input = repartitionIfRequired(changes, { name, owner: named ? grouped : {}, codecs: {}, operator })
	if (named) {
		grouped.repartitioned = input
	}
	return input
}
