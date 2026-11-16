/**
 * KTable.groupBy and the KGroupedTable aggregations (6.15), with today's mapping-store semantics (WP3.8 switches to
 * Change pairs). groupBy registers the repartition-map processor and its mapping store `groupby-mapping-<n>` (always
 * logged). Each aggregation reads the grouped rows back from a repartition topic, `<applicationId>-<Grouped.name |
 * Materialized.storeName | repartition-<n>>-repartition`: every aggregation of a named grouping shares one (KF6), every
 * aggregation of an unnamed one has its own (Kafka Streams).
 */
import { TopologyError } from '@/api/errors.js'
import type { KGroupedTable } from '@/api/grouped.js'
import type { KeyValue } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions, Grouped } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec } from '@/dsl/format.js'
import { KGroupedTableImpl } from '@/dsl/kgrouped-table.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import {
	KTableAggregate,
	tableAggregate,
	tableCount,
	tableReduce,
	type GroupAggregator,
} from '@/operators/ktable-aggregate.js'
import { groupedRowCodec, repartitionMapProcessor } from '@/operators/ktable-repartition-map.js'
import { keyValueStoreBuilder, timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'

/** Default codec of counts and row counts: an 8-byte little-endian double (today's format; WP3.10 switches to int64). */
const COUNT_CODEC: Codec<number> = {
	encode: count => {
		const encoded = Buffer.alloc(8)
		encoded.writeDoubleLE(count, 0)
		return encoded
	},
	decode: encoded => encoded.readDoubleLE(0),
}

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
	const mapping = storeName(ctx.builder, StorePrefix.GROUP_BY_MAPPING)
	ctx.builder.addStateStore(
		keyValueStoreBuilder(ctx.changelog, {
			name: mapping,
			keyCodec: requireCodec<K>(undefined, table.format.keyCodec, 'key', operator),
			valueCodec: groupedRowCodec(keyCodec, valueCodec),
		})
	)
	const supplier = repartitionMapProcessor(selector, mapping)
	const node = ctx.processor(NodePrefix.TABLE_SELECT, options.name, [table.node], supplier, [mapping])
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
	const valueCodec = requireCodec(options.value, grouped.format.valueCodec, 'value', 'reduce()')
	return aggregateGroups<K, V, V>(grouped, 'reduce', options, valueCodec, tableReduce(adder, subtractor))
}

/** Counts the rows of each group too: a group whose last row left is deleted. */
export function aggregate<K, V, A>(
	grouped: KGroupedTableImpl<K, V>,
	initializer: () => A,
	adder: (key: K, value: V, aggregate: A) => A,
	subtractor: (key: K, value: V, aggregate: A) => A,
	options: AggregateOptions<K, A> = {}
): KTable<K, A> {
	if (options.value === undefined) {
		throw new TopologyError('aggregate() needs a codec for its aggregates: pass one as the value option')
	}
	const aggregator = tableAggregate(initializer, adder, subtractor)
	return aggregateGroups(grouped, 'aggregate', options, options.value, aggregator)
}

type AggregationKind = 'count' | 'reduce' | 'aggregate'

/** Store-name and node-name prefixes of each kind. */
const PREFIXES: Readonly<Record<AggregationKind, readonly [store: string, node: string]>> = {
	count: [StorePrefix.TABLE_COUNT, NodePrefix.TABLE_AGGREGATE],
	reduce: [StorePrefix.TABLE_REDUCE, NodePrefix.TABLE_REDUCE],
	aggregate: [StorePrefix.TABLE_AGGREGATE, NodePrefix.TABLE_AGGREGATE],
}

/** Registers the aggregation's stores and processor after the repartitioning of the grouped rows. */
function aggregateGroups<K, V, A>(
	grouped: KGroupedTableImpl<K, V>,
	kind: AggregationKind,
	options: AggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregator: GroupAggregator<K, V, A>
): KTable<K, A> {
	const operator = `${kind}()`
	const { ctx } = grouped
	const keyCodec = requireCodec(options.key, grouped.format.keyCodec, 'key', operator)
	const input = groupedRows(grouped, options.storeName, operator)
	const [storePrefix, nodePrefix] = PREFIXES[kind]
	const store = storeName(ctx.builder, storePrefix, options.storeName)
	const logging = options.changelog
	ctx.builder.addStateStore(
		timestampedKeyValueStoreBuilder(ctx.changelog, { name: store, keyCodec, valueCodec, logging })
	)
	const membersStore = kind === 'aggregate' ? addMembersStore(ctx, store, keyCodec, logging) : undefined
	const stores = membersStore === undefined ? [store] : [store, membersStore]
	const node = new KTableAggregate<K, V, A>({ storeName: store, membersStore }, aggregator)
	const processor = ctx.processor(nodePrefix, options.name, [input.node], node.supplier, stores)
	return new KTableImpl<K, A>(ctx, processor, { keyCodec, valueCodec }, node)
}

/**
 * aggregate()'s row counts per group, `<store>-members`: logged unless the aggregate's changelog is off; a
 * ChangelogConfig is not applied (B20; today's rule until WP3.8 removes the store).
 */
function addMembersStore<K>(
	ctx: DslContext,
	store: string,
	keyCodec: Codec<K>,
	changelog: AggregateOptions<K, unknown>['changelog']
): string {
	const name = `${store}-members`
	const logging = changelog === false ? false : undefined
	ctx.builder.addStateStore(keyValueStoreBuilder(ctx.changelog, { name, keyCodec, valueCodec: COUNT_CODEC, logging }))
	return name
}

/**
 * The grouped rows an aggregation reads: written to the grouping's repartition topic (named after the grouping, else
 * after the aggregation's store) and read back, so each group is aggregated in the partition its key hashes to.
 */
function groupedRows<K, V>(
	grouped: KGroupedTableImpl<K, V>,
	store: string | undefined,
	operator: string
): KStreamImpl<K, V> {
	if (grouped.repartitioned !== undefined) {
		return grouped.repartitioned
	}
	const rows = new KStreamImpl<K, V>(grouped.ctx, grouped.node, grouped.format, true)
	const named = grouped.name !== undefined
	const name = grouped.name ?? store
	const input = repartitionIfRequired(rows, { name, owner: named ? grouped : {}, codecs: {}, operator })
	if (named) {
		grouped.repartitioned = input
	}
	return input
}
