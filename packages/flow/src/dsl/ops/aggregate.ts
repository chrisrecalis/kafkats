/**
 * Non-windowed KGroupedStream aggregations (6.3): count, reduce and aggregate, each materialized in its own timestamped
 * key-value store; the result table's format is the store's codecs.
 */
import { TopologyError } from '@/api/errors.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import { requireCodec } from '@/dsl/format.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { aggregationInput } from '@/dsl/ops/grouping.js'
import { aggregateWith, KStreamAggregate, reduceWith, type Aggregation } from '@/operators/kstream-aggregate.js'
import { timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'

/**
 * Default codec of counts, also of the row counts of a KGroupedTable aggregate(): an int64 big-endian (KS
 * `Serdes.Long`, BC21).
 */
export const COUNT_CODEC: Codec<number> = {
	encode: count => {
		const encoded = Buffer.allocUnsafe(8)
		encoded.writeBigInt64BE(BigInt(count))
		return encoded
	},
	decode: encoded => Number(encoded.readBigInt64BE()),
}

/** count(): +1 per record; merging two counts adds them. */
export const counting = {
	initializer: (): number => 0,
	aggregator: (_key: unknown, _value: unknown, count: number): number => count + 1,
	merger: (_key: unknown, count1: number, count2: number): number => count1 + count2,
}

/** The codec of aggregates of another type than the values: only Materialized.value can give it. */
export function requireAggregateCodec<A>(codec: Codec<A> | undefined, operator: string): Codec<A> {
	if (codec === undefined) {
		throw new TopologyError(`${operator} needs a codec for its aggregates: pass one as the value option`)
	}
	return codec
}

/**
 * The codec of the aggregates of a grouped stream's aggregate(), windowed or not: Materialized.value, else the value
 * codec, which the API lets only aggregates of the value type go without (6.3).
 */
export function aggregateCodec<K, V, A>(
	grouped: KGroupedStreamImpl<K, V>,
	codec: Codec<A> | undefined,
	operator: string
): Codec<A> {
	return requireAggregateCodec(codec ?? (grouped.stream.format.valueCodec as Codec<A> | undefined), operator)
}

/** count(), reduce() or aggregate(). */
export type AggregationKind = 'count' | 'reduce' | 'aggregate'

/** Store-name and node-name prefixes of each kind. */
const PREFIXES: Readonly<Record<AggregationKind, readonly [store: string, node: string]>> = {
	count: [StorePrefix.COUNT, NodePrefix.AGGREGATE],
	reduce: [StorePrefix.REDUCE, NodePrefix.REDUCE],
	aggregate: [StorePrefix.AGGREGATE, NodePrefix.AGGREGATE],
}

export function count<K, V>(
	grouped: KGroupedStreamImpl<K, V>,
	options: AggregateOptions<K, number> = {}
): KTable<K, number> {
	const aggregation = aggregateWith<K, V, number>(counting.initializer, counting.aggregator)
	return materialize(grouped, 'count', options, options.value ?? COUNT_CODEC, aggregation)
}

export function reduce<K, V>(
	grouped: KGroupedStreamImpl<K, V>,
	reducer: (aggregate: V, value: V) => V,
	options: AggregateOptions<K, V> = {}
): KTable<K, V> {
	const valueCodec = requireCodec(options.value, grouped.stream.format.valueCodec, 'value', 'reduce()')
	return materialize(grouped, 'reduce', options, valueCodec, reduceWith(reducer))
}

export function aggregate<K, V, A>(
	grouped: KGroupedStreamImpl<K, V>,
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A,
	options: AggregateOptions<K, A> = {}
): KTable<K, A> {
	const valueCodec = aggregateCodec(grouped, options.value, 'aggregate()')
	return materialize(grouped, 'aggregate', options, valueCodec, aggregateWith(initializer, aggregator))
}

/**
 * Registers the aggregation's store, then the repartitioning its input needs (whose topic may be named after the store:
 * a duplicate store name is reported as such, B5) and its processor; returns its table.
 */
function materialize<K, V, A>(
	grouped: KGroupedStreamImpl<K, V>,
	kind: AggregationKind,
	options: AggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregation: Aggregation<K, V, A>
): KTable<K, A> {
	const operator = `${kind}()`
	const keyCodec = requireCodec(options.key, grouped.stream.format.keyCodec, 'key', operator)
	const { ctx } = grouped.stream
	const [storePrefix, nodePrefix] = PREFIXES[kind]
	const store = storeName(ctx.builder, storePrefix, options.storeName)
	const logging = options.changelog
	ctx.builder.addStateStore(
		timestampedKeyValueStoreBuilder(ctx.changelog, { name: store, keyCodec, valueCodec, logging })
	)
	const input = aggregationInput(grouped, keyCodec, options.storeName)
	const node = new KStreamAggregate<K, V, A>(store, aggregation)
	const processor = ctx.processor(nodePrefix, options.name, [input.node], node.supplier, [store])
	return new KTableImpl<K, A>(ctx, processor, { keyCodec, valueCodec }, node)
}
