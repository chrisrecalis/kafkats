/**
 * Time-window and session-window aggregations (6.4, 6.5, 6.8): each keeps its windows in a clocked store whose
 * retention follows the retention model (`dsl/retention.ts`), emits each update or, with `emit: 'on-window-close'`,
 * each final result, and keys its result table by `Windowed<K>` in Kafka Streams' formats (`WindowedCodecs`, 6.9).
 * The results stay in the partition of their base key, where key-based operations on them find them: they need no
 * repartitioning (BC10).
 */
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec, type WindowedFormat } from '@/dsl/format.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { COUNT_CODEC, counting, requireAggregateCodec, type AggregationKind } from '@/dsl/ops/aggregate.js'
import { aggregationInput } from '@/dsl/ops/grouping.js'
import { aggregationRetentionMs } from '@/dsl/retention.js'
import type { SessionWindowedKStreamImpl } from '@/dsl/session-windowed-kstream.js'
import type { TimeWindowedKStreamImpl } from '@/dsl/time-windowed-kstream.js'
import { aggregateWith, reduceWith, type Aggregation } from '@/operators/kstream-aggregate.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import {
	sessionAggregateWith,
	sessionReduceWith,
	SessionWindowAggregate,
	type SessionMerge,
} from '@/operators/session-aggregate.js'
import { TimeWindowAggregate } from '@/operators/window-aggregate.js'
import { WindowedCodecs, type WindowedCodec } from '@/operators/windowed.js'
import { sessionStoreBuilder, timestampedWindowStoreBuilder } from '@/state/store-builders.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/** The session merger, or the deprecated options form carrying a two-argument merger. */
export type SessionMergerArg<K, A> =
	| ((key: K, aggregate1: A, aggregate2: A) => A)
	| (WindowedAggregateOptions<K, A> & { merger: (aggregate1: A, aggregate2: A) => A })

/** Store-name and node-name prefixes of each kind (time and session windows share the store prefixes, as today). */
const PREFIXES: Readonly<Record<AggregationKind, readonly [store: string, node: string]>> = {
	count: [StorePrefix.WINDOW_COUNT, NodePrefix.AGGREGATE],
	reduce: [StorePrefix.WINDOW_REDUCE, NodePrefix.REDUCE],
	aggregate: [StorePrefix.WINDOW_AGGREGATE, NodePrefix.AGGREGATE],
}

/** The store, the processor and the result key codec of one windowed aggregation, given its store name and key codec. */
type Create<K, A> = (
	ctx: DslContext,
	store: string,
	keyCodec: Codec<K>
) => {
	readonly builder: StoreBuilder
	readonly node: KTableNode<Windowed<K>, A>
	readonly resultKeyCodec: WindowedCodec<K>
}

export function timeWindowedCount<K, V>(
	windowed: TimeWindowedKStreamImpl<K, V>,
	options: WindowedAggregateOptions<K, number> = {}
): KTable<Windowed<K>, number> {
	const aggregation = aggregateWith<K, V, number>(counting.initializer, counting.aggregator)
	return timeWindowed(windowed, 'count', options, options.value ?? COUNT_CODEC, aggregation)
}

export function timeWindowedReduce<K, V>(
	windowed: TimeWindowedKStreamImpl<K, V>,
	reducer: (aggregate: V, value: V) => V,
	options: WindowedAggregateOptions<K, V> = {}
): KTable<Windowed<K>, V> {
	const valueCodec = requireCodec(
		options.value,
		windowed.grouped.stream.format.valueCodec,
		'value',
		'windowed reduce()'
	)
	return timeWindowed(windowed, 'reduce', options, valueCodec, reduceWith(reducer))
}

export function timeWindowedAggregate<K, V, A>(
	windowed: TimeWindowedKStreamImpl<K, V>,
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A,
	options: WindowedAggregateOptions<K, A> = {}
): KTable<Windowed<K>, A> {
	const valueCodec = requireAggregateCodec(options.value, 'windowed aggregate()')
	return timeWindowed(windowed, 'aggregate', options, valueCodec, aggregateWith(initializer, aggregator))
}

export function sessionWindowedCount<K, V>(
	windowed: SessionWindowedKStreamImpl<K, V>,
	options: WindowedAggregateOptions<K, number> = {}
): KTable<Windowed<K>, number> {
	const merge = sessionAggregateWith<K, V, number>(counting.initializer, counting.aggregator, counting.merger)
	return sessionWindowed(windowed, 'count', options, options.value ?? COUNT_CODEC, merge)
}

export function sessionWindowedReduce<K, V>(
	windowed: SessionWindowedKStreamImpl<K, V>,
	reducer: (aggregate: V, value: V) => V,
	options: WindowedAggregateOptions<K, V> = {}
): KTable<Windowed<K>, V> {
	const valueCodec = requireCodec(
		options.value,
		windowed.grouped.stream.format.valueCodec,
		'value',
		'windowed reduce()'
	)
	return sessionWindowed(windowed, 'reduce', options, valueCodec, sessionReduceWith(reducer))
}

export function sessionWindowedAggregate<K, V, A>(
	windowed: SessionWindowedKStreamImpl<K, V>,
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A,
	merger: SessionMergerArg<K, A>,
	options?: WindowedAggregateOptions<K, A>
): KTable<Windowed<K>, A> {
	const resolved = (typeof merger === 'function' ? options : merger) ?? {}
	const valueCodec = requireAggregateCodec(resolved.value, 'windowed aggregate()')
	const merge = sessionAggregateWith(initializer, aggregator, sessionMerger(merger))
	return sessionWindowed(windowed, 'aggregate', resolved, valueCodec, merge)
}

/** The merger of `aggregate()` on session windows; the deprecated two-argument form is wrapped. */
function sessionMerger<K, A>(merger: SessionMergerArg<K, A> | undefined): (key: K, a1: A, a2: A) => A {
	if (typeof merger === 'function') {
		return merger
	}
	const deprecated = merger?.merger
	if (deprecated === undefined) {
		throw new TopologyError(
			'aggregate() on session windows needs a merger to combine the aggregates of the sessions a record ' +
				'merges: aggregate(initializer, aggregator, (key, aggregate1, aggregate2) => ...)'
		)
	}
	return (_key, aggregate1, aggregate2) => deprecated(aggregate1, aggregate2)
}

/** Windows `[start, start + size)` whose values carry the max timestamp of their records, keyed `[key][start]`. */
function timeWindowed<K, V, A>(
	windowed: TimeWindowedKStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregation: Aggregation<K, V, A>
): KTable<Windowed<K>, A> {
	const { windows } = windowed
	const format: WindowedFormat = { kind: 'time', sizeMs: windows.sizeMs }
	const emitFinal = options.emit === 'on-window-close'
	return materialize(windowed.grouped, kind, options, valueCodec, format, (ctx, name, keyCodec) => ({
		builder: timestampedWindowStoreBuilder(ctx.changelog, {
			name,
			keyCodec,
			valueCodec,
			logging: options.changelog,
			retentionMs: aggregationRetentionMs(name, windows, options.retention),
			windowSizeMs: windows.sizeMs,
			clock: { emitFinal: emitFinal ? format : undefined },
		}),
		node: new TimeWindowAggregate(name, windows, aggregation, emitFinal),
		resultKeyCodec: WindowedCodecs.timeWindowed(keyCodec, windows.sizeMs),
	}))
}

/** Sessions `[start, end]`, whose results carry their end as timestamp, keyed `[key][end][start]`. */
function sessionWindowed<K, V, A>(
	windowed: SessionWindowedKStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	merge: SessionMerge<K, V, A>
): KTable<Windowed<K>, A> {
	const { windows } = windowed
	const format: WindowedFormat = { kind: 'session' }
	const emitFinal = options.emit === 'on-window-close'
	return materialize(windowed.grouped, kind, options, valueCodec, format, (ctx, name, keyCodec) => ({
		builder: sessionStoreBuilder(ctx.changelog, {
			name,
			keyCodec,
			valueCodec,
			logging: options.changelog,
			retentionMs: aggregationRetentionMs(name, windows, options.retention),
			clock: { emitFinal: emitFinal ? format : undefined },
		}),
		node: new SessionWindowAggregate(name, windows, merge, emitFinal),
		resultKeyCodec: WindowedCodecs.sessionWindowed(keyCodec),
	}))
}

/**
 * Registers the aggregation's store and processor (after the repartitioning its input needs); returns its table,
 * keyed by `Windowed<K>`. Its options are validated before anything is registered.
 */
function materialize<K, V, A>(
	grouped: KGroupedStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	windowed: WindowedFormat,
	create: Create<K, A>
): KTable<Windowed<K>, A> {
	const operator = `windowed ${kind}()`
	const keyCodec = requireCodec(options.key, grouped.stream.format.keyCodec, 'key', operator)
	const { ctx } = grouped.stream
	const [storePrefix, nodePrefix] = PREFIXES[kind]
	const store = storeName(ctx.builder, storePrefix, options.storeName)
	const { builder, node, resultKeyCodec } = create(ctx, store, keyCodec)
	const input = aggregationInput(grouped, keyCodec)
	ctx.builder.addStateStore(builder)
	const processor = ctx.processor(nodePrefix, options.name, [input.node], node.supplier, [store])
	const format = { keyCodec: resultKeyCodec, valueCodec, windowed }
	return new KTableImpl(ctx, processor, format, node)
}
