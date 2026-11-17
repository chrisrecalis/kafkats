/**
 * Time-window and session-window aggregations (6.4, 6.5), as today: stores keep 24 window sizes / session gaps, and
 * result keys are `Windowed<K>` in today's format, `[key][start int64BE][end int64BE]`. Results stay in the partition
 * of their base key, so a key-based operation on them repartitions first. Retention options, emit on window close and
 * Kafka Streams' windowed formats come in Stage 3 (WP3.3, WP3.4).
 */
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec, type WindowedFormat } from '@/dsl/format.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import { COUNT_CODEC, counting, requireAggregateCodec, type AggregationKind } from '@/dsl/ops/aggregate.js'
import { aggregationInput } from '@/dsl/ops/grouping.js'
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
import { TimeWindowAggregate, WINDOW_STORE_RETENTION_MULTIPLIER } from '@/operators/window-aggregate.js'
import { sessionStoreBuilder, windowStoreBuilder } from '@/state/store-builders.js'
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

/** The store and processor of one windowed aggregation, given its store name and key codec. */
type Create<K, A> = (
	ctx: DslContext,
	store: string,
	keyCodec: Codec<K>
) => { readonly builder: StoreBuilder; readonly node: KTableNode<Windowed<K>, A> }

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

/** Today's windowed key format: the key, then window start and end as int64 big-endian (as in changelogs). */
function windowedCodec<K>(inner: Codec<K>): Codec<Windowed<K>> {
	const codec = windowedKeyCodec(inner)
	return {
		encode: ({ key, window }) => codec.encode({ key, windowStart: window.start, windowEnd: window.end }),
		decode: bytes => {
			const { key, windowStart, windowEnd } = codec.decode(bytes)
			return { key, window: { start: windowStart, end: windowEnd } }
		},
	}
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

function timeWindowed<K, V, A>(
	windowed: TimeWindowedKStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregation: Aggregation<K, V, A>
): KTable<Windowed<K>, A> {
	const { windows } = windowed
	const format: WindowedFormat = { kind: 'time', sizeMs: windows.sizeMs }
	return materialize(windowed.grouped, kind, options, valueCodec, format, (ctx, name, keyCodec) => ({
		builder: windowStoreBuilder(ctx.changelog, {
			name,
			keyCodec,
			valueCodec,
			logging: options.changelog,
			retentionMs: windows.sizeMs * WINDOW_STORE_RETENTION_MULTIPLIER,
			windowSizeMs: windows.sizeMs,
		}),
		node: new TimeWindowAggregate(name, windows, aggregation),
	}))
}

function sessionWindowed<K, V, A>(
	windowed: SessionWindowedKStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	merge: SessionMerge<K, V, A>
): KTable<Windowed<K>, A> {
	const { gapMs } = windowed.windows
	return materialize(windowed.grouped, kind, options, valueCodec, { kind: 'session' }, (ctx, name, keyCodec) => ({
		builder: sessionStoreBuilder(ctx.changelog, {
			name,
			keyCodec,
			valueCodec,
			logging: options.changelog,
			retentionMs: gapMs * WINDOW_STORE_RETENTION_MULTIPLIER,
		}),
		node: new SessionWindowAggregate(name, gapMs, merge),
	}))
}

/**
 * Registers the aggregation's store and processor (after the repartitioning its input needs); returns its table,
 * keyed by `Windowed<K>`.
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
	if (options.retention !== undefined) {
		notImplemented(`${operator} with a retention`)
	}
	if (options.emit === 'on-window-close') {
		notImplemented(`${operator} emitting on window close`)
	}
	const keyCodec = requireCodec(options.key, grouped.stream.format.keyCodec, 'key', operator)
	const input = aggregationInput(grouped, keyCodec)
	const { ctx } = input
	const [storePrefix, nodePrefix] = PREFIXES[kind]
	const store = storeName(ctx.builder, storePrefix, options.storeName)
	const { builder, node } = create(ctx, store, keyCodec)
	ctx.builder.addStateStore(builder)
	const processor = ctx.processor(nodePrefix, options.name, [input.node], node.supplier, [store])
	return new KTableImpl(ctx, processor, { keyCodec: windowedCodec(keyCodec), valueCodec, windowed }, node)
}
