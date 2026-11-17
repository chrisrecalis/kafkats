/**
 * Sliding-window aggregations (6.6, 6.8): the windows `[start, start + d]` of each key are kept as sessions of a
 * clocked session-layout store, so the windows a record may change are read from one key (B15), whose values carry the
 * max timestamp of their records and whose retention follows the retention model (`dsl/retention.ts`). Each update is
 * emitted or, with `emit: 'on-window-close'`, each final result. The result table is keyed by `Windowed<K>` in Kafka
 * Streams' time-windowed format with size `d` (end = start + d, 6.9) and, like the other windowed results, needs no
 * repartitioning (BC10).
 */
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import { requireCodec, type WindowedFormat } from '@/dsl/format.js'
import { COUNT_CODEC, counting, requireAggregateCodec, type AggregationKind } from '@/dsl/ops/aggregate.js'
import { materialize } from '@/dsl/ops/windowed-aggregate.js'
import { aggregationRetentionMs } from '@/dsl/retention.js'
import type { SlidingWindowedKStreamImpl } from '@/dsl/sliding-windowed-kstream.js'
import { aggregateWith, reduceWith, type Aggregation } from '@/operators/kstream-aggregate.js'
import { SlidingWindowAggregate } from '@/operators/sliding-aggregate.js'
import { WindowedCodecs } from '@/operators/windowed.js'
import { timestampedSessionStoreBuilder } from '@/state/store-builders.js'

export function slidingWindowedCount<K, V>(
	windowed: SlidingWindowedKStreamImpl<K, V>,
	options: WindowedAggregateOptions<K, number> = {}
): KTable<Windowed<K>, number> {
	const aggregation = aggregateWith<K, V, number>(counting.initializer, counting.aggregator)
	return slidingWindowed(windowed, 'count', options, options.value ?? COUNT_CODEC, aggregation)
}

export function slidingWindowedReduce<K, V>(
	windowed: SlidingWindowedKStreamImpl<K, V>,
	reducer: (aggregate: V, value: V) => V,
	options: WindowedAggregateOptions<K, V> = {}
): KTable<Windowed<K>, V> {
	const valueCodec = requireCodec(
		options.value,
		windowed.grouped.stream.format.valueCodec,
		'value',
		'windowed reduce()'
	)
	return slidingWindowed(windowed, 'reduce', options, valueCodec, reduceWith(reducer))
}

export function slidingWindowedAggregate<K, V, A>(
	windowed: SlidingWindowedKStreamImpl<K, V>,
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A,
	options: WindowedAggregateOptions<K, A> = {}
): KTable<Windowed<K>, A> {
	const valueCodec = requireAggregateCodec(options.value, 'windowed aggregate()')
	return slidingWindowed(windowed, 'aggregate', options, valueCodec, aggregateWith(initializer, aggregator))
}

function slidingWindowed<K, V, A>(
	windowed: SlidingWindowedKStreamImpl<K, V>,
	kind: AggregationKind,
	options: WindowedAggregateOptions<K, A>,
	valueCodec: Codec<A>,
	aggregation: Aggregation<K, V, A>
): KTable<Windowed<K>, A> {
	const { windows } = windowed
	const format: WindowedFormat = { kind: 'sliding', sizeMs: windows.timeDifferenceMs }
	const emitFinal = options.emit === 'on-window-close'
	return materialize(windowed.grouped, kind, options, valueCodec, format, (ctx, name, keyCodec) => ({
		builder: timestampedSessionStoreBuilder(ctx.changelog, {
			name,
			keyCodec,
			valueCodec,
			logging: options.changelog,
			retentionMs: aggregationRetentionMs(name, windows, options.retention),
			clock: { emitFinal: emitFinal ? format : undefined },
		}),
		node: new SlidingWindowAggregate(name, windows, aggregation, emitFinal),
		resultKeyCodec: WindowedCodecs.timeWindowed(keyCodec, windows.timeDifferenceMs),
		graceMs: windows.graceMs,
	}))
}
