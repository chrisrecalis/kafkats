/** Sliding-window aggregations (6.6, 6.8). */
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { SlidingWindowedKStreamImpl } from '@/dsl/sliding-windowed-kstream.js'

export function slidingWindowedCount<K, V>(
	_windowed: SlidingWindowedKStreamImpl<K, V>,
	_options?: WindowedAggregateOptions<K, number>
): KTable<Windowed<K>, number> {
	return notImplemented('SlidingWindowedKStream.count')
}

export function slidingWindowedReduce<K, V>(
	_windowed: SlidingWindowedKStreamImpl<K, V>,
	_reducer: (aggregate: V, value: V) => V,
	_options?: WindowedAggregateOptions<K, V>
): KTable<Windowed<K>, V> {
	return notImplemented('SlidingWindowedKStream.reduce')
}

export function slidingWindowedAggregate<K, V, A>(
	_windowed: SlidingWindowedKStreamImpl<K, V>,
	_initializer: () => A,
	_aggregator: (key: K, value: V, aggregate: A) => A,
	_options?: WindowedAggregateOptions<K, A>
): KTable<Windowed<K>, A> {
	return notImplemented('SlidingWindowedKStream.aggregate')
}
