/** Non-windowed KGroupedStream aggregations: count, reduce, aggregate (6.3). */
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions } from '@/api/options.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function count<K, V>(
	_grouped: KGroupedStreamImpl<K, V>,
	_options?: AggregateOptions<K, number>
): KTable<K, number> {
	return notImplemented('KGroupedStream.count')
}

export function reduce<K, V>(
	_grouped: KGroupedStreamImpl<K, V>,
	_reducer: (aggregate: V, value: V) => V,
	_options?: AggregateOptions<K, V>
): KTable<K, V> {
	return notImplemented('KGroupedStream.reduce')
}

export function aggregate<K, V, A>(
	_grouped: KGroupedStreamImpl<K, V>,
	_initializer: () => A,
	_aggregator: (key: K, value: V, aggregate: A) => A,
	_options?: AggregateOptions<K, A>
): KTable<K, A> {
	return notImplemented('KGroupedStream.aggregate')
}
