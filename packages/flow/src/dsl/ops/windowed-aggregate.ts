/** Time-window and session-window aggregations (6.4, 6.5, 6.8). */
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { SessionWindowedKStreamImpl } from '@/dsl/session-windowed-kstream.js'
import type { TimeWindowedKStreamImpl } from '@/dsl/time-windowed-kstream.js'

/** The session merger, or the deprecated options form carrying a two-argument merger. */
export type SessionMergerArg<K, A> =
	| ((key: K, aggregate1: A, aggregate2: A) => A)
	| (WindowedAggregateOptions<K, A> & { merger: (aggregate1: A, aggregate2: A) => A })

export function timeWindowedCount<K, V>(
	_windowed: TimeWindowedKStreamImpl<K, V>,
	_options?: WindowedAggregateOptions<K, number>
): KTable<Windowed<K>, number> {
	return notImplemented('TimeWindowedKStream.count')
}

export function timeWindowedReduce<K, V>(
	_windowed: TimeWindowedKStreamImpl<K, V>,
	_reducer: (aggregate: V, value: V) => V,
	_options?: WindowedAggregateOptions<K, V>
): KTable<Windowed<K>, V> {
	return notImplemented('TimeWindowedKStream.reduce')
}

export function timeWindowedAggregate<K, V, A>(
	_windowed: TimeWindowedKStreamImpl<K, V>,
	_initializer: () => A,
	_aggregator: (key: K, value: V, aggregate: A) => A,
	_options?: WindowedAggregateOptions<K, A>
): KTable<Windowed<K>, A> {
	return notImplemented('TimeWindowedKStream.aggregate')
}

export function sessionWindowedCount<K, V>(
	_windowed: SessionWindowedKStreamImpl<K, V>,
	_options?: WindowedAggregateOptions<K, number>
): KTable<Windowed<K>, number> {
	return notImplemented('SessionWindowedKStream.count')
}

export function sessionWindowedReduce<K, V>(
	_windowed: SessionWindowedKStreamImpl<K, V>,
	_reducer: (aggregate: V, value: V) => V,
	_options?: WindowedAggregateOptions<K, V>
): KTable<Windowed<K>, V> {
	return notImplemented('SessionWindowedKStream.reduce')
}

export function sessionWindowedAggregate<K, V, A>(
	_windowed: SessionWindowedKStreamImpl<K, V>,
	_initializer: () => A,
	_aggregator: (key: K, value: V, aggregate: A) => A,
	_merger: SessionMergerArg<K, A>,
	_options?: WindowedAggregateOptions<K, A>
): KTable<Windowed<K>, A> {
	return notImplemented('SessionWindowedKStream.aggregate')
}
