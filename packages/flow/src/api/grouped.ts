import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions, WindowedAggregateOptions } from '@/api/options.js'
import type { SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'

/** A key and the window it was aggregated in: `[start, end)` for time windows, `[start, end]` for sessions and sliding windows. */
export interface Windowed<K> {
	key: K
	window: { start: number; end: number }
}

/** A stream grouped by key, ready to be aggregated. Records with a null key or value are dropped. */
export interface KGroupedStream<K, V> {
	count(options?: AggregateOptions<K, number>): KTable<K, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: AggregateOptions<K, V>): KTable<K, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: AggregateOptions<K, A>
	): KTable<K, A>
	windowedBy(windows: TimeWindows): TimeWindowedKStream<K, V>
	windowedBy(windows: SessionWindows): SessionWindowedKStream<K, V>
	windowedBy(windows: SlidingWindows): SlidingWindowedKStream<K, V>
}

/** A table re-grouped by a new key: every update subtracts the old value and adds the new one. */
export interface KGroupedTable<K, V> {
	count(options?: AggregateOptions<K, number>): KTable<K, number>
	reduce(
		adder: (aggregate: V, value: V) => V,
		subtractor: (aggregate: V, value: V) => V,
		options?: AggregateOptions<K, V>
	): KTable<K, V>
	aggregate<A>(
		initializer: () => A,
		adder: (key: K, value: V, aggregate: A) => A,
		subtractor: (key: K, value: V, aggregate: A) => A,
		options?: AggregateOptions<K, A>
	): KTable<K, A>
}

/** A grouped stream aggregated per time window. */
export interface TimeWindowedKStream<K, V> {
	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: WindowedAggregateOptions<K, V>): KTable<Windowed<K>, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A>
}

/** A grouped stream aggregated per session: sessions that a record bridges are merged. */
export interface SessionWindowedKStream<K, V> {
	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number>
	/** The reducer also merges sessions. */
	reduce(reducer: (aggregate: V, value: V) => V, options?: WindowedAggregateOptions<K, V>): KTable<Windowed<K>, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		merger: (key: K, aggregate1: A, aggregate2: A) => A,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A>
	/** @deprecated Pass the merger as the third argument: `(key, aggregate1, aggregate2) => A`. */
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options: WindowedAggregateOptions<K, A> & { merger: (aggregate1: A, aggregate2: A) => A }
	): KTable<Windowed<K>, A>
}

/** A grouped stream aggregated per sliding window. */
export interface SlidingWindowedKStream<K, V> {
	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: WindowedAggregateOptions<K, V>): KTable<Windowed<K>, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A>
}
