import type { SlidingWindowedKStream, Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { slidingWindowedAggregate, slidingWindowedCount, slidingWindowedReduce } from '@/dsl/ops/sliding-aggregate.js'
import type { SlidingWindows } from '@/dsl/windows.js'

export class SlidingWindowedKStreamImpl<K, V> implements SlidingWindowedKStream<K, V> {
	constructor(
		readonly grouped: KGroupedStreamImpl<K, V>,
		readonly windows: SlidingWindows
	) {}

	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number> {
		return slidingWindowedCount(this, options)
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: WindowedAggregateOptions<K, V>): KTable<Windowed<K>, V> {
		return slidingWindowedReduce(this, reducer, options)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A> {
		return slidingWindowedAggregate(this, initializer, aggregator, options)
	}
}
