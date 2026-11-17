import type { TimeWindowedKStream, Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { timeWindowedAggregate, timeWindowedCount, timeWindowedReduce } from '@/dsl/ops/windowed-aggregate.js'
import type { TimeWindows } from '@/dsl/windows.js'

export class TimeWindowedKStreamImpl<K, V> implements TimeWindowedKStream<K, V> {
	constructor(
		readonly grouped: KGroupedStreamImpl<K, V>,
		readonly windows: TimeWindows
	) {}

	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number> {
		return timeWindowedCount(this, options)
	}

	reduce(
		reducer: (aggregate: V, value: V | null) => V,
		options?: WindowedAggregateOptions<K, V>
	): KTable<Windowed<K>, V> {
		return timeWindowedReduce(this, reducer, options)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V | null, aggregate: A) => A,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A> {
		return timeWindowedAggregate(this, initializer, aggregator, options)
	}
}
