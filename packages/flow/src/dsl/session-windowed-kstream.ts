import type { SessionWindowedKStream, Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import type { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import {
	sessionWindowedAggregate,
	sessionWindowedCount,
	sessionWindowedReduce,
	type SessionMergerArg,
} from '@/dsl/ops/windowed-aggregate.js'
import type { SessionWindows } from '@/dsl/windows.js'

export class SessionWindowedKStreamImpl<K, V> implements SessionWindowedKStream<K, V> {
	constructor(
		readonly grouped: KGroupedStreamImpl<K, V>,
		readonly windows: SessionWindows
	) {}

	count(options?: WindowedAggregateOptions<K, number>): KTable<Windowed<K>, number> {
		return sessionWindowedCount(this, options)
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: WindowedAggregateOptions<K, V>): KTable<Windowed<K>, V> {
		return sessionWindowedReduce(this, reducer, options)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		merger: SessionMergerArg<K, A>,
		options?: WindowedAggregateOptions<K, A>
	): KTable<Windowed<K>, A> {
		return sessionWindowedAggregate(this, initializer, aggregator, merger, options)
	}
}
