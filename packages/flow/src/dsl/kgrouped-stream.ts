import type {
	KGroupedStream,
	SessionWindowedKStream,
	SlidingWindowedKStream,
	TimeWindowedKStream,
} from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { aggregate, count, reduce } from '@/dsl/ops/aggregate.js'
import { SessionWindowedKStreamImpl } from '@/dsl/session-windowed-kstream.js'
import { SlidingWindowedKStreamImpl } from '@/dsl/sliding-windowed-kstream.js'
import { TimeWindowedKStreamImpl } from '@/dsl/time-windowed-kstream.js'
import { SessionWindows, SlidingWindows, type TimeWindows } from '@/dsl/windows.js'

export class KGroupedStreamImpl<K, V> implements KGroupedStream<K, V> {
	constructor(
		/** The records to aggregate: Grouped codecs applied, repartitionRequired as upstream (always after groupBy). */
		readonly stream: KStreamImpl<K, V>,
		/** Grouped.name: one repartition topic shared by every aggregation of this grouping. */
		readonly name: string | undefined
	) {}

	count(options?: AggregateOptions<K, number>): KTable<K, number> {
		return count(this, options)
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: AggregateOptions<K, V>): KTable<K, V> {
		return reduce(this, reducer, options)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: AggregateOptions<K, A>
	): KTable<K, A> {
		return aggregate(this, initializer, aggregator, options)
	}

	windowedBy(windows: TimeWindows): TimeWindowedKStream<K, V>
	windowedBy(windows: SessionWindows): SessionWindowedKStream<K, V>
	windowedBy(windows: SlidingWindows): SlidingWindowedKStream<K, V>
	windowedBy(
		windows: TimeWindows | SessionWindows | SlidingWindows
	): TimeWindowedKStream<K, V> | SessionWindowedKStream<K, V> | SlidingWindowedKStream<K, V> {
		if (windows instanceof SessionWindows) {
			return new SessionWindowedKStreamImpl(this, windows)
		}
		if (windows instanceof SlidingWindows) {
			return new SlidingWindowedKStreamImpl(this, windows)
		}
		return new TimeWindowedKStreamImpl(this, windows)
	}
}
