/** Grouping a stream for aggregations: KStream.groupBy and groupByKey (6.0). */
import type { KGroupedStream } from '@/api/grouped.js'
import type { Grouped } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function groupBy<K, V, KR>(
	_stream: KStreamImpl<K, V>,
	_selector: (key: K | null, value: V | null) => KR,
	_options?: Grouped<KR, V>
): KGroupedStream<KR, V> {
	return notImplemented('KStream.groupBy')
}

export function groupByKey<K, V>(_stream: KStreamImpl<K, V>, _options?: Grouped<K, V>): KGroupedStream<K, V> {
	return notImplemented('KStream.groupByKey')
}
