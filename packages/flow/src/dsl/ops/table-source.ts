/** Tables read from a stream: KStream.toTable (6.0, 6.11). */
import type { KTable } from '@/api/ktable.js'
import type { Materialized, NamedOptions } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function toTable<K, V>(_stream: KStreamImpl<K, V>, _options?: Materialized<K, V> & NamedOptions): KTable<K, V> {
	return notImplemented('KStream.toTable')
}
