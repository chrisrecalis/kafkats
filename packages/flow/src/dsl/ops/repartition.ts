/** Repartitioning: KStream.repartition and the deprecated KStream.through (6.18). */
import type { KStream } from '@/api/kstream.js'
import type { Produced, Repartitioned } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function repartition<K, V>(_stream: KStreamImpl<K, V>, _options?: Repartitioned<K, V>): KStream<K, V> {
	return notImplemented('KStream.repartition')
}

export function through<K, V>(_stream: KStreamImpl<K, V>, _topic: string, _options?: Produced<K, V>): KStream<K, V> {
	return notImplemented('KStream.through')
}
