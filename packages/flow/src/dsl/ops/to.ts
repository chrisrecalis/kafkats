/** Sinks: KStream.to (6.20). */
import type { Produced } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { TopicNameExtractor } from '@/topology/nodes.js'

export function to<K, V>(
	_stream: KStreamImpl<K, V>,
	_topic: string | TopicNameExtractor<K | null, V | null>,
	_options?: Produced<K, V>
): void {
	notImplemented('KStream.to')
}
