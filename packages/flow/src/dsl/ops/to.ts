/**
 * Sinks: KStream.to (6.20) and the sinks of repartition and through topics. Codecs: `Produced` first, then the
 * stream's format; without either, only Buffers can be written (the sink fails on anything else).
 */
import type { Produced } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import type { SinkDef, StreamPartitioner, TopicNameExtractor } from '@/topology/nodes.js'

export function to<K, V>(
	stream: KStreamImpl<K, V>,
	topic: string | TopicNameExtractor<K | null, V | null>,
	options?: Produced<K, V>
): void {
	addSink(stream, topic, options, false)
}

/** Registers the sink `options.name ?? KSTREAM-SINK-<n>` of `stream`. */
export function addSink<K, V>(
	stream: KStreamImpl<K, V>,
	topic: string | TopicNameExtractor<K | null, V | null>,
	options: Produced<K, V> | undefined,
	internal: SinkDef['internal']
): void {
	const { builder } = stream.ctx
	const partitioner = options?.partitioner
	const sinkPartitioner: StreamPartitioner<K | null, V | null> | undefined =
		partitioner && ((_topic, key, value, count) => partitioner(key, value, count))
	builder.addSink({
		name: builder.nodeName(NodePrefix.SINK, options?.name),
		parents: [stream.node],
		topic: topic as SinkDef['topic'],
		keyCodec: options?.key ?? stream.format.keyCodec,
		valueCodec: options?.value ?? stream.format.valueCodec,
		partitioner: sinkPartitioner as SinkDef['partitioner'],
		internal,
	})
}
