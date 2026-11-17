/**
 * Sinks: KStream.to (6.20) and the sinks of repartition and through topics. Codecs: `Produced` first, then the
 * stream's format; without either, only Buffers can be written (the sink fails on anything else). Partitions: the
 * given partitioner, else the base key's partition for windowed keys (6.9), else the producer's default.
 */
import type { Produced, RecordPartitioner } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import { isWindowedCodec, windowedPartitioner } from '@/operators/windowed.js'
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
	const keyCodec = options?.key ?? stream.format.keyCodec
	builder.addSink({
		name: builder.nodeName(NodePrefix.SINK, options?.name),
		parents: [stream.node],
		topic: topic as SinkDef['topic'],
		keyCodec,
		valueCodec: options?.value ?? stream.format.valueCodec,
		partitioner: sinkPartitioner(options?.partitioner, keyCodec),
		internal,
	})
}

/**
 * The given partitioner; without one, a windowed key codec gets the windowed partitioner (KS `StreamSinkNode`), so
 * windowed records go to the partition of their base key. Kafka Streams does so only at `to()`; flow at repartition
 * sinks too, so every windowed topic it writes is co-partitioned by base key (KF15).
 */
function sinkPartitioner<K, V>(
	partitioner: RecordPartitioner<K, V> | undefined,
	keyCodec: Codec<unknown> | undefined
): SinkDef['partitioner'] {
	if (partitioner) {
		const given: StreamPartitioner<K | null, V | null> = (_topic, key, value, count) =>
			partitioner(key, value, count)
		return given as SinkDef['partitioner']
	}
	return isWindowedCodec(keyCodec) ? (windowedPartitioner(keyCodec) as SinkDef['partitioner']) : undefined
}
