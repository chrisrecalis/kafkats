/**
 * Repartitioning (6.0, 6.18): the helper every key-based operator calls on a stream whose keys changed upstream,
 * KStream.repartition and the deprecated KStream.through. Each writes the stream to a topic and continues with what is
 * read back from it, so that downstream operations see every record in the partition it was written to: where its
 * encoded key hashes to, where its base key hashes to for a windowed key codec (6.9), or where a given partitioner
 * puts it. Repartition topics drop records with a null key unless a relaxed join is downstream (4.1.6, KS ksdsl§15.3).
 */
import type { KStream } from '@/api/kstream.js'
import type { Produced, Repartitioned } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import { requireCodec } from '@/dsl/format.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { repartitionName, suffixed } from '@/dsl/naming.js'
import { addSink } from '@/dsl/ops/to.js'

export interface RepartitionOptions<K, V> {
	/** Names the topic `<applicationId>-<name>-repartition` and its nodes; generated (`repartition-<n>`) otherwise. */
	readonly name?: string
	/** Who claims the name: the same owner (a named grouping) gets the same topic, another one a TopologyError. */
	readonly owner: object
	/** The operator's codec options; the stream's format fills the gaps. */
	readonly codecs: { readonly key?: Codec<K>; readonly value?: Codec<V> }
	/** Names the operator in a missing-codec error. */
	readonly operator: string
}

/**
 * The stream a key-based operator works on: `stream` itself unless its keys changed upstream. Then its records are
 * written to a repartition topic and read back from it, so that each key is processed in the partition it hashes to.
 */
export function repartitionIfRequired<K, V>(
	stream: KStreamImpl<K, V>,
	options: RepartitionOptions<K, V>
): KStreamImpl<K, V> {
	if (!stream.repartitionRequired) {
		return stream
	}
	const codecs = requiredCodecs(stream, options.codecs, options.operator)
	return viaRepartitionTopic(stream, options.name, { owner: options.owner }, codecs)
}

/**
 * KStream.repartition (KS `KStreamImpl.doRepartition`): always writes and reads back a topic of its own,
 * `<applicationId>-<name | repartition-<n>>-repartition`, whether keys changed upstream or not, with the Repartitioned
 * codecs (else the stream's) and partitioner. The topic has `numberOfPartitions` partitions, else as many as the topics
 * the stream comes from (4.1.3); operations co-partitioned with it must agree with an explicit count. The result needs
 * no further repartitioning.
 */
export function repartition<K, V>(stream: KStreamImpl<K, V>, options: Repartitioned<K, V> = {}): KStream<K, V> {
	const codecs = requiredCodecs(stream, options, 'repartition()')
	const claim = { owner: {}, partitions: options.numberOfPartitions }
	return viaRepartitionTopic(stream, options.name, claim, { ...codecs, partitioner: options.partitioner })
}

/**
 * The deprecated KStream.through (removed in Kafka Streams 4.0): writes to a user topic and continues from what is
 * read back from it (from the earliest offset without a commit). Unlike a repartition topic, it keeps null keys.
 */
export function through<K, V>(stream: KStreamImpl<K, V>, topic: string, options: Produced<K, V> = {}): KStream<K, V> {
	stream.ctx.builder.markThroughTopic(topic)
	return writeAndReadBack(stream, topic, options, 'through')
}

/** The key and value codecs of the options, else of the stream; a TopologyError naming the operator without one. */
function requiredCodecs<K, V>(
	stream: KStreamImpl<K, V>,
	codecs: { readonly key?: Codec<K>; readonly value?: Codec<V> },
	operator: string
): { key: Codec<K>; value: Codec<V> } {
	return {
		key: requireCodec(codecs.key, stream.format.keyCodec, 'key', operator),
		value: requireCodec(codecs.value, stream.format.valueCodec, 'value', operator),
	}
}

/**
 * Writes `stream` to the repartition topic `<applicationId>-<name | repartition-<n>>-repartition`, claimed for `owner`,
 * and reads it back. A named repartition names its nodes after the topic, `<name>-repartition-sink` and
 * `<name>-repartition-source` (KS `KStreamImpl.createRepartitionedSource`); an unnamed one generates them.
 */
function viaRepartitionTopic<K, V>(
	stream: KStreamImpl<K, V>,
	name: string | undefined,
	claim: { readonly owner: object; readonly partitions?: number },
	produced: Produced<K, V>
): KStreamImpl<K, V> {
	const { builder } = stream.ctx
	const topic = builder.addRepartitionTopic(repartitionName(builder, name), claim.owner, claim.partitions)
	const sink = { ...produced, name: suffixed(name, 'repartition-sink') }
	return writeAndReadBack(stream, topic, sink, 'repartition', suffixed(name, 'repartition-source'))
}

/**
 * Writes `stream` to `topic` with the sink `produced.name` and returns what is read back from it by the source
 * `sourceName` (both generated without a name), decoded with the codecs it was written with (raw Buffers where neither
 * the options nor the format have one): a new stream that needs no repartitioning.
 */
function writeAndReadBack<K, V>(
	stream: KStreamImpl<K, V>,
	topic: string,
	produced: Produced<K, V>,
	internal: 'repartition' | 'through',
	sourceName?: string
): KStreamImpl<K, V> {
	const { ctx, format } = stream
	addSink(stream, topic, produced, internal === 'repartition' ? 'repartition' : false)
	const key = produced.key ?? (format.keyCodec as Codec<K> | undefined)
	const value = produced.value ?? (format.valueCodec as Codec<V> | undefined)
	return ctx.stream(topic, { key, value, name: sourceName }, internal)
}
