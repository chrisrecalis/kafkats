/**
 * Repartitioning (6.0, 6.18): the helper every key-based operator calls on a stream whose keys changed upstream, the
 * deprecated KStream.through and KStream.repartition.
 */
import type { KStream } from '@/api/kstream.js'
import type { Produced, Repartitioned } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import { requireCodec } from '@/dsl/format.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { repartitionName } from '@/dsl/naming.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import { addSink } from '@/dsl/ops/to.js'

export interface RepartitionOptions<K, V> {
	/** Names the topic `<applicationId>-<name>-repartition`; generated (`repartition-<n>`) otherwise. */
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
 * written to a repartition topic and read back from it, so that each key is processed in the partition it hashes
 * to (null keys are dropped by the sink unless a relaxed join is downstream, 4.1.6). The read-back is a new stream
 * that needs no repartitioning.
 */
export function repartitionIfRequired<K, V>(
	stream: KStreamImpl<K, V>,
	options: RepartitionOptions<K, V>
): KStreamImpl<K, V> {
	if (!stream.repartitionRequired) {
		return stream
	}
	const key = requireCodec(options.codecs.key, stream.format.keyCodec, 'key', options.operator)
	const value = requireCodec(options.codecs.value, stream.format.valueCodec, 'value', options.operator)
	const { ctx } = stream
	const topic = ctx.builder.addRepartitionTopic(repartitionName(ctx.builder, options.name), options.owner)
	addSink(stream, topic, { key, value }, 'repartition')
	return ctx.stream(topic, { key, value }, 'repartition')
}

export function repartition<K, V>(_stream: KStreamImpl<K, V>, _options?: Repartitioned<K, V>): KStream<K, V> {
	return notImplemented('KStream.repartition')
}

/**
 * The deprecated KStream.through: writes to a user topic and continues from what is read back from it (from the
 * earliest offset without a commit), so downstream operations see each record in the partition it was written to.
 */
export function through<K, V>(stream: KStreamImpl<K, V>, topic: string, options?: Produced<K, V>): KStream<K, V> {
	const { ctx, format } = stream
	addSink(stream, topic, options, false)
	ctx.builder.markThroughTopic(topic)
	const key = options?.key ?? (format.keyCodec as Codec<K> | undefined)
	const value = options?.value ?? (format.valueCodec as Codec<V> | undefined)
	return ctx.stream(topic, { key, value }, 'through')
}
