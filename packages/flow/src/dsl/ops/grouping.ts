/**
 * Grouping a stream for aggregations (6.0, 4.1.1): KStream.groupBy and groupByKey, and the records an aggregation of
 * a grouped stream reads.
 */
import type { KGroupedStream } from '@/api/grouped.js'
import type { Grouped } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { StreamFormat } from '@/dsl/format.js'
import { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { selectKeyProcessor } from '@/operators/stateless.js'

/** Re-keys the records (the key-select node takes the grouping's name, as in Kafka Streams); always repartitioned. */
export function groupBy<K, V, KR>(
	stream: KStreamImpl<K, V>,
	selector: (key: K | null, value: V | null) => KR,
	options?: Grouped<KR, V>
): KGroupedStream<KR, V> {
	const { ctx } = stream
	const supplier = selectKeyProcessor<K | null, V | null, KR>(selector)
	const node = ctx.processor(NodePrefix.KEY_SELECT, options?.name, [stream.node], supplier)
	const format: StreamFormat = { keyCodec: options?.key, valueCodec: options?.value ?? stream.format.valueCodec }
	return new KGroupedStreamImpl(new KStreamImpl<KR, V>(ctx, node, format, true), options?.name)
}

/** Groups by the current key: repartitioned only if the keys changed upstream. */
export function groupByKey<K, V>(stream: KStreamImpl<K, V>, options?: Grouped<K, V>): KGroupedStream<K, V> {
	const { format } = stream
	const grouped: StreamFormat = {
		keyCodec: options?.key ?? format.keyCodec,
		valueCodec: options?.value ?? format.valueCodec,
		windowed: options?.key === undefined ? format.windowed : undefined,
	}
	const records = new KStreamImpl<K, V>(stream.ctx, stream.node, grouped, stream.repartitionRequired)
	return new KGroupedStreamImpl(records, options?.name)
}

/**
 * The records an aggregation of `grouped` reads (KS GroupedStreamAggregateBuilder): the grouped records, or, when
 * their keys changed upstream, what is read back from a repartition topic, written with the Grouped codecs (the
 * aggregation's key codec when Grouped has none). A named grouping repartitions once, for every aggregation built from
 * it (KF6); an unnamed one once per aggregation, as in Kafka Streams, through a topic named after the aggregation's
 * `storeName` if it has one (as KGroupedTable aggregations, 6.15).
 */
export function aggregationInput<K, V>(
	grouped: KGroupedStreamImpl<K, V>,
	keyCodec: Codec<K>,
	storeName: string | undefined
): KStreamImpl<K, V> {
	if (grouped.repartitioned !== undefined) {
		return grouped.repartitioned
	}
	const { stream, name } = grouped
	const input = repartitionIfRequired(stream, {
		name: name ?? storeName,
		owner: name === undefined ? {} : grouped,
		codecs: { key: (stream.format.keyCodec as Codec<K> | undefined) ?? keyCodec },
		operator: 'groupBy()/groupByKey()',
	})
	if (name !== undefined) {
		grouped.repartitioned = input
	}
	return input
}
