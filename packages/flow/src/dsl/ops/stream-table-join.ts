/**
 * Stream-table joins: inner and left (6.14). The stream is repartitioned first if its keys changed upstream; the
 * join processor reads the table through its value getter, whose stores it is connected to, so the stream and the
 * table end in one subtopology and their topics must be co-partitioned (4.1.3).
 */
import { TopologyError } from '@/api/errors.js'
import type { KStream, ValueJoiner } from '@/api/kstream.js'
import type { Joined } from '@/api/options.js'
import type { StreamFormat } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { joinerWithKey, streamTableJoinProcessor } from '@/operators/stream-table-join.js'

/** The joiner receives null for a missing table value (left join) and the key may then be null. */
export type StreamTableJoinArgs<K, V, VT, VR> = [joiner: ValueJoiner<K, V, VT, VR>, options?: Joined<K, V, VT>]

export function streamTableJoin<K, V, VT, VR>(
	stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left',
	table: KTableImpl<K, VT>,
	...[joiner, options = {}]: StreamTableJoinArgs<K, V, VT, VR>
): KStream<K, VR> {
	const operator = kind === 'inner' ? 'join()' : 'leftJoin()'
	if (table.ctx !== stream.ctx) {
		throw new TopologyError(`${operator} needs a table of the same app`)
	}
	const { name, key, value } = options
	const self = repartitionIfRequired(stream, { name, owner: {}, codecs: { key, value }, operator })
	const { valueGetter } = table.table
	const supplier = streamTableJoinProcessor<K, V, K, VT, VR>({
		valueGetter,
		lookupKey: recordKey => recordKey,
		joiner: joinerWithKey(joiner) as (key: K | null, value: V, tableValue: VT | null) => VR,
		left: kind === 'left',
	})
	const prefix = kind === 'inner' ? NodePrefix.JOIN : NodePrefix.LEFT_JOIN
	const node = self.ctx.processor(prefix, name, [self.node], supplier, valueGetter.storeNames)
	return new KStreamImpl<K, VR>(self.ctx, node, joinedFormat(self.format, key), false)
}

/** Results keep the stream's keys: the key codec of the options or the stream (with its windows). */
function joinedFormat(format: StreamFormat, key: StreamFormat['keyCodec']): StreamFormat {
	return key === undefined ? { keyCodec: format.keyCodec, windowed: format.windowed } : { keyCodec: key }
}
