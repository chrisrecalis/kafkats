/**
 * Stream-global table joins: inner and left, on the key a selector picks (6.19, KS KStreamGlobalKTableJoin). Every
 * instance holds the whole global table, so the stream is not repartitioned (a pending repartition stays pending) and
 * its topics need no co-partitioning; the join processor reads the global store without being connected to it. A left
 * join joins records whose selector picks no key with null (KIP-962), so a repartition topic upstream keeps null keys
 * (4.1.6). The deprecated two-argument form looks up the record key (DL21).
 */
import { TopologyError } from '@/api/errors.js'
import type { KeyValueMapper, KStream, ValueJoinerWithKey } from '@/api/kstream.js'
import type { NamedOptions } from '@/api/options.js'
import { withNewValues } from '@/dsl/format.js'
import type { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import { storeValueGetter } from '@/operators/ktable-node.js'
import { streamTableJoinProcessor } from '@/operators/stream-table-join.js'

/**
 * `joiner` gets the record's key (`join()` and `leftJoin()` pass one that ignores it); a left join's joiner receives
 * null values. `operator`: the method called, as the topology errors name it.
 */
export function globalJoin<K, V, GK, GV, VR>(
	stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left',
	operator: string,
	table: GlobalKTableImpl<GK, GV>,
	keySelector: KeyValueMapper<K | null, V, GK | null>,
	joiner: ValueJoinerWithKey<K | null, V, GV | null, VR>,
	options?: NamedOptions
): KStream<K, VR> {
	if (table.ctx !== stream.ctx) {
		throw new TopologyError(`${operator} needs a global table of the same app`)
	}
	const supplier = streamTableJoinProcessor<K, V, GK, GV, VR>({
		valueGetter: storeValueGetter(table.storeName),
		lookupKey: keySelector,
		joiner,
		left: kind === 'left',
	})
	const prefix = kind === 'inner' ? NodePrefix.JOIN : NodePrefix.LEFT_JOIN
	const marks = kind === 'left' ? { relaxedNullKeyJoin: true as const } : {}
	const node = stream.ctx.processor(prefix, options?.name, [stream.node], supplier, [], marks)
	return new KStreamImpl<K, VR>(stream.ctx, node, withNewValues(stream.format), stream.repartitionRequired)
}
