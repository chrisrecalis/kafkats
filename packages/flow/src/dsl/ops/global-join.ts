/**
 * Stream-global table joins: inner and left, on the key a selector picks (6.19). Every instance holds the whole
 * global table, so the stream is not repartitioned (a pending repartition stays pending) and its topics need no
 * co-partitioning; the join processor reads the global store without being connected to it.
 */
import { TopologyError } from '@/api/errors.js'
import type { KStream, ValueJoiner } from '@/api/kstream.js'
import type { NamedOptions } from '@/api/options.js'
import { withNewValues } from '@/dsl/format.js'
import type { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix } from '@/dsl/naming.js'
import { storeValueGetter } from '@/operators/ktable-node.js'
import { joinerWithKey, streamTableJoinProcessor } from '@/operators/stream-table-join.js'

/**
 * `(keySelector, joiner, options?)`, or the deprecated `(joiner)` that looks up the record key. The joiner
 * receives null for a missing table value (left join).
 */
export type GlobalJoinArgs<K, V, GK, GV, VR> =
	| [keySelector: (key: K | null, value: V) => GK | null, joiner: ValueJoiner<K, V, GV, VR>, options?: NamedOptions]
	| [joiner: (value: V, globalValue: GV) => VR]

export function globalJoin<K, V, GK, GV, VR>(
	stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left',
	table: GlobalKTableImpl<GK, GV>,
	...args: GlobalJoinArgs<K, V, GK, GV, VR>
): KStream<K, VR> {
	if (table.ctx !== stream.ctx) {
		throw new TopologyError(`${kind === 'inner' ? 'join()' : 'leftJoin()'} needs a global table of the same app`)
	}
	const [keySelector, joiner, options] =
		args.length === 1 ? [(key: K | null) => key as GK | null, args[0], undefined] : args
	const supplier = streamTableJoinProcessor<K, V, GK, GV, VR>({
		valueGetter: storeValueGetter(table.storeName),
		lookupKey: keySelector,
		joiner: joinerWithKey(joiner as ValueJoiner<K, V, GV, VR>) as (key: K | null, value: V, other: GV | null) => VR,
		left: kind === 'left',
	})
	const prefix = kind === 'inner' ? NodePrefix.JOIN : NodePrefix.LEFT_JOIN
	const node = stream.ctx.processor(prefix, options?.name, [stream.node], supplier)
	return new KStreamImpl<K, VR>(stream.ctx, node, withNewValues(stream.format), stream.repartitionRequired)
}
