/** Stream-table joins: inner and left (6.14). */
import type { KStream, ValueJoiner } from '@/api/kstream.js'
import type { Joined } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

/** The joiner receives null for a missing table value (left join) and the key may then be null. */
export type StreamTableJoinArgs<K, V, VT, VR> = [joiner: ValueJoiner<K, V, VT, VR>, options?: Joined<K, V, VT>]

export function streamTableJoin<K, V, VT, VR>(
	_stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left',
	_table: KTableImpl<K, VT>,
	..._args: StreamTableJoinArgs<K, V, VT, VR>
): KStream<K, VR> {
	return notImplemented(`KStream ${kind} join with a KTable`)
}
