/** Stream-global table joins: inner and left, on the key a selector picks (6.19). */
import type { KStream, ValueJoiner } from '@/api/kstream.js'
import type { NamedOptions } from '@/api/options.js'
import type { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'

/**
 * `(keySelector, joiner, options?)`, or the deprecated `(joiner)` that looks up the record key. The joiner
 * receives null for a missing table value (left join).
 */
export type GlobalJoinArgs<K, V, GK, GV, VR> =
	| [keySelector: (key: K | null, value: V) => GK | null, joiner: ValueJoiner<K, V, GV, VR>, options?: NamedOptions]
	| [joiner: (value: V, globalValue: GV) => VR]

export function globalJoin<K, V, GK, GV, VR>(
	_stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left',
	_table: GlobalKTableImpl<GK, GV>,
	..._args: GlobalJoinArgs<K, V, GK, GV, VR>
): KStream<K, VR> {
	return notImplemented(`KStream ${kind} join with a GlobalKTable`)
}
