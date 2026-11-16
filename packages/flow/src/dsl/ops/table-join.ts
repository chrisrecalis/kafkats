/** Table-table joins: inner, left and outer (6.13). */
import type { KTable } from '@/api/ktable.js'
import type { Joined, TableOptions } from '@/api/options.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

/** The joiner receives null for the missing side of left and outer results. */
export function tableJoin<K, V, VO, VR>(
	_table: KTableImpl<K, V>,
	kind: 'inner' | 'left' | 'outer',
	_other: KTable<K, VO>,
	_joiner: (value: V, otherValue: VO) => VR,
	_options?: TableOptions<K, VR> | Joined<K, V, VO>
): KTable<K, VR> {
	return notImplemented(`KTable ${kind} join`)
}
