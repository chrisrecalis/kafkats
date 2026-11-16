/** KTable.groupBy and the KGroupedTable aggregations (6.15). */
import type { KGroupedTable } from '@/api/grouped.js'
import type { KeyValue } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions, Grouped } from '@/api/options.js'
import type { KGroupedTableImpl } from '@/dsl/kgrouped-table.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function groupBy<K, V, KR, VR>(
	_table: KTableImpl<K, V>,
	_selector: (key: K, value: V) => KeyValue<KR, VR>,
	_options?: Grouped<KR, VR>
): KGroupedTable<KR, VR> {
	return notImplemented('KTable.groupBy')
}

export function count<K, V>(
	_grouped: KGroupedTableImpl<K, V>,
	_options?: AggregateOptions<K, number>
): KTable<K, number> {
	return notImplemented('KGroupedTable.count')
}

export function reduce<K, V>(
	_grouped: KGroupedTableImpl<K, V>,
	_adder: (aggregate: V, value: V) => V,
	_subtractor: (aggregate: V, value: V) => V,
	_options?: AggregateOptions<K, V>
): KTable<K, V> {
	return notImplemented('KGroupedTable.reduce')
}

export function aggregate<K, V, A>(
	_grouped: KGroupedTableImpl<K, V>,
	_initializer: () => A,
	_adder: (key: K, value: V, aggregate: A) => A,
	_subtractor: (key: K, value: V, aggregate: A) => A,
	_options?: AggregateOptions<K, A>
): KTable<K, A> {
	return notImplemented('KGroupedTable.aggregate')
}
