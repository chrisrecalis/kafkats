/** Table filter, filterNot and mapValues (6.12). */
import type { ValueMapper } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { TableOptions } from '@/api/options.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function filter<K, V>(
	_table: KTableImpl<K, V>,
	_predicate: (key: K, value: V) => boolean,
	_options?: TableOptions<K, V>
): KTable<K, V> {
	return notImplemented('KTable.filter')
}

export function filterNot<K, V>(
	_table: KTableImpl<K, V>,
	_predicate: (key: K, value: V) => boolean,
	_options?: TableOptions<K, V>
): KTable<K, V> {
	return notImplemented('KTable.filterNot')
}

export function mapValues<K, V, VR>(
	_table: KTableImpl<K, V>,
	_mapper: ValueMapper<K, V, VR | null>,
	_options?: TableOptions<K, VR>
): KTable<K, VR> {
	return notImplemented('KTable.mapValues')
}
