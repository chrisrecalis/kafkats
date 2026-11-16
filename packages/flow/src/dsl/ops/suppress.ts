/** KTable.suppress (6.16). */
import type { KTable } from '@/api/ktable.js'
import type { SuppressOptions } from '@/api/options.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

export function suppress<K, V>(_table: KTableImpl<K, V>, _options: SuppressOptions): KTable<K, V> {
	return notImplemented('KTable.suppress')
}
