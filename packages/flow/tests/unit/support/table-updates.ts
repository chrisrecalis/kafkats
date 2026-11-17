import type { KTable } from '@/api/ktable.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import type { Change } from '@/operators/change.js'
import type { StreamRecord } from '@/processor/record.js'

/** An update a table forwarded: `[key, newValue, oldValue, timestamp]`. */
export type Update<K, V> = [K, V | null, V | null, number]

/** Every update a table forwards, collected by a processor registered under its node. */
export function updatesOf<K, V>(table: KTable<K, V>): Update<K, V>[] {
	const impl = table as KTableImpl<K, V>
	const updates: Update<K, V>[] = []
	impl.ctx.processor('UPDATES', undefined, [impl.node], () => ({
		process: ({ key, value, timestamp }: StreamRecord<K, Change<V>>) =>
			void updates.push([key, value.newValue, value.oldValue, timestamp]),
	}))
	return updates
}

/** The old values of the updates. */
export const oldValues = <K, V>(updates: readonly Update<K, V>[]): (V | null)[] => updates.map(update => update[2])
