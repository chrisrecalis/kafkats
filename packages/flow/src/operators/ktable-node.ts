/**
 * What the DSL keeps of a table node (KS KTableProcessorSupplier, 6.11): its processor, whether it can send old
 * values, and how a consuming processor reads the table's current value of a key (the value getter, whose stores
 * are connected to that processor).
 */
import type { Change } from '@/operators/change.js'
import type { InternalProcessorContext, MaybePromise, ProcessorSupplier } from '@/processor/api.js'
import type { TimestampedKeyValueStore, ValueAndTimestamp } from '@/state.js'

/** Reads the current value of a key of a table, from its store or recomputed from its parent's getter. */
export interface KTableValueGetter<K, V> {
	init(context: InternalProcessorContext<unknown, unknown>): MaybePromise<void>
	get(key: K): Promise<ValueAndTimestamp<V> | null>
	close?(): MaybePromise<void>
}

export interface KTableValueGetterSupplier<K, V> {
	/** Connected to every processor that creates a getter. */
	readonly storeNames: readonly string[]
	create(): KTableValueGetter<K, V>
}

/** What the DSL keeps of a table node: a plain descriptor, not a callable object. */
export interface KTableNode<K, V> {
	/** Its input: the parent table's changes, the records of a source, or the grouped records an aggregation reads. */
	readonly supplier: ProcessorSupplier<unknown, unknown, K, Change<V>>
	/** Kafka Streams' enableSendingOldValues: false if old values need a store this node does not have and !force. */
	enableSendingOldValues(force: boolean): boolean
	readonly valueGetter: KTableValueGetterSupplier<K, V>
}

/** The getter of a materialized table: a point read of its timestamped store. */
export function storeValueGetter<K, V>(storeName: string): KTableValueGetterSupplier<K, V> {
	return {
		storeNames: [storeName],
		create: () => {
			let store: TimestampedKeyValueStore<K, V>
			return {
				init: context => {
					store = context.getStateStore(storeName)
				},
				get: async key => (await store.get(key)) ?? null,
			}
		},
	}
}
