import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'
import type { Codec } from '@/codec.js'
import type { KeyValue } from '@/types.js'

/**
 * Processor node that maintains table state in a KeyValueStore.
 * Intercepts records and stores them, then forwards downstream.
 */
export class TableStateNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, V> | null }
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new TableStateNode<K, V>(this.storeName, this.storeRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Table state store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key !== null) {
			// Handle tombstones: null value means delete from table
			if (record.value === null) {
				await store.delete(key)
			} else {
				await store.put(key, record.value)
			}
		}

		await this.forward(record)
	}
}

/**
 * Processor node for KTable.groupBy() that handles retractions.
 * When a source table row changes, this node:
 * 1. Emits a tombstone for the old grouped key (if key changed)
 * 2. Emits the new key-value pair
 * 3. Tracks source-key -> grouped-key mappings in a state store
 */
export class TableGroupByNode<K, V, K2> extends Processor<K, V, K2, V> {
	constructor(
		private readonly fn: (key: K, value: V) => KeyValue<K2, V>,
		private readonly keyMappingStoreRef: { store: KeyValueStore<K, K2> | null },
		private readonly sourceKeyCodec: Codec<K>,
		private readonly groupedKeyCodec: Codec<K2>
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K2, V> {
		void worker
		return new TableGroupByNode<K, V, K2>(
			this.fn,
			this.keyMappingStoreRef,
			this.sourceKeyCodec,
			this.groupedKeyCodec
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.keyMappingStoreRef.store
		if (!store) {
			throw new Error('Key mapping store not initialized for table groupBy')
		}

		const sourceKey = record.key
		if (sourceKey === null) {
			return
		}

		const previousGroupedKey = await store.get(sourceKey)

		// Handle tombstone: source row deleted
		if (record.value === null) {
			if (previousGroupedKey !== undefined) {
				const tombstone: StreamRecord<K2, V> = {
					...record,
					key: previousGroupedKey,
					value: null,
				}
				await this.forward(tombstone)
				await store.delete(sourceKey)
			}
			return
		}

		const [newGroupedKey, newValue] = this.fn(sourceKey, record.value)

		// If the grouping key changed, retract the previous grouped key.
		if (previousGroupedKey !== undefined) {
			const prevKeyBytes = this.groupedKeyCodec.encode(previousGroupedKey)
			const newKeyBytes = this.groupedKeyCodec.encode(newGroupedKey)
			if (!prevKeyBytes.equals(newKeyBytes)) {
				const tombstone: StreamRecord<K2, V> = {
					...record,
					key: previousGroupedKey,
					value: null,
				}
				await this.forward(tombstone)
			}
		}

		await store.put(sourceKey, newGroupedKey)

		const next: StreamRecord<K2, V> = {
			...record,
			key: newGroupedKey,
			value: newValue,
		}
		await this.forward(next)
	}
}
