import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'
import type { Codec } from '@/codec.js'
import type { KeyValue } from '@/types.js'

// Delta operation header used by table groupBy to signal add/subtract semantics
const DELTA_OP_HEADER = '__kafkats_delta_op'
const DELTA_ADD = Buffer.from('add')
const DELTA_SUB = Buffer.from('sub')

export function isDeltaAdd(record: StreamRecord<unknown, unknown>): boolean {
	return record.headers[DELTA_OP_HEADER]?.equals(DELTA_ADD) ?? false
}

export function isDeltaSub(record: StreamRecord<unknown, unknown>): boolean {
	return record.headers[DELTA_OP_HEADER]?.equals(DELTA_SUB) ?? false
}

/**
 * Mapping stored for each source key in a table groupBy.
 * Tracks both the grouped key and the source value so retractions
 * can emit the correct old value for delta-aware downstream nodes.
 */
export type GroupedTableMapping<K, V> = {
	groupedKey: K
	value: V
}

export function groupedTableMappingCodec<K, V>(
	keyCodec: Codec<K>,
	valueCodec: Codec<V>
): Codec<GroupedTableMapping<K, V>> {
	return {
		encode(mapping: GroupedTableMapping<K, V>): Buffer {
			const keyBuf = keyCodec.encode(mapping.groupedKey)
			const valBuf = valueCodec.encode(mapping.value)
			const lenBuf = Buffer.alloc(4)
			lenBuf.writeUInt32BE(keyBuf.length, 0)
			return Buffer.concat([lenBuf, keyBuf, valBuf])
		},
		decode(buffer: Buffer): GroupedTableMapping<K, V> {
			const keyLen = buffer.readUInt32BE(0)
			const keyBuf = buffer.subarray(4, 4 + keyLen)
			const valBuf = buffer.subarray(4 + keyLen)
			return {
				groupedKey: keyCodec.decode(keyBuf),
				value: valueCodec.decode(valBuf),
			}
		},
	}
}

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
 * Emits delta-tagged records (add/sub headers) so downstream aggregation
 * nodes can properly handle retractions.
 *
 * When a source table row changes, this node:
 * 1. Emits a SUB record with old grouped key + old value (retraction)
 * 2. Emits an ADD record with new grouped key + new value
 * 3. Updates the source-key → {groupedKey, value} mapping store
 */
export class TableGroupByNode<K, V, K2> extends Processor<K, V, K2, V> {
	constructor(
		private readonly fn: (key: K, value: V) => KeyValue<K2, V>,
		private readonly keyMappingStoreRef: { store: KeyValueStore<K, GroupedTableMapping<K2, V>> | null },
		private readonly groupedKeyCodec: Codec<K2>
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K2, V> {
		void worker
		return new TableGroupByNode<K, V, K2>(this.fn, this.keyMappingStoreRef, this.groupedKeyCodec)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.keyMappingStoreRef.store
		if (!store) {
			throw new Error('Key mapping store not initialized for table groupBy')
		}

		const sourceKey = record.key
		if (sourceKey === null) return

		const previousMapping = await store.get(sourceKey)

		// Handle tombstone: source row deleted
		if (record.value === null) {
			if (previousMapping !== undefined) {
				// Emit SUB for old grouped key with old value
				await this.forward({
					...record,
					key: previousMapping.groupedKey,
					value: previousMapping.value,
					headers: { ...record.headers, [DELTA_OP_HEADER]: DELTA_SUB },
				})
				await store.delete(sourceKey)
			}
			return
		}

		const [newGroupedKey, newValue] = this.fn(sourceKey, record.value)

		// Retract old mapping if it exists
		if (previousMapping !== undefined) {
			await this.forward({
				...record,
				key: previousMapping.groupedKey,
				value: previousMapping.value,
				headers: { ...record.headers, [DELTA_OP_HEADER]: DELTA_SUB },
			})
		}

		// Store new mapping
		await store.put(sourceKey, { groupedKey: newGroupedKey, value: newValue })

		// Emit ADD for the new value
		await this.forward({
			...record,
			key: newGroupedKey,
			value: newValue,
			headers: { ...record.headers, [DELTA_OP_HEADER]: DELTA_ADD },
		})
	}
}

/**
 * Delta-aware count node for table groupBy.
 * Increments on ADD, decrements on SUB.
 * Deletes the key when count reaches zero.
 */
export class TableDeltaCountNode<K, V> extends Processor<K, V, K, number> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, number> | null }
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, number> {
		void worker
		return new TableDeltaCountNode<K, V>(this.storeName, this.storeRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null || record.value === null) return

		const current = (await store.get(key)) ?? 0

		let newCount: number
		if (isDeltaSub(record)) {
			newCount = current - 1
		} else {
			newCount = current + 1
		}

		if (newCount <= 0) {
			await store.delete(key)
			// Emit tombstone for downstream
			await this.forward({ ...record, key, value: null as unknown as number, headers: {} })
		} else {
			await store.put(key, newCount)
			await this.forward({ ...record, key, value: newCount, headers: {} })
		}
	}
}

/**
 * Delta-aware reduce node for table groupBy.
 * Applies adder on ADD, subtractor on SUB.
 * Requires both adder and subtractor functions.
 */
export class TableDeltaReduceNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, V> | null },
		private readonly adder: (aggregate: V, value: V) => V,
		private readonly subtractor: (aggregate: V, value: V) => V
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new TableDeltaReduceNode<K, V>(this.storeName, this.storeRef, this.adder, this.subtractor)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null || record.value === null) return

		const current = await store.get(key)

		let newAggregate: V
		if (isDeltaSub(record)) {
			if (current === undefined) return
			newAggregate = this.subtractor(current, record.value)
		} else {
			newAggregate = current === undefined ? record.value : this.adder(current, record.value)
		}

		await store.put(key, newAggregate)
		await this.forward({ ...record, key, value: newAggregate, headers: {} })
	}
}

/**
 * Delta-aware aggregate node for table groupBy.
 * Applies aggregator on ADD, subtractor on SUB.
 * Requires both aggregator and subtractor functions.
 */
export class TableDeltaAggregateNode<K, V, A> extends Processor<K, V, K, A> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly subtractor: (key: K, value: V, aggregate: A) => A
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, A> {
		void worker
		return new TableDeltaAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.subtractor
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null || record.value === null) return

		const current = (await store.get(key)) ?? this.initializer()

		let newAggregate: A
		if (isDeltaSub(record)) {
			newAggregate = this.subtractor(key, record.value, current)
		} else {
			newAggregate = this.aggregator(key, record.value, current)
		}

		await store.put(key, newAggregate)
		await this.forward({ ...record, key, value: newAggregate, headers: {} })
	}
}
