import { Processor, requireStore, type StoreRef, type StreamRecord } from '@/legacy/processors/base.js'
import type { KeyValueStore } from '@/state.js'
import type { Codec } from '@/codec.js'
import type { KeyValue } from '@/legacy/types.js'

// Delta operation header used by table groupBy to signal add/subtract semantics
const DELTA_OP_HEADER = '__kafkats_delta_op'
const DELTA_ADD = Buffer.from('add')
const DELTA_SUB = Buffer.from('sub')

function isDeltaSub(record: StreamRecord<unknown, unknown>): boolean {
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
		private readonly storeRef: StoreRef<KeyValueStore<K, V>>
	) {
		super()
	}

	clone(): Processor<K, V, K, V> {
		return new TableStateNode<K, V>(this.storeName, this.storeRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const key = record.key
		// Capture the prior value before mutating so downstream join nodes can tell whether a join
		// result previously existed (and thus whether a tombstone is a real retraction, not a no-op).
		let oldValue: V | null = null
		if (key !== null) {
			oldValue = (await store.get(key)) ?? null
			if (record.value === null) {
				await store.delete(key)
			} else {
				await store.put(key, record.value)
			}
		}

		await this.forward({ ...record, oldValue })
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
 *
 * The deltas are meant to be repartitioned by the grouped key before they are aggregated.
 */
export class TableGroupByNode<K, V, K2> extends Processor<K, V, K2, V> {
	constructor(
		private readonly fn: (key: K, value: V) => KeyValue<K2, V>,
		private readonly keyMappingStoreRef: StoreRef<KeyValueStore<K, GroupedTableMapping<K2, V>>>
	) {
		super()
	}

	clone(): Processor<K, V, K2, V> {
		return new TableGroupByNode<K, V, K2>(this.fn, this.keyMappingStoreRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.keyMappingStoreRef, 'table groupBy key mapping')
		const sourceKey = record.key
		if (sourceKey === null) return

		const previousMapping = await store.get(sourceKey)
		const added = record.value === null ? undefined : this.fn(sourceKey, record.value)

		// Deltas are forwarded BEFORE the mapping is updated. If processing is interrupted in
		// between, the replay sees the old mapping and emits the same deltas again (at-least-once);
		// updating first would make the replay emit a net-zero pair and lose the change for good.
		if (previousMapping !== undefined) {
			await this.forward({
				...record,
				key: previousMapping.groupedKey,
				value: previousMapping.value,
				headers: { ...record.headers, [DELTA_OP_HEADER]: DELTA_SUB },
			})
		}

		// Source row deleted
		if (added === undefined) {
			if (previousMapping !== undefined) {
				await store.delete(sourceKey)
			}
			return
		}

		const [groupedKey, value] = added
		await this.forward({
			...record,
			key: groupedKey,
			value,
			headers: { ...record.headers, [DELTA_OP_HEADER]: DELTA_ADD },
		})
		await store.put(sourceKey, { groupedKey, value })
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
		private readonly storeRef: StoreRef<KeyValueStore<K, number>>
	) {
		super()
	}

	clone(): Processor<K, V, K, number> {
		return new TableDeltaCountNode<K, V>(this.storeName, this.storeRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const key = record.key
		if (key === null || record.value === null) return

		const count = ((await store.get(key)) ?? 0) + (isDeltaSub(record) ? -1 : 1)
		if (count <= 0) {
			await store.delete(key)
			await this.forward({ ...record, key, value: null, headers: {} })
		} else {
			await store.put(key, count)
			await this.forward({ ...record, key, value: count, headers: {} })
		}
	}
}

/**
 * Delta-aware aggregation for table groupBy: applies `add` on ADD and `subtract` on SUB. A SUB
 * for a group that has no aggregate is ignored.
 */
class TableDeltaAggregationNode<K, V, A> extends Processor<K, V, K, A> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: StoreRef<KeyValueStore<K, A>>,
		private readonly add: (key: K, value: V, current: A | undefined) => A,
		private readonly subtract: (key: K, value: V, current: A) => A,
		/** Number of rows currently in each group, so an emptied group can be deleted. */
		private readonly membersRef?: StoreRef<KeyValueStore<K, number>>
	) {
		super()
	}

	clone(): Processor<K, V, K, A> {
		return new TableDeltaAggregationNode(this.storeName, this.storeRef, this.add, this.subtract, this.membersRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const key = record.key
		if (key === null || record.value === null) return

		const current = await store.get(key)
		const sub = isDeltaSub(record)
		let newAggregate: A
		if (sub) {
			if (current === undefined) return
			newAggregate = this.subtract(key, record.value, current)
		} else {
			newAggregate = this.add(key, record.value, current)
		}

		let groupEmpty = false
		const members = this.membersRef?.store ?? null
		if (members) {
			const count = ((await members.get(key)) ?? 0) + (sub ? -1 : 1)
			if (count <= 0) {
				groupEmpty = true
				await members.delete(key)
			} else {
				await members.put(key, count)
			}
		}

		// An emptied group (or a null result, e.g. the last member was subtracted out) is deleted and
		// forwarded as a tombstone rather than kept as a "zero" aggregate, matching count(). Storing a
		// null instead would leak it into the next ADD's add(current) call.
		if (groupEmpty || newAggregate === null || newAggregate === undefined) {
			await store.delete(key)
			await this.forward({ ...record, key, value: null, headers: {} })
			return
		}

		await store.put(key, newAggregate)
		await this.forward({ ...record, key, value: newAggregate, headers: {} })
	}
}

/** KGroupedTable.reduce(): adder on ADD, subtractor on SUB. */
export class TableDeltaReduceNode<K, V> extends TableDeltaAggregationNode<K, V, V> {
	constructor(
		storeName: string,
		storeRef: StoreRef<KeyValueStore<K, V>>,
		adder: (aggregate: V, value: V) => V,
		subtractor: (aggregate: V, value: V) => V
	) {
		super(
			storeName,
			storeRef,
			(_key, value, current) => (current === undefined ? value : adder(current, value)),
			(_key, value, current) => subtractor(current, value)
		)
	}
}

/** KGroupedTable.aggregate(): aggregator on ADD, subtractor on SUB. */
export class TableDeltaAggregateNode<K, V, A> extends TableDeltaAggregationNode<K, V, A> {
	constructor(
		storeName: string,
		storeRef: StoreRef<KeyValueStore<K, A>>,
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		subtractor: (key: K, value: V, aggregate: A) => A,
		membersRef?: StoreRef<KeyValueStore<K, number>>
	) {
		super(
			storeName,
			storeRef,
			(key, value, current) => aggregator(key, value, current ?? initializer()),
			subtractor,
			membersRef
		)
	}
}
