import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'

/**
 * Processor node for Stream-Table inner join.
 * For each stream record, looks up the table value and applies the joiner.
 */
export class StreamTableJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly tableStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamTableJoinNode<K, V1, V2, VR>(this.tableStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.tableStoreRef.store
		if (!store) {
			throw new Error('Table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys
			return
		}
		if (record.value === null) {
			// Skip tombstones from the stream side
			return
		}

		const tableValue = await store.get(key)
		if (tableValue === undefined) {
			// Inner join: skip if no matching table record
			return
		}

		const joinedValue = this.joiner(record.value, tableValue)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next)
	}
}

/**
 * Processor node for Stream-Table left join.
 * For each stream record, looks up the table value (may be null) and applies the joiner.
 */
export class StreamTableLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly tableStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamTableLeftJoinNode<K, V1, V2, VR>(this.tableStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.tableStoreRef.store
		if (!store) {
			throw new Error('Table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys
			return
		}
		if (record.value === null) {
			// Skip tombstones from the stream side
			return
		}

		const tableValue = await store.get(key)
		const joinedValue = this.joiner(record.value, tableValue ?? null)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next)
	}
}
