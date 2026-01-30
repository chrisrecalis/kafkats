import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'

/**
 * Processor node for Table-Table inner join.
 * When a record arrives from one table, looks up the other table and emits a join result.
 */
export class TableTableJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly otherStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new TableTableJoinNode<K, V1, V2, VR>(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.otherStoreRef.store
		if (!store) {
			throw new Error('Other table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}
		if (record.value === null) {
			const next: StreamRecord<K, VR> = { ...record, value: null }
			await this.forward(next)
			return
		}

		const otherValue = await store.get(key)
		if (otherValue === undefined) {
			// Inner join: skip if no matching record in other table
			return
		}

		const joinedValue = this.joiner(record.value, otherValue)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next)
	}
}

/**
 * Processor node for Table-Table left join.
 */
export class TableTableLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly otherStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new TableTableLeftJoinNode<K, V1, V2, VR>(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.otherStoreRef.store
		if (!store) {
			throw new Error('Other table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}
		if (record.value === null) {
			const next: StreamRecord<K, VR> = { ...record, value: null }
			await this.forward(next)
			return
		}

		const otherValue = await store.get(key)
		const joinedValue = this.joiner(record.value, otherValue ?? null)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next)
	}
}

/**
 * Processor node for Table-Table outer join from the "right" side.
 * Used when the right table updates and needs to look up the left table.
 */
export class TableTableOuterJoinRightNode<K, V1, V2, VR> extends Processor<K, V2, K, VR> {
	constructor(
		private readonly leftStoreRef: { store: KeyValueStore<K, V1> | null },
		private readonly joiner: (leftValue: V1 | null, rightValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V2, K, VR> {
		void worker
		return new TableTableOuterJoinRightNode<K, V1, V2, VR>(this.leftStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V2>): Promise<void> {
		const store = this.leftStoreRef.store
		if (!store) {
			throw new Error('Left table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}
		if (record.value === null) {
			const next: StreamRecord<K, VR> = { ...record, value: null }
			await this.forward(next)
			return
		}

		const leftValue = await store.get(key)
		const joinedValue = this.joiner(leftValue ?? null, record.value)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next)
	}
}
