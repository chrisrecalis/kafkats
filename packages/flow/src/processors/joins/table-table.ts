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
 * Processor node for a Table-Table left join driven by the RIGHT (secondary) table.
 *
 * The result only exists when the left (primary) row exists, so this node emits nothing
 * when there is no left row. Crucially, a right-side tombstone re-emits joiner(left, null)
 * rather than deleting the result, because the left row still exists.
 */
export class TableTableLeftJoinOtherNode<K, V1, V2, VR> extends Processor<K, V2, K, VR> {
	constructor(
		private readonly leftStoreRef: { store: KeyValueStore<K, V1> | null },
		private readonly joiner: (leftValue: V1, rightValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V2, K, VR> {
		void worker
		return new TableTableLeftJoinOtherNode<K, V1, V2, VR>(this.leftStoreRef, this.joiner)
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

		const leftValue = await store.get(key)
		if (leftValue === undefined) {
			// No left row: a left join produces no result for this key (not even a tombstone).
			return
		}

		// Left row exists: emit joiner(left, right). record.value may be null (right tombstone),
		// in which case the result is joiner(left, null) — the left row is retained, not deleted.
		const joinedValue = this.joiner(leftValue, record.value)
		await this.forward({ ...record, value: joinedValue })
	}
}

/**
 * Processor node for Table-Table outer join from the "left" side.
 * Used when the left table updates and needs to look up the right table.
 *
 * The result exists when EITHER side exists, so a left-side tombstone only deletes the
 * result when the right side is also absent; otherwise it re-emits joiner(null, right).
 */
export class TableTableOuterJoinLeftNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly rightStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (leftValue: V1 | null, rightValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new TableTableOuterJoinLeftNode<K, V1, V2, VR>(this.rightStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.rightStoreRef.store
		if (!store) {
			throw new Error('Right table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const rightValue = await store.get(key)
		if (record.value === null && rightValue === undefined) {
			// Both sides absent: the result no longer exists.
			await this.forward({ ...record, value: null })
			return
		}

		const joinedValue = this.joiner(record.value, rightValue ?? null)
		await this.forward({ ...record, value: joinedValue })
	}
}

/**
 * Processor node for Table-Table outer join from the "right" side.
 * Used when the right table updates and needs to look up the left table.
 *
 * The result exists when EITHER side exists, so a right-side tombstone only deletes the
 * result when the left side is also absent; otherwise it re-emits joiner(left, null).
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

		const leftValue = await store.get(key)
		if (record.value === null && leftValue === undefined) {
			// Both sides absent: the result no longer exists.
			await this.forward({ ...record, value: null })
			return
		}

		const joinedValue = this.joiner(leftValue ?? null, record.value)
		await this.forward({ ...record, value: joinedValue })
	}
}
