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

		// The other side doesn't change on this side's update, so the same lookup decides both the new
		// and prior results. An inner-join result exists only when both sides are present.
		const otherValue = await store.get(key)
		const newResult =
			record.value !== null && otherValue !== undefined ? this.joiner(record.value, otherValue) : null

		if (newResult !== null) {
			await this.forward({ ...record, value: newResult, oldValue: undefined })
			return
		}

		// No current result. Forward a tombstone only if one previously existed — this side had a value
		// AND the other side is present. oldValue is undefined for records not produced by a table state
		// node (e.g. a derived table); fall back to retracting whenever the other side is present.
		const oldValue = record.oldValue as V1 | null | undefined
		const hadResult =
			oldValue === undefined ? otherValue !== undefined : oldValue !== null && otherValue !== undefined
		if (hadResult) {
			await this.forward({ ...record, value: null, oldValue: undefined })
		}
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

		// A left join emits a result for any existing left row, regardless of the other side.
		const otherValue = await store.get(key)
		const newResult = record.value !== null ? this.joiner(record.value, otherValue ?? null) : null

		if (newResult !== null) {
			await this.forward({ ...record, value: newResult, oldValue: undefined })
			return
		}

		// Left tombstone: retract only if the left row previously existed. oldValue is undefined for
		// records not produced by a table state node; fall back to the legacy always-retract behavior.
		const oldValue = record.oldValue as V1 | null | undefined
		if (oldValue === undefined || oldValue !== null) {
			await this.forward({ ...record, value: null, oldValue: undefined })
		}
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
		// Clear oldValue so the right input's prior value can't masquerade as the result's prior value.
		const joinedValue = this.joiner(leftValue, record.value)
		await this.forward({ ...record, value: joinedValue, oldValue: undefined })
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

		// Right doesn't change on this side's update, so the same lookup decides new and prior results.
		const rightValue = await store.get(key)
		if (record.value !== null || rightValue !== undefined) {
			// At least one side present → the outer result exists; emit (left, right|null).
			await this.forward({ ...record, value: this.joiner(record.value, rightValue ?? null), oldValue: undefined })
			return
		}

		// Both sides absent now. Retract only if a result previously existed: the right side is absent
		// (unchanged), so that requires the left side to have had a value. oldValue is undefined for
		// records not produced by a table state node; fall back to always retracting.
		const oldValue = record.oldValue as V1 | null | undefined
		if (oldValue === undefined || oldValue !== null) {
			await this.forward({ ...record, value: null, oldValue: undefined })
		}
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

		// Left doesn't change on this side's update, so the same lookup decides new and prior results.
		const leftValue = await store.get(key)
		if (record.value !== null || leftValue !== undefined) {
			// At least one side present → the outer result exists; emit (left|null, right).
			await this.forward({ ...record, value: this.joiner(leftValue ?? null, record.value), oldValue: undefined })
			return
		}

		// Both sides absent now. Retract only if a result previously existed: the left side is absent
		// (unchanged), so that requires the right side to have had a value. oldValue is undefined for
		// records not produced by a table state node; fall back to always retracting.
		const oldValue = record.oldValue as V2 | null | undefined
		if (oldValue === undefined || oldValue !== null) {
			await this.forward({ ...record, value: null, oldValue: undefined })
		}
	}
}
