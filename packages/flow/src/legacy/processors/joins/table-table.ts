import { Processor, requireStore, type StoreRef, type StreamRecord } from '@/legacy/processors/base.js'
import type { KeyValueStore } from '@/state.js'

/**
 * Processor node for Table-Table inner join.
 * When a record arrives from one table, looks up the other table and emits a join result.
 */
export class TableTableJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly otherStoreRef: StoreRef<KeyValueStore<K, V2>>,
		private readonly joiner: (value: V1, otherValue: V2) => VR
	) {
		super()
	}

	clone(): Processor<K, V1, K, VR> {
		return new TableTableJoinNode(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = requireStore(this.otherStoreRef, 'table-table join lookup')
		const key = record.key
		if (key === null) return

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
		private readonly otherStoreRef: StoreRef<KeyValueStore<K, V2>>,
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(): Processor<K, V1, K, VR> {
		return new TableTableLeftJoinNode(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = requireStore(this.otherStoreRef, 'table-table join lookup')
		const key = record.key
		if (key === null) return

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
		private readonly leftStoreRef: StoreRef<KeyValueStore<K, V1>>,
		private readonly joiner: (leftValue: V1, rightValue: V2 | null) => VR
	) {
		super()
	}

	clone(): Processor<K, V2, K, VR> {
		return new TableTableLeftJoinOtherNode(this.leftStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V2>): Promise<void> {
		const store = requireStore(this.leftStoreRef, 'table-table join lookup')
		const key = record.key
		if (key === null) return

		const leftValue = await store.get(key)
		if (leftValue === undefined) {
			// No left row: a left join produces no result for this key (not even a tombstone).
			return
		}

		// Left row exists: emit joiner(left, right). record.value may be null (right tombstone),
		// in which case the result is joiner(left, null) — the left row is retained, not deleted.
		// Clear oldValue so the right input's prior value can't masquerade as the result's prior value.
		await this.forward({ ...record, value: this.joiner(leftValue, record.value), oldValue: undefined })
	}
}

/**
 * One side of a Table-Table outer join; the other side uses the same node with the joiner's
 * arguments swapped.
 *
 * The result exists when EITHER side exists, so a tombstone on this side only deletes the
 * result when the other side is also absent; otherwise it re-emits joiner(null, other).
 */
export class TableTableOuterJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly otherStoreRef: StoreRef<KeyValueStore<K, V2>>,
		private readonly joiner: (value: V1 | null, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(): Processor<K, V1, K, VR> {
		return new TableTableOuterJoinNode(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = requireStore(this.otherStoreRef, 'table-table join lookup')
		const key = record.key
		if (key === null) return

		// The other side doesn't change on this side's update, so the same lookup decides new and prior results.
		const otherValue = await store.get(key)
		if (record.value !== null || otherValue !== undefined) {
			// At least one side present → the outer result exists.
			await this.forward({ ...record, value: this.joiner(record.value, otherValue ?? null), oldValue: undefined })
			return
		}

		// Both sides absent now. Retract only if a result previously existed: the other side is absent
		// (unchanged), so that requires this side to have had a value. oldValue is undefined for
		// records not produced by a table state node; fall back to always retracting.
		const oldValue = record.oldValue as V1 | null | undefined
		if (oldValue === undefined || oldValue !== null) {
			await this.forward({ ...record, value: null, oldValue: undefined })
		}
	}
}
