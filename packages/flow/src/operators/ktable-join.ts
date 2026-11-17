/**
 * Table-table joins (KS KTableKTableInnerJoin, LeftJoin, RightJoin, OuterJoin and KTableKTableJoinMerger; 6.13,
 * ksdsl§10): an update of one side reads the other side's value of its key through that side's value getter, and a
 * merger node forwards the results of both sides (storing them first when the result is materialized).
 *
 * An update `Change(new, old)` that finds the other side's value `vB` (absent: null) forwards nothing if `vB` is absent
 * and the side has no result without it, or has one but neither `new` nor `old` exists. Otherwise it forwards
 * `Change(new', old')`: `new'` / `old'` join `new` / `old` with `vB` where the join has a result for them, and are null
 * elsewhere (`old'` only when old values are sent), at `max(ts, tB)` (`ts` without `vB`) with the update's headers.
 * So a delete is forwarded whenever `vB` exists, with or without old values: as `vB` joined with null where the join
 * has a result without the deleted value (a right delete in a left join, any delete in an outer join), else as a
 * tombstone. A joiner returning null deletes the result. Null keys are dropped.
 */
import type { Change } from '@/operators/change.js'
import {
	storeValueGetter,
	type KTableNode,
	type KTableValueGetter,
	type KTableValueGetterSupplier,
} from '@/operators/ktable-node.js'
import type {
	ForwardResult,
	InternalProcessorContext,
	MaybePromise,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
} from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

export type TableJoinKind = 'inner' | 'left' | 'outer'

/** The joiner of the left and right values; null for a side without a value (left and outer joins). */
export type TableJoiner<V, VO, VR> = (value: V | null, otherValue: VO | null) => VR

/** Whether a side's join has a result while only it, or only the other side, has a value. */
interface Outer {
	readonly self: boolean
	readonly other: boolean
}

/** The left side's view; the right side's is mirrored (the right side of a left join is KS's KTableKTableRightJoin). */
const LEFT_OUTER: Readonly<Record<TableJoinKind, Outer>> = {
	inner: { self: false, other: false },
	left: { self: true, other: false },
	outer: { self: true, other: true },
}

/** Whether the join has a result for a key given the values of this side and the other side (null: none). */
function joined(outer: Outer, value: unknown, otherValue: unknown): boolean {
	return value !== null ? otherValue !== null || outer.self : otherValue !== null && outer.other
}

export class KTableJoin<K, V, VO, VR> implements KTableNode<K, VR> {
	/** Processes the left table's updates. */
	readonly thisSide: ProcessorSupplier<K, Change<V>, K, Change<VR>>
	/** Processes the right table's updates. */
	readonly otherSide: ProcessorSupplier<K, Change<VO>, K, Change<VR>>
	/** The merger: the result table's node. */
	readonly supplier: ProcessorSupplier<K, Change<VR>, K, Change<VR>>
	readonly valueGetter: KTableValueGetterSupplier<K, VR>
	private sendOldValues = false

	constructor(
		private readonly left: KTableNode<K, V>,
		private readonly right: KTableNode<K, VO>,
		kind: TableJoinKind,
		joiner: TableJoiner<V, VO, VR>,
		/** The result's store: the result is materialized. */
		storeName?: string
	) {
		const outer = LEFT_OUTER[kind]
		const mirrored = { self: outer.other, other: outer.self }
		const sendOldValues = () => this.sendOldValues
		this.thisSide = () => new KTableJoinProcessor(right.valueGetter, outer, joiner, sendOldValues)
		this.otherSide = () =>
			new KTableJoinProcessor(
				left.valueGetter,
				mirrored,
				(value: VO | null, otherValue: V | null) => joiner(otherValue, value),
				sendOldValues
			)
		this.supplier = () => new KTableJoinMerger<K, VR>(storeName)
		this.valueGetter =
			storeName === undefined
				? joinValueGetter(left.valueGetter, right.valueGetter, outer, joiner)
				: storeValueGetter(storeName)
		// A side with results of its own tells by its old value whether a delete retracts one (KS forces it at the join):
		// the left side of left joins, both sides of outer joins.
		if (outer.self) {
			left.enableSendingOldValues(true)
		}
		if (outer.other) {
			right.enableSendingOldValues(true)
		}
	}

	/** Old results are joined from the old values of both sides (KS forces both inputs). */
	enableSendingOldValues(): boolean {
		this.left.enableSendingOldValues(true)
		this.right.enableSendingOldValues(true)
		this.sendOldValues = true
		return true
	}
}

/**
 * The getter of a result that is not materialized: the join of both sides' current values (ksdsl§10.3), at the max
 * timestamp of the values it joins. The right side is not read when the left side alone rules out a result.
 */
function joinValueGetter<K, V, VO, VR>(
	left: KTableValueGetterSupplier<K, V>,
	right: KTableValueGetterSupplier<K, VO>,
	outer: Outer,
	joiner: TableJoiner<V, VO, VR>
): KTableValueGetterSupplier<K, VR> {
	return {
		storeNames: [...new Set([...left.storeNames, ...right.storeNames])],
		create: () => {
			const leftValues = left.create()
			const rightValues = right.create()
			return {
				init: async context => {
					await leftValues.init(context)
					await rightValues.init(context)
				},
				get: async key => {
					const leftValue = await leftValues.get(key)
					if (leftValue === null && !outer.other) {
						return null
					}
					const rightValue = await rightValues.get(key)
					const value = leftValue?.value ?? null
					const otherValue = rightValue?.value ?? null
					const result = joined(outer, value, otherValue) ? joiner(value, otherValue) : null
					const timestamp = Math.max(leftValue?.timestamp ?? -1, rightValue?.timestamp ?? -1)
					return result === null ? null : { value: result, timestamp }
				},
				close: async () => {
					await leftValues.close?.()
					await rightValues.close?.()
				},
			}
		},
	}
}

class KTableJoinProcessor<K, V, VO, VR> implements Processor<K, Change<V>, K, Change<VR>> {
	private context!: InternalProcessorContext<K, Change<VR>>
	private readonly otherValues: KTableValueGetter<K, VO>

	constructor(
		other: KTableValueGetterSupplier<K, VO>,
		private readonly outer: Outer,
		private readonly joiner: TableJoiner<V, VO, VR>,
		private readonly sendOldValues: () => boolean
	) {
		this.otherValues = other.create()
	}

	init(context: ProcessorContext<K, Change<VR>>): MaybePromise<void> {
		this.context = context as InternalProcessorContext<K, Change<VR>>
		return this.otherValues.init(this.context as InternalProcessorContext<unknown, unknown>)
	}

	async process(record: StreamRecord<K, Change<V>>): Promise<void> {
		const { key, timestamp } = record
		if (key === null || key === undefined) {
			this.context.recordDropped('null-key')
			return
		}
		const other = await this.otherValues.get(key)
		const { newValue, oldValue } = record.value
		if (other === null && !(this.outer.self && (newValue !== null || oldValue !== null))) {
			return
		}
		const otherValue = other?.value ?? null
		const change: Change<VR> = {
			newValue: this.join(newValue, otherValue),
			oldValue: this.sendOldValues() ? this.join(oldValue, otherValue) : null,
		}
		const resultTimestamp = other === null ? timestamp : Math.max(timestamp, other.timestamp)
		await this.context.forward(new StreamRecord(key, change, resultTimestamp, record.headers))
	}

	close(): MaybePromise<void> {
		return this.otherValues.close?.()
	}

	private join(value: V | null, otherValue: VO | null): VR | null {
		return joined(this.outer, value, otherValue) ? this.joiner(value, otherValue) : null
	}
}

/** Forwards both sides' results; a materialized result is stored first (a null result deletes the key). */
class KTableJoinMerger<K, VR> implements Processor<K, Change<VR>, K, Change<VR>> {
	private context!: ProcessorContext<K, Change<VR>>
	private store: TimestampedKeyValueStore<K, VR> | undefined

	constructor(private readonly storeName: string | undefined) {}

	init(context: ProcessorContext<K, Change<VR>>): void {
		this.context = context
		this.store = this.storeName === undefined ? undefined : context.getStateStore(this.storeName)
	}

	process(record: StreamRecord<K, Change<VR>>): ForwardResult {
		return this.store === undefined ? this.context.forward(record) : this.materialize(this.store, record)
	}

	private async materialize(
		store: TimestampedKeyValueStore<K, VR>,
		record: StreamRecord<K, Change<VR>>
	): Promise<void> {
		const { key, timestamp, value } = record
		await (value.newValue === null ? store.delete(key) : store.put(key, { value: value.newValue, timestamp }))
		await this.context.forward(record)
	}
}
