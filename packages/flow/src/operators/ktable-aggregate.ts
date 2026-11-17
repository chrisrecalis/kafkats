/**
 * KGroupedTable count, reduce and aggregate (KS KTableAggregate, KTableReduce; 6.15), ported with today's semantics
 * (WP3.8 switches to Change pairs and keeps emptied groups): each grouped record read back from the repartition topic
 * adds a row's value to its group or, marked as a retraction (ktable-repartition-map.ts), subtracts it. A null
 * aggregate deletes the group and forwards a tombstone; aggregate() also counts the rows of each group and deletes a
 * group whose last row left, whatever the subtractor returned. Results carry the record's timestamp and no headers.
 */
import type { Change } from '@/operators/change.js'
import { storeValueGetter, type KTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { isRetraction } from '@/operators/ktable-repartition-map.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import type { KeyValueStore, TimestampedKeyValueStore } from '@/state.js'

/** Returned by a subtraction that does not apply (no aggregate to subtract from): nothing changes or is forwarded. */
export const UNCHANGED = Symbol('unchanged')

/** How a group's aggregate changes when a row's value joins or leaves the group; null deletes the group. */
export interface GroupAggregator<K, V, A> {
	/** `aggregate` is undefined while the group has none. */
	add(key: K, value: V, aggregate: A | undefined): A | null
	subtract(key: K, value: V, aggregate: A | undefined): A | null | typeof UNCHANGED
}

/**
 * count(): a group whose count drops to 0 is deleted. A subtraction from a missing group deletes it again (a tombstone
 * for a group that does not exist, B10; kept until WP3.8).
 */
export const tableCount: GroupAggregator<unknown, unknown, number> = {
	add: (_key, _value, count = 0) => count + 1,
	subtract: (_key, _value, count = 0) => (count > 1 ? count - 1 : null),
}

/** reduce(): the first value as it is, then the adder; a subtraction from a missing group is ignored. */
export function tableReduce<V>(
	adder: (aggregate: V, value: V) => V,
	subtractor: (aggregate: V, value: V) => V
): GroupAggregator<unknown, V, V> {
	return {
		add: (_key, value, aggregate) => (aggregate === undefined ? value : adder(aggregate, value)),
		subtract: (_key, value, aggregate) => (aggregate === undefined ? UNCHANGED : subtractor(aggregate, value)),
	}
}

/** aggregate(): the adder over the initializer's value for a new group; a subtraction from a missing group is ignored. */
export function tableAggregate<K, V, A>(
	initializer: () => A,
	adder: (key: K, value: V, aggregate: A) => A,
	subtractor: (key: K, value: V, aggregate: A) => A
): GroupAggregator<K, V, A> {
	return {
		add: (key, value, aggregate) => adder(key, value, aggregate ?? initializer()),
		subtract: (key, value, aggregate) => (aggregate === undefined ? UNCHANGED : subtractor(key, value, aggregate)),
	}
}

export interface TableAggregateStores {
	/** The aggregates. */
	readonly storeName: string
	/** Row counts per group (aggregate()): a group without rows is deleted. */
	readonly membersStore?: string
}

export class KTableAggregate<K, V, A> implements KTableNode<K, A> {
	readonly supplier: ProcessorSupplier<K, V | null, K, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<K, A>
	private sendOldValues = false

	constructor(stores: TableAggregateStores, aggregator: GroupAggregator<K, V, A>) {
		this.supplier = () => new KTableAggregateProcessor(stores, aggregator, () => this.sendOldValues)
		this.valueGetter = storeValueGetter(stores.storeName)
	}

	/** Always materialized: the store has the old values. */
	enableSendingOldValues(): boolean {
		this.sendOldValues = true
		return true
	}
}

class KTableAggregateProcessor<K, V, A> implements Processor<K, V | null, K, Change<A>> {
	private context!: InternalProcessorContext<K, Change<A>>
	private store!: TimestampedKeyValueStore<K, A>
	private members: KeyValueStore<K, number> | undefined

	constructor(
		private readonly stores: TableAggregateStores,
		private readonly aggregator: GroupAggregator<K, V, A>,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<K, Change<A>>): void {
		const { storeName, membersStore } = this.stores
		this.context = context as InternalProcessorContext<K, Change<A>>
		this.store = context.getStateStore(storeName)
		this.members = membersStore === undefined ? undefined : context.getStateStore(membersStore)
	}

	async process(record: StreamRecord<K, V | null>): Promise<void> {
		const { key, value, timestamp } = record
		if (key === null || key === undefined || value === null || value === undefined) {
			const reason = key === null || key === undefined ? 'null-key' : 'null-value'
			this.context.recordDropped(reason, { store: this.stores.storeName })
			return
		}
		const retraction = isRetraction(record.headers)
		const old = (await this.store.get(key))?.value
		const next = retraction ? this.aggregator.subtract(key, value, old) : this.aggregator.add(key, value, old)
		if (next === UNCHANGED) {
			return
		}
		const emptied = this.members !== undefined && (await countRow(this.members, key, retraction ? -1 : 1))
		const aggregate = emptied ? null : (next ?? null)
		await (aggregate === null ? this.store.delete(key) : this.store.put(key, { value: aggregate, timestamp }))
		const change = { newValue: aggregate, oldValue: this.sendOldValues() ? (old ?? null) : null }
		await this.context.forward(new StreamRecord(key, change, timestamp, {}))
	}
}

/** Counts a row into (+1) or out of (-1) its group; true when the group has no rows left. */
async function countRow<K>(members: KeyValueStore<K, number>, key: K, delta: 1 | -1): Promise<boolean> {
	const rows = ((await members.get(key)) ?? 0) + delta
	await (rows > 0 ? members.put(key, rows) : members.delete(key))
	return rows <= 0
}
