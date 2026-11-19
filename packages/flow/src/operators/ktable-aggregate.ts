/**
 * KGroupedTable count, reduce and aggregate (KS `KTableAggregate.java:97-143`, `KTableReduce.java:94-135`; 6.15) over
 * the changes the repartition map wrote for each group: the old value leaves the group's aggregate (when it has one),
 * then the new value joins it. The result is stored and forwarded with the max timestamp of the update and the
 * previous aggregate, and the update's headers. An aggregate is never deleted because its group emptied (a count
 * stays at 0, BC11); only a null result deletes it and forwards a tombstone. A group without an aggregate before and
 * after the update (an old value leaving a group that has none, B10) changes nothing and forwards nothing, as Kafka
 * Streams' default record cache does.
 */
import type { Change } from '@/operators/change.js'
import { MaterializedTableNode, storeValueGetter, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

/** How a value leaving and a value joining a group change its aggregate; a null aggregate: the group has none. */
export interface TableAggregator<K, V, A> {
	readonly subtract: (key: K, value: V, aggregate: A) => A | null
	readonly add: (key: K, value: V, aggregate: A | null) => A | null
}

/** aggregate(): the adder starts from the initializer's value in a group without an aggregate. */
export function tableAggregate<K, V, A>(
	initializer: () => A,
	adder: (key: K, value: V, aggregate: A) => A,
	subtractor: (key: K, value: V, aggregate: A) => A
): TableAggregator<K, V, A> {
	return { subtract: subtractor, add: (key, value, aggregate) => adder(key, value, aggregate ?? initializer()) }
}

/** reduce(): the first value of a group without an aggregate is its aggregate. */
export function tableReduce<V>(
	adder: (aggregate: V, value: V) => V,
	subtractor: (aggregate: V, value: V) => V
): TableAggregator<unknown, V, V> {
	return {
		subtract: (_key, value, aggregate) => subtractor(aggregate, value),
		add: (_key, value, aggregate) => (aggregate === null ? value : adder(aggregate, value)),
	}
}

/** count(): aggregate(0, +1, -1) (KS `KGroupedTableImpl.java:53-57`). */
export const tableCount: TableAggregator<unknown, unknown, number> = tableAggregate(
	() => 0,
	(_key, _value, count) => count + 1,
	(_key, _value, count) => count - 1
)

export class KTableAggregate<K, V, A> extends MaterializedTableNode<K, A> {
	readonly supplier: ProcessorSupplier<K, Change<V> | null, K, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<K, A>

	constructor(storeName: string, aggregator: TableAggregator<K, V, A>) {
		super()
		this.supplier = () => new KTableAggregateProcessor(storeName, aggregator, this.sendOldValues)
		this.valueGetter = storeValueGetter(storeName)
	}
}

class KTableAggregateProcessor<K, V, A> implements Processor<K, Change<V> | null, K, Change<A>> {
	private context!: InternalProcessorContext<K, Change<A>>
	private store!: TimestampedKeyValueStore<K, A>

	constructor(
		private readonly storeName: string,
		private readonly aggregator: TableAggregator<K, V, A>,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<K, Change<A>>): void {
		this.context = context as InternalProcessorContext<K, Change<A>>
		this.store = context.getStateStore(this.storeName)
	}

	async process(record: StreamRecord<K, Change<V> | null>): Promise<void> {
		const { key, value: change } = record
		if (key === null || key === undefined) {
			this.context.recordDropped('null-key', { store: this.storeName })
			return
		}
		if (change === null || change === undefined) {
			this.context.recordDropped('null-value', { store: this.storeName })
			return
		}
		const old = await this.store.get(key)
		const oldAggregate = old?.value ?? null
		const { subtract, add } = this.aggregator
		const remaining =
			change.oldValue === null || oldAggregate === null
				? oldAggregate
				: (subtract(key, change.oldValue, oldAggregate) ?? null)
		const aggregate = change.newValue === null ? remaining : (add(key, change.newValue, remaining) ?? null)
		if (old === undefined && aggregate === null) {
			return
		}
		const timestamp = old === undefined ? record.timestamp : Math.max(record.timestamp, old.timestamp)
		await (aggregate === null ? this.store.delete(key) : this.store.put(key, { value: aggregate, timestamp }))
		const oldValue = this.sendOldValues() ? oldAggregate : null
		await this.context.forward(record.withValue({ newValue: aggregate, oldValue }).withTimestamp(timestamp))
	}
}
