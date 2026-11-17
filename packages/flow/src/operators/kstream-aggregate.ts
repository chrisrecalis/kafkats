/**
 * KStream count, reduce and aggregate (KS KStreamAggregate, KStreamReduce; 6.3). A result carries the max timestamp
 * of the records aggregated into it (`max(record, previous aggregate)`, KS `KStreamAggregate.java:127-133`), which is
 * also the timestamp stored with the aggregate; headers are the triggering record's.
 *
 * Read-modify-write per record: under at-least-once a replayed record is aggregated again (KS behaves the same);
 * exactly-once commits the store writes with the offsets.
 */
import type { Change } from '@/operators/change.js'
import { MaterializedTableNode, storeValueGetter, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

/**
 * Folds a record's value into the current aggregate (undefined when there is none yet). A null result deletes the
 * aggregate and is forwarded as a tombstone (KS: putting null deletes).
 */
export type Aggregation<K, V, A> = (key: K, value: V, current: A | undefined) => A | null

/** aggregate() and count(): the aggregator over the current aggregate, or over the initializer's for a new key. */
export function aggregateWith<K, V, A>(
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A
): Aggregation<K, V, A> {
	return (key, value, current) => aggregator(key, value, current === undefined ? initializer() : current)
}

/**
 * reduce(): the first value as it is, then the reducer over the current aggregate. Values may be null where the
 * aggregation passes null values on (time and session windows): a first null value keeps no aggregate.
 */
export function reduceWith<A, V extends A | null>(reducer: (aggregate: A, value: V) => A): Aggregation<unknown, V, A> {
	return (_key, value, current) => (current === undefined ? value : reducer(current, value))
}

/** Whether a record takes part in an aggregation: null keys and values are dropped (ksdsl§1.4). */
function aggregatable<K, V>(
	context: Pick<InternalProcessorContext, 'recordDropped'>,
	store: string,
	record: StreamRecord<K | null, V | null>
): record is StreamRecord<K, V> {
	if (record.key === null || record.key === undefined) {
		context.recordDropped('null-key', { store })
		return false
	}
	if (record.value === null || record.value === undefined) {
		context.recordDropped('null-value', { store })
		return false
	}
	return true
}

export class KStreamAggregate<K, V, A> extends MaterializedTableNode<K, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, K, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<K, A>

	constructor(storeName: string, aggregation: Aggregation<K, V, A>) {
		super()
		this.supplier = () => new KStreamAggregateProcessor(storeName, aggregation, this.sendOldValues)
		this.valueGetter = storeValueGetter(storeName)
	}
}

class KStreamAggregateProcessor<K, V, A> implements Processor<K | null, V | null, K, Change<A>> {
	private context!: InternalProcessorContext<K, Change<A>>
	private store!: TimestampedKeyValueStore<K, A>

	constructor(
		private readonly storeName: string,
		private readonly aggregation: Aggregation<K, V, A>,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<K, Change<A>>): void {
		this.context = context as InternalProcessorContext<K, Change<A>>
		this.store = context.getStateStore(this.storeName)
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		if (!aggregatable(this.context, this.storeName, record)) {
			return
		}
		const { key, value } = record
		const old = await this.store.get(key)
		const aggregate = this.aggregation(key, value, old?.value)
		const timestamp = old === undefined ? record.timestamp : Math.max(record.timestamp, old.timestamp)
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(key)
		} else {
			await this.store.put(key, { value: aggregate, timestamp })
		}
		const oldValue = this.sendOldValues() ? (old?.value ?? null) : null
		await this.context.forward(record.withValue({ newValue: aggregate ?? null, oldValue }).withTimestamp(timestamp))
	}
}
