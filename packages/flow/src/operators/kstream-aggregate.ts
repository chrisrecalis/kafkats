/**
 * KStream count, reduce and aggregate (KS KStreamAggregate, KStreamReduce; 6.3), ported with today's semantics: a
 * result carries the timestamp of the record that produced it (Stage 3 changes it to the max timestamp, WP3.10).
 *
 * Read-modify-write per record: under at-least-once a replayed record is aggregated again (KS behaves the same);
 * exactly-once commits the store writes with the offsets.
 */
import type { Change } from '@/operators/change.js'
import { storeValueGetter, type KTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
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

/** reduce(): the first value as it is, then the reducer over the current aggregate. */
export function reduceWith<V>(reducer: (aggregate: V, value: V) => V): Aggregation<unknown, V, V> {
	return (_key, value, current) => (current === undefined ? value : reducer(current, value))
}

/** Whether a record takes part in an aggregation: null keys and values are dropped (ksdsl§1.4). */
export function aggregatable<K, V>(
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

export class KStreamAggregate<K, V, A> implements KTableNode<K, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, K, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<K, A>
	private sendOldValues = false

	constructor(storeName: string, aggregation: Aggregation<K, V, A>) {
		this.supplier = () => new KStreamAggregateProcessor(storeName, aggregation, () => this.sendOldValues)
		this.valueGetter = storeValueGetter(storeName)
	}

	/** Always materialized: the store has the old values. */
	enableSendingOldValues(): boolean {
		this.sendOldValues = true
		return true
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
		const { key, value, timestamp } = record
		const old = (await this.store.get(key))?.value
		const aggregate = this.aggregation(key, value, old)
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(key)
		} else {
			await this.store.put(key, { value: aggregate, timestamp })
		}
		const oldValue = this.sendOldValues() ? (old ?? null) : null
		await this.context.forward(record.withValue({ newValue: aggregate ?? null, oldValue }))
	}
}
