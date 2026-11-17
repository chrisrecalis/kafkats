/**
 * The table of a topic or a stream (KS KTableSource): `table()`, `toTable()` and global tables. Null keys are
 * dropped; a null value deletes the key. Every update is written to the table's timestamped store and forwarded as
 * a Change, with the old value once downstream asked for old values.
 */
import { MaterializedTableNode, storeValueGetter, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { Change } from '@/operators/change.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

export class KTableSource<K, V> extends MaterializedTableNode<K, V> {
	readonly supplier: ProcessorSupplier<K, V | null, K, Change<V>>
	readonly valueGetter: KTableValueGetterSupplier<K, V>

	constructor(readonly storeName: string) {
		super()
		this.supplier = () => new KTableSourceProcessor<K, V>(storeName, this.sendOldValues)
		this.valueGetter = storeValueGetter(storeName)
	}
}

class KTableSourceProcessor<K, V> implements Processor<K, V | null, K, Change<V>> {
	private context!: InternalProcessorContext<K, Change<V>>
	private store!: TimestampedKeyValueStore<K, V>

	constructor(
		private readonly storeName: string,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<K, Change<V>>): void {
		this.context = context as InternalProcessorContext<K, Change<V>>
		this.store = context.getStateStore(this.storeName)
	}

	async process(record: StreamRecord<K, V | null>): Promise<void> {
		const { key, value, timestamp } = record
		if (key === null || key === undefined) {
			this.context.recordDropped('null-key', { store: this.storeName })
			return
		}
		const old = this.sendOldValues() ? await this.store.get(key) : undefined
		if (value === null) {
			await this.store.delete(key)
		} else {
			await this.store.put(key, { value, timestamp })
		}
		await this.context.forward(record.withValue({ newValue: value, oldValue: old?.value ?? null }))
	}
}
