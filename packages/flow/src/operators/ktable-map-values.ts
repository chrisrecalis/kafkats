/**
 * KTable.mapValues (KS KTableMapValues, 6.12) and with it every table derived value by value from its parent: filter
 * and filterNot are mappings that keep or drop a value (ktable-filter.ts). The mapping never sees null: a tombstone
 * stays a tombstone, and a null result deletes the key. Old values are mapped like new ones once downstream asked for
 * them, or read from the table's own store when it is materialized. Results keep the input's timestamp and headers.
 */
import type { Change } from '@/operators/change.js'
import { storeValueGetter, type KTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { ForwardResult, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

/** The value of a key in the derived table, given its value in the parent; null: the key has none. */
export type ValueMapping<K, V, VR> = (key: K, value: V) => VR | null

export interface MapValuesOptions {
	/** The table's store: the table is materialized. */
	readonly storeName?: string
	/** Filters: an update without a value before and after is not forwarded, when old values tell (KS KTableFilter). */
	readonly dropAbsent?: boolean
}

export class KTableMapValues<K, V, VR> implements KTableNode<K, VR> {
	readonly supplier: ProcessorSupplier<K, Change<V>, K, Change<VR>>
	readonly valueGetter: KTableValueGetterSupplier<K, VR>
	private sendOldValues = false

	constructor(
		private readonly parent: KTableNode<K, V>,
		mapping: ValueMapping<K, V, VR>,
		private readonly options: MapValuesOptions = {}
	) {
		// A mapping to undefined is no value, as null is.
		const map = (key: K, value: V | null) => (value === null ? null : (mapping(key, value) ?? null))
		const { storeName } = options
		this.supplier = () => new KTableMapValuesProcessor(map, options, () => this.sendOldValues)
		this.valueGetter =
			storeName === undefined ? mappedValueGetter(parent.valueGetter, map) : storeValueGetter(storeName)
	}

	/** A materialized table has its own old values; otherwise they are mapped from the parent's, if it sends them. */
	enableSendingOldValues(force: boolean): boolean {
		if (this.options.storeName !== undefined || this.parent.enableSendingOldValues(force)) {
			this.sendOldValues = true
		}
		return this.sendOldValues
	}
}

/** The getter of a table that is not materialized: the parent's value of the key, mapped, with its timestamp. */
function mappedValueGetter<K, V, VR>(
	parent: KTableValueGetterSupplier<K, V>,
	map: (key: K, value: V | null) => VR | null
): KTableValueGetterSupplier<K, VR> {
	return {
		storeNames: parent.storeNames,
		create: () => {
			const values = parent.create()
			return {
				init: context => values.init(context),
				get: async key => {
					const parentValue = await values.get(key)
					const value = map(key, parentValue?.value ?? null)
					return parentValue === null || value === null ? null : { value, timestamp: parentValue.timestamp }
				},
				close: () => values.close?.(),
			}
		},
	}
}

class KTableMapValuesProcessor<K, V, VR> implements Processor<K, Change<V>, K, Change<VR>> {
	private context!: ProcessorContext<K, Change<VR>>
	private store: TimestampedKeyValueStore<K, VR> | undefined

	constructor(
		private readonly map: (key: K, value: V | null) => VR | null,
		private readonly options: MapValuesOptions,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<K, Change<VR>>): void {
		this.context = context
		const { storeName } = this.options
		this.store = storeName === undefined ? undefined : context.getStateStore(storeName)
	}

	/** Synchronous unless the table is materialized. */
	process(record: StreamRecord<K, Change<V>>): ForwardResult {
		const newValue = this.map(record.key, record.value.newValue)
		if (this.store !== undefined) {
			return this.materialize(this.store, record, newValue)
		}
		const oldValue = this.sendOldValues() ? this.map(record.key, record.value.oldValue) : null
		return this.drops(newValue, oldValue)
			? undefined
			: this.context.forward(record.withValue({ newValue, oldValue }))
	}

	private async materialize(
		store: TimestampedKeyValueStore<K, VR>,
		record: StreamRecord<K, Change<V>>,
		newValue: VR | null
	): Promise<void> {
		const { key, timestamp } = record
		const oldValue = this.sendOldValues() ? ((await store.get(key))?.value ?? null) : null
		if (this.drops(newValue, oldValue)) {
			return
		}
		await (newValue === null ? store.delete(key) : store.put(key, { value: newValue, timestamp }))
		await this.context.forward(record.withValue({ newValue, oldValue }))
	}

	private drops(newValue: VR | null, oldValue: VR | null): boolean {
		return this.options.dropAbsent === true && this.sendOldValues() && newValue === null && oldValue === null
	}
}
