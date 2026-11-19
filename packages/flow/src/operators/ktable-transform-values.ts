/**
 * KTable.transformValues (KS `KTableTransformValues`, 6.12, KF9): one ValueTransformerWithKey per processor, with a
 * context that cannot forward; it sees every update's key and new value, tombstones included (null), and its result is
 * the new value (null deletes the key). Old values come from the table's own store when it is materialized, otherwise
 * from transforming the parent's old value, once downstream asked for them. Results keep the input's timestamp and
 * headers. A table that is not materialized is read by its value getter, which runs a transformer of its own over the
 * parent's value.
 */
import type { Change } from '@/operators/change.js'
import {
	storeValueGetter,
	type KTableNode,
	type KTableValueGetter,
	type KTableValueGetterSupplier,
} from '@/operators/ktable-node.js'
import type {
	MaybePromise,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
	RecordMetadata,
	ValueTransformerWithKey,
	ValueTransformerWithKeySupplier,
} from '@/processor/api.js'
import { ForwardingDisabledProcessorContext } from '@/processor/context-views.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampedKeyValueStore } from '@/state.js'

export interface TransformValuesNodeOptions {
	/** The table's store: the table is materialized. */
	readonly storeName?: string
	/** The stores the transformers use: connected to the processor and to every processor reading the value getter. */
	readonly stores: readonly string[]
}

export class KTableTransformValues<K, V, VR> implements KTableNode<K, VR> {
	readonly supplier: ProcessorSupplier<K, Change<V>, K, Change<VR>>
	readonly valueGetter: KTableValueGetterSupplier<K, VR>
	private sendOldValues = false

	constructor(
		private readonly parent: KTableNode<K, V>,
		transformers: ValueTransformerWithKeySupplier<K, V, VR>,
		private readonly options: TransformValuesNodeOptions
	) {
		const { storeName } = options
		this.supplier = () => new KTableTransformValuesProcessor(transformers(), storeName, () => this.sendOldValues)
		this.valueGetter =
			storeName === undefined
				? transformedValueGetter(parent.valueGetter, transformers, options.stores)
				: storeValueGetter(storeName)
	}

	/** A materialized table has its own old values; otherwise they are transformed from the parent's, if it sends them. */
	enableSendingOldValues(force: boolean): boolean {
		if (this.options.storeName !== undefined || this.parent.enableSendingOldValues(force)) {
			this.sendOldValues = true
		}
		return this.sendOldValues
	}
}

class KTableTransformValuesProcessor<K, V, VR> implements Processor<K, Change<V>, K, Change<VR>> {
	private context!: ProcessorContext<K, Change<VR>>
	private store: TimestampedKeyValueStore<K, VR> | undefined

	constructor(
		private readonly transformer: ValueTransformerWithKey<K, V, VR>,
		private readonly storeName: string | undefined,
		private readonly sendOldValues: () => boolean
	) {}

	async init(context: ProcessorContext<K, Change<VR>>): Promise<void> {
		this.context = context
		this.store = this.storeName === undefined ? undefined : context.getStateStore(this.storeName)
		await this.transformer.init?.(new ForwardingDisabledProcessorContext(context))
	}

	async process(record: StreamRecord<K, Change<V>>): Promise<void> {
		const { key, timestamp } = record
		// A transformation to undefined is no value, as null is.
		const newValue = (await this.transformer.transform(key, record.value.newValue)) ?? null
		let oldValue: VR | null = null
		if (this.store !== undefined) {
			if (this.sendOldValues()) {
				oldValue = (await this.store.get(key))?.value ?? null
			}
			await (newValue === null ? this.store.delete(key) : this.store.put(key, { value: newValue, timestamp }))
		} else if (this.sendOldValues()) {
			oldValue = (await this.transformer.transform(key, record.value.oldValue)) ?? null
		}
		await this.context.forward(record.withValue({ newValue, oldValue }))
	}

	close(): MaybePromise<void> {
		return this.transformer.close?.()
	}
}

/**
 * The getter of a table that is not materialized: the parent's value of the key (null if it has none) transformed by a
 * transformer of the getter's own, with the parent value's timestamp (-1 without one, KS UNKNOWN).
 */
function transformedValueGetter<K, V, VR>(
	parent: KTableValueGetterSupplier<K, V>,
	transformers: ValueTransformerWithKeySupplier<K, V, VR>,
	stores: readonly string[]
): KTableValueGetterSupplier<K, VR> {
	return {
		storeNames: [...parent.storeNames, ...stores],
		create: (): KTableValueGetter<K, VR> => {
			const values = parent.create()
			const transformer = transformers()
			return {
				init: async context => {
					await values.init(context)
					await transformer.init?.(new GetterContext(context))
				},
				get: async key => {
					const parentValue = await values.get(key)
					const value = (await transformer.transform(key, parentValue?.value ?? null)) ?? null
					return value === null ? null : { value, timestamp: parentValue?.timestamp ?? -1 }
				},
				close: async () => {
					await values.close?.()
					await transformer.close?.()
				},
			}
		},
	}
}

/** A getter reads outside the record its processor processes: there is no record metadata (KS: unknown). */
class GetterContext extends ForwardingDisabledProcessorContext {
	override recordMetadata(): RecordMetadata | null {
		return null
	}
}
