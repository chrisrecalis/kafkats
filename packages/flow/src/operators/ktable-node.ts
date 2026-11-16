import type { Change } from '@/operators/change.js'
import type { InternalProcessorContext, MaybePromise, ProcessorSupplier } from '@/processor/api.js'
import type { ValueAndTimestamp } from '@/state.js'

/** Reads the current value of a key of a table, from its store or recomputed from its parent's getter. */
export interface KTableValueGetter<K, V> {
	init(context: InternalProcessorContext<unknown, unknown>): MaybePromise<void>
	get(key: K): Promise<ValueAndTimestamp<V> | null>
	close?(): MaybePromise<void>
}

/** What the DSL keeps of a table node: a plain descriptor, not a callable object. */
export interface KTableNode<K, V> {
	readonly supplier: ProcessorSupplier<K, Change<unknown>, K, Change<V>>
	/** Kafka Streams' enableSendingOldValues: false if old values need a store this node does not have and !force. */
	enableSendingOldValues(force: boolean): boolean
	readonly valueGetter: { readonly storeNames: readonly string[]; create(): KTableValueGetter<K, V> }
}
