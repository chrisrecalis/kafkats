/**
 * The Processor API in the DSL (6.12, 6.21; KS `KStreamImpl.process` / `processValues`, `KTableImpl.transformValues`,
 * `StreamsBuilder.addStateStore`): user processors and transformers as nodes, connected to the stores they name. A
 * store definition is registered with the app the first time it is named, by `app.addStateStore()` or in a processor's
 * `stores`, and shared by every processor connected to it, which puts those processors in one subtopology (4.1.2).
 */
import type { KStream } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { ConnectedStores, ProcessOptions, TransformValuesOptions } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { DslContext } from '@/dsl/context.js'
import { withNewValues } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import { derivedFormat, materializedStore } from '@/dsl/ops/table-ops.js'
import { fixedKeyProcessorSupplier } from '@/operators/fixed-key.js'
import { KTableTransformValues } from '@/operators/ktable-transform-values.js'
import type { FixedKeyProcessorSupplier, ProcessorSupplier, ValueTransformerWithKeySupplier } from '@/processor/api.js'
import type { StateStore } from '@/state.js'
import { definedStoreBuilder } from '@/state/store-builders.js'
import { checkSupplier } from '@/topology/builder.js'
import type { NodeName, StoreBuilder } from '@/topology/nodes.js'

/** Keys and values may change: neither codec is known downstream, and a key-based operation repartitions first (KS). */
export function process<K, V, KR, VR>(
	stream: KStreamImpl<K, V>,
	supplier: ProcessorSupplier<K | null, V | null, KR, VR>,
	options: ProcessOptions = {}
): KStream<KR, VR> {
	const { ctx } = stream
	const node = ctx.processor(NodePrefix.PROCESSOR, options.name, [stream.node], supplier)
	connectStores(ctx, node, options.stores)
	return new KStreamImpl<KR, VR>(ctx, node, {}, true)
}

/** The key stays: its codec is kept, and so is the stream's repartition flag (KS). */
export function processValues<K, V, VR>(
	stream: KStreamImpl<K, V>,
	supplier: FixedKeyProcessorSupplier<K | null, V | null, VR>,
	options: ProcessOptions = {}
): KStream<K, VR> {
	const { ctx } = stream
	const name = ctx.builder.nodeName(NodePrefix.PROCESS_VALUES, options.name)
	checkSupplier(name, supplier)
	const node = ctx.processor(NodePrefix.PROCESS_VALUES, name, [stream.node], fixedKeyProcessorSupplier(supplier))
	connectStores(ctx, node, options.stores)
	return new KStreamImpl<K, VR>(ctx, node, withNewValues(stream.format), stream.repartitionRequired)
}

/** A derived table (6.12): materialized only with a Materialized store name, which needs the result's value codec. */
export function transformValues<K, V, VR>(
	table: KTableImpl<K, V>,
	supplier: ValueTransformerWithKeySupplier<K, V, VR>,
	options: TransformValuesOptions<K, VR> = {}
): KTable<K, VR> {
	const { ctx } = table
	const name = ctx.builder.nodeName(NodePrefix.TABLE_TRANSFORM_VALUES, options.name)
	checkSupplier(name, supplier)
	const format = derivedFormat(table.format, options.materialized, false)
	const storeName = materializedStore(ctx, options.materialized, format, 'transformValues()')
	const stores = (options.stores ?? []).map(store => (typeof store === 'string' ? store : store.name))
	const node = new KTableTransformValues(table.table, supplier, { storeName, stores })
	const own = storeName === undefined ? [] : [storeName]
	const processor = ctx.processor(NodePrefix.TABLE_TRANSFORM_VALUES, name, [table.node], node.supplier, own)
	connectStores(ctx, processor, options.stores)
	return new KTableImpl(ctx, processor, format, node)
}

/** Registering a definition again, or naming it in a processor's `stores`, only connects processors to it. */
export function addStateStore(ctx: DslContext, definition: StoreDefinition<StateStore>): void {
	ctx.builder.addStateStore(storeBuilder(ctx, definition), [], { shared: true })
}

/** Connects the processor to its stores, registering the definitions among them that are not registered yet. */
function connectStores(ctx: DslContext, processor: NodeName, stores: ConnectedStores = []): void {
	for (const store of stores) {
		if (typeof store === 'string') {
			ctx.builder.connectProcessorAndStores(processor, [store])
		} else {
			ctx.builder.addStateStore(storeBuilder(ctx, store), [processor], { shared: true })
		}
	}
}

/** The app's one builder of a definition. */
function storeBuilder(ctx: DslContext, definition: StoreDefinition<StateStore>): StoreBuilder {
	let builder = ctx.definedStores.get(definition)
	if (builder === undefined) {
		builder = definedStoreBuilder(definition, ctx.changelog)
		ctx.definedStores.set(definition, builder)
	}
	return builder
}
