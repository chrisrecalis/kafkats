/** Processor API: process, processValues, KTable.transformValues and app.addStateStore (6.12, 6.21). */
import type { KStream } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type { ProcessOptions, TransformValuesOptions } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { DslContext } from '@/dsl/context.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { FixedKeyProcessorSupplier, ProcessorSupplier, ValueTransformerWithKeySupplier } from '@/processor/api.js'
import type { StateStore } from '@/state.js'

export function process<K, V, KR, VR>(
	_stream: KStreamImpl<K, V>,
	_supplier: ProcessorSupplier<K, V, KR, VR>,
	_options?: ProcessOptions
): KStream<KR, VR> {
	return notImplemented('KStream.process')
}

export function processValues<K, V, VR>(
	_stream: KStreamImpl<K, V>,
	_supplier: FixedKeyProcessorSupplier<K, V, VR>,
	_options?: ProcessOptions
): KStream<K, VR> {
	return notImplemented('KStream.processValues')
}

export function transformValues<K, V, VR>(
	_table: KTableImpl<K, V>,
	_supplier: ValueTransformerWithKeySupplier<K, V, VR>,
	_options?: TransformValuesOptions<K, VR>
): KTable<K, VR> {
	return notImplemented('KTable.transformValues')
}

export function addStateStore(_ctx: DslContext, _definition: StoreDefinition<StateStore>): void {
	notImplemented('FlowApp.addStateStore')
}
