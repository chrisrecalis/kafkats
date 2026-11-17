/**
 * processValues (KS `KStreamImpl.processValues`): a FixedKeyProcessor runs as a processor whose records it receives
 * and forwards as FixedKeyRecords, which have no `withKey()`.
 */
import type {
	FixedKeyProcessor,
	FixedKeyProcessorSupplier,
	MaybePromise,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
} from '@/processor/api.js'
import { FixedKeyProcessorContextImpl } from '@/processor/context-views.js'
import { FixedKeyRecord, type StreamRecord } from '@/processor/record.js'

export function fixedKeyProcessorSupplier<K, VIn, VOut>(
	supplier: FixedKeyProcessorSupplier<K, VIn, VOut>
): ProcessorSupplier<K, VIn, K, VOut> {
	return () => new FixedKeyProcessorAdapter(supplier())
}

class FixedKeyProcessorAdapter<K, VIn, VOut> implements Processor<K, VIn, K, VOut> {
	constructor(private readonly processor: FixedKeyProcessor<K, VIn, VOut>) {}

	init(context: ProcessorContext<K, VOut>): MaybePromise<void> {
		return this.processor.init?.(new FixedKeyProcessorContextImpl(context))
	}

	process(record: StreamRecord<K, VIn>): MaybePromise<void> {
		const { key, value, timestamp, headers } = record
		return this.processor.process(new FixedKeyRecord(key, value, timestamp, headers))
	}

	close(): MaybePromise<void> {
		return this.processor.close?.()
	}
}
