/**
 * The contexts user code gets where forwarding is restricted: a ValueTransformerWithKey cannot forward at all (KS
 * `ForwardingDisabledProcessorContext`), a FixedKeyProcessor forwards FixedKeyRecords only. Everything else is the
 * context of the processor that runs the user code.
 */
import type { StoreDefinition } from '@/api/stores.js'
import type {
	Cancellable,
	FixedKeyProcessorContext,
	ForwardResult,
	ForwardingDisabledContext,
	ProcessorContext,
	PunctuationType,
	Punctuator,
	RecordMetadata,
	TaskId,
} from '@/processor/api.js'
import { StreamRecord, type FixedKeyRecord } from '@/processor/record.js'
import type { StateStore } from '@/state.js'

export class ForwardingDisabledProcessorContext implements ForwardingDisabledContext {
	constructor(private readonly context: ProcessorContext<unknown, unknown>) {}

	get applicationId(): string {
		return this.context.applicationId
	}

	get taskId(): TaskId {
		return this.context.taskId
	}

	get processorName(): string {
		return this.context.processorName
	}

	recordMetadata(): RecordMetadata | null {
		return this.context.recordMetadata()
	}

	currentStreamTimeMs(): number {
		return this.context.currentStreamTimeMs()
	}

	currentSystemTimeMs(): number {
		return this.context.currentSystemTimeMs()
	}

	getStateStore<S extends StateStore>(store: StoreDefinition<S>): S
	getStateStore<S extends StateStore = StateStore>(name: string): S
	getStateStore(store: string | StoreDefinition<StateStore>): StateStore {
		return typeof store === 'string' ? this.context.getStateStore(store) : this.context.getStateStore(store)
	}

	schedule(intervalMs: number, type: PunctuationType, punctuator: Punctuator): Cancellable {
		return this.context.schedule(intervalMs, type, punctuator)
	}

	commit(): void {
		this.context.commit()
	}
}

/** processValues: forwards FixedKeyRecords as the records of the processor's own context. */
export class FixedKeyProcessorContextImpl<K, VOut>
	extends ForwardingDisabledProcessorContext
	implements FixedKeyProcessorContext<K, VOut>
{
	constructor(private readonly target: ProcessorContext<K, VOut>) {
		super(target as ProcessorContext<unknown, unknown>)
	}

	forward<V extends VOut>(record: FixedKeyRecord<K, V>, childName?: string): ForwardResult {
		const { key, value, timestamp, headers } = record
		return this.target.forward(new StreamRecord(key, value, timestamp, headers), childName)
	}
}
