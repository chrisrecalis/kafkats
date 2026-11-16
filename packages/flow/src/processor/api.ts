import type { StoreDefinition } from '@/api/stores.js'
import type { TaskOutput } from '@/processor/output.js'
import type { FixedKeyRecord, RecordHeaders, StreamRecord } from '@/processor/record.js'
import type { StateStore } from '@/state.js'

export type MaybePromise<T> = T | Promise<T>

/** Where the input record of a task comes from. */
export interface RecordMetadata {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
}

export type PunctuationType = 'stream-time' | 'wall-clock'
/** Receives the current stream time / wall clock (not the due time), as in Kafka Streams. */
export type Punctuator = (timestamp: number) => MaybePromise<void>
export interface Cancellable {
	cancel(): void
}

/** Subtopology x partition. Interned: one frozen object per id, so `===` and Map keys work. */
export interface TaskId {
	readonly subtopology: number
	readonly partition: number
	/** `"<subtopology>_<partition>"` */
	readonly key: string
}

/** void when the downstream subtree completed synchronously; a promise otherwise. */
export type ForwardResult = void | Promise<void>

export interface ProcessorContext<KOut = never, VOut = never> {
	readonly applicationId: string
	readonly taskId: TaskId
	readonly processorName: string
	/** Input record of the task being processed; null in init() and in punctuators. */
	recordMetadata(): RecordMetadata | null
	/** Task stream time (max timestamp polled by the task), -1 while unknown. */
	currentStreamTimeMs(): number
	/** Wall clock captured at the start of the current slice. */
	currentSystemTimeMs(): number
	/** A store connected to this processor (TopologyError otherwise). The definition overload is typed. */
	getStateStore<S extends StateStore>(store: StoreDefinition<S>): S
	getStateStore<S extends StateStore = StateStore>(name: string): S
	/**
	 * Depth-first to the children in connection order (or only `childName`). The result MUST be awaited or
	 * returned before the next forward and before process() or the punctuator settles. Calling forward outside
	 * process() or a punctuator of this node (init() included), or while a previous forward of this node is
	 * pending, throws StreamsError; so does a process() that settles with a pending forward.
	 */
	forward<K extends KOut, V extends VOut>(record: StreamRecord<K, V>, childName?: string): ForwardResult
	/** intervalMs >= 1. stream-time: first due 0 (fires once stream time is known); wall-clock: now + interval. */
	schedule(intervalMs: number, type: PunctuationType, punctuator: Punctuator): Cancellable
	/** Requests a commit of all tasks at the next commit barrier. */
	commit(): void
}

export interface FixedKeyProcessorContext<K, VOut> extends Omit<ProcessorContext<K, VOut>, 'forward'> {
	forward<V extends VOut>(record: FixedKeyRecord<K, V>, childName?: string): ForwardResult
}

export interface Processor<KIn, VIn, KOut = never, VOut = never> {
	init?(context: ProcessorContext<KOut, VOut>): MaybePromise<void>
	process(record: StreamRecord<KIn, VIn>): MaybePromise<void>
	close?(): MaybePromise<void>
}

export interface FixedKeyProcessor<K, VIn, VOut = never> {
	init?(context: FixedKeyProcessorContext<K, VOut>): MaybePromise<void>
	process(record: FixedKeyRecord<K, VIn>): MaybePromise<void>
	close?(): MaybePromise<void>
}

/** MUST return a new instance per call (checked at registration by calling it twice). */
export type ProcessorSupplier<KIn, VIn, KOut = never, VOut = never> = () => Processor<KIn, VIn, KOut, VOut>
export type FixedKeyProcessorSupplier<K, VIn, VOut = never> = () => FixedKeyProcessor<K, VIn, VOut>

/** The context of a KTable.transformValues transformer: everything but forwarding. */
export type ForwardingDisabledContext = Omit<ProcessorContext<never, never>, 'forward'>

/** KTable.transformValues (Kafka Streams' ValueTransformerWithKey): exactly one result per call, no forwarding. */
export interface ValueTransformerWithKey<K, V, VR> {
	init?(context: ForwardingDisabledContext): MaybePromise<void>
	/** Also called with null: tombstones reach the transformer. */
	transform(key: K, value: V | null): MaybePromise<VR | null>
	close?(): MaybePromise<void>
}
export type ValueTransformerWithKeySupplier<K, V, VR> = () => ValueTransformerWithKey<K, V, VR>

// ----------------------------------------------------------------------------------------------------------
// Internal context: not exported publicly; DSL operators narrow `context` to it.
// ----------------------------------------------------------------------------------------------------------

/** The record context at the current node. */
export interface RecordContext {
	/** The task's input record; null/-1/-1n in punctuators. */
	readonly topic: string | null
	readonly partition: number
	readonly offset: bigint
	/** Of the record at the current node (follows forwards). */
	readonly timestamp: number
	readonly headers: RecordHeaders
	readonly rawKey: Buffer | null
	readonly rawValue: Buffer | null
}

/**
 * The processor metadata of a task: processor clocks (`<store>`), store clocks (`<store>.observed`) and
 * emit progress (`<store>.emit`), persisted in the commit metadata of the task's input partitions. One
 * object per task, shared by its contexts, processor clocks and store clocks; implemented by
 * `processor/metadata.ts`.
 */
export interface ProcessorMetadata {
	get(key: string): number | undefined
	/** Sets the value; the task then needs a commit. */
	put(key: string, value: number): void
}

export type DroppedReason =
	| 'null-key'
	| 'null-value'
	| 'late'
	| 'expired-window'
	| 'negative-timestamp'
	| 'deserialization'
	| 'processing'
	| 'production'

export interface InternalProcessorContext<KOut = never, VOut = never> extends ProcessorContext<KOut, VOut> {
	/** The task's processor metadata, keyed by store name. */
	readonly metadata: ProcessorMetadata
	readonly config: Readonly<{ emitIntervalMs: number; eos: boolean }>
	recordContext(): RecordContext | null
	/** Logs and counts a dropped record at most once per second per reason per task. */
	recordDropped(reason: DroppedReason, detail?: Readonly<Record<string, unknown>>): void
	/** Sinks and logging layers only. */
	output(): TaskOutput
}
