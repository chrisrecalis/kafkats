/** Resolved internal configuration types shared by the runtime modules (type-only; resolved by app/config.ts). */
import type {
	DeserializationExceptionHandler,
	ProcessingExceptionHandler,
	ProductionExceptionHandler,
	UncaughtExceptionHandler,
} from '@/api/handlers.js'
import type { RecordContext } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { OffsetAndMetadata } from '@/runtime/io.js'
import type { ResetPolicy } from '@/topology/nodes.js'

export interface ResolvedRestorationOptions {
	idleTimeoutMs: number
	initialIdleTimeoutMs: number
	checkIntervalMs: number
	consumerMaxWaitMs?: number
	/** Restore jobs running at once (default 4). */
	maxConcurrentTasks: number
}

export interface ResolvedHandlers {
	deserialization: DeserializationExceptionHandler
	processing: ProcessingExceptionHandler
	production: ProductionExceptionHandler
	uncaught: UncaughtExceptionHandler
}

/** RecordQueue's view: the resolved deserialization handler. */
export type ResolvedDeserializationHandler = DeserializationExceptionHandler

/** ProcessorNode's error route: returns normally for 'continue', throws for 'fail' and runtime-control errors. */
export type ProcessingErrorRoute = (
	error: unknown,
	record: StreamRecord<unknown, unknown> | null,
	ctx: RecordContext | null
) => void

export interface ResolvedFlowConfig {
	readonly applicationId: string
	readonly processId: string
	readonly eos: boolean
	readonly commitIntervalMs: number
	readonly taskTimeoutMs: number
	readonly startTimeoutMs: number
	readonly maxTaskIdleMs: number
	readonly bufferedRecordsPerPartition: number
	readonly emitIntervalMs: number
	readonly repartitionPurgeIntervalMs: number
	readonly transactionTimeoutMs: number
	readonly rebalanceTimeoutMs: number
	readonly defaultTimestampExtractor: TimestampExtractor
	readonly defaultResetPolicy: ResetPolicy
	readonly handlers: ResolvedHandlers
	readonly restoration: ResolvedRestorationOptions
}

/**
 * Partition counts for sinks, one per Run, shared by every TaskOutput of its collector. `get` is
 * synchronous; an entry older than 60 s triggers one background refresh (single-flight per topic) through
 * the producer's metadata, whose result replaces the entry; refresh errors are logged and keep the old
 * value. Static sink topics are loaded before the Run starts processing.
 */
export interface PartitionCountCache {
	get(topic: string): number | undefined
	load(topic: string, signal: AbortSignal): Promise<number>
}

/** Told about every successful commit. RepartitionPurger implements it; tests use a no-op. */
export interface CommitListener {
	noteCommitted(offsets: readonly OffsetAndMetadata[]): void
}
