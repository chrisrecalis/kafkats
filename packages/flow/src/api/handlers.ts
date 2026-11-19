import type { Message } from '@kafkats/client'
import type { TaskId } from '@/processor/api.js'
import type { CollectedRecord } from '@/processor/output.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'

/** Where a failure happened (Kafka Streams' ErrorHandlerContext). */
export interface ErrorHandlerContext {
	/** The task's input record; null/-1/-1n in punctuators. */
	readonly topic: string | null
	readonly partition: number
	readonly offset: bigint
	/** Record context at the failing node. */
	readonly headers: RecordHeaders
	readonly timestamp: number
	readonly processorNodeId: string | null
	readonly taskId: TaskId
	readonly sourceRawKey: Buffer | null
	readonly sourceRawValue: Buffer | null
}

/** 'fail' stops the app (the uncaught exception handler decides what then); 'continue' drops the record. */
export type HandlerResponse = 'fail' | 'continue'

export type DeserializationExceptionHandler = (
	ctx: ErrorHandlerContext,
	record: Message<Buffer, Buffer>,
	error: unknown
) => HandlerResponse

export type ProcessingExceptionHandler = (
	ctx: ErrorHandlerContext,
	record: StreamRecord<unknown, unknown> | null,
	error: unknown
) => HandlerResponse

export type ProductionExceptionHandler = (
	ctx: ErrorHandlerContext,
	record: CollectedRecord,
	error: unknown,
	origin: 'serialization' | 'send'
) => HandlerResponse

/** 'shutdown' stops the app with state ERROR; 'restart' replaces the failed run with a new one. */
export type UncaughtExceptionResponse = 'shutdown' | 'restart'
export type UncaughtExceptionHandler = (error: Error) => UncaughtExceptionResponse
