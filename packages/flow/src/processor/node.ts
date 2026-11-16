/**
 * A processor instance of one task with its children (KS `ProcessorNode`). The lean path: a synchronous processor
 * whose subtree is synchronous allocates no promise; a thenable result gets the error route attached.
 */
import { isConnectionFailure, KafkaError } from '@kafkats/client'
import {
	FailedProcessingError,
	IllegalStateError,
	StreamsError,
	TaskCorruptedError,
	TaskMigratedError,
	TopologyError,
} from '@/api/errors.js'
import type { ErrorHandlerContext, HandlerResponse, ProcessingExceptionHandler } from '@/api/handlers.js'
import type {
	ForwardResult,
	InternalProcessorContext,
	MaybePromise,
	Processor,
	Punctuator,
	RecordContext,
	TaskId,
} from '@/processor/api.js'
import { ForwardGuardError, type ProcessorContextImpl } from '@/processor/context.js'
import type { StreamRecord } from '@/processor/record.js'
import type { ProcessingErrorRoute } from '@/runtime/config.js'

/** What the error routes need of a processor context. */
export type HandlerScope = Pick<InternalProcessorContext, 'processorName' | 'taskId' | 'recordDropped'>

export function isThenable(value: unknown): value is PromiseLike<unknown> {
	return typeof (value as PromiseLike<unknown> | undefined)?.then === 'function'
}

export class ProcessorNode<KIn, VIn, KOut, VOut> {
	private readonly childList: ProcessorNode<KOut, VOut, unknown, unknown>[] = []
	private readonly childMap = new Map<string, ProcessorNode<KOut, VOut, unknown, unknown>>()

	constructor(
		readonly name: string,
		private readonly processor: Processor<KIn, VIn, KOut, VOut>,
		readonly context: ProcessorContextImpl<KOut, VOut>,
		private readonly onError: ProcessingErrorRoute
	) {
		context.attach(this as ProcessorNode<unknown, unknown, KOut, VOut>)
	}

	/** In connection order. */
	get children(): readonly ProcessorNode<KOut, VOut, unknown, unknown>[] {
		return this.childList
	}

	get childByName(): ReadonlyMap<string, ProcessorNode<KOut, VOut, unknown, unknown>> {
		return this.childMap
	}

	addChild(child: ProcessorNode<KOut, VOut, unknown, unknown>): void {
		this.childList.push(child)
		this.childMap.set(child.name, child)
	}

	/** No record context; forward() is not allowed (KS: failures are wrapped). */
	async init(): Promise<void> {
		try {
			await this.processor.init?.(this.context)
		} catch (error) {
			throw new StreamsError(`failed to initialize processor '${this.name}'`, { cause: error })
		}
	}

	/** Synchronous when the processor and its subtree are; a promise otherwise. Errors per the processing handler. */
	process(record: StreamRecord<KIn, VIn>): ForwardResult {
		this.context.enter(record.timestamp, record.headers)
		let result: MaybePromise<void>
		try {
			result = this.processor.process(record)
		} catch (error) {
			return this.fail(error, record)
		}
		return this.settle(result, record)
	}

	/** Runs a punctuator of this node: record context with the punctuation time, no input record (KS). */
	punctuate(timestamp: number, punctuator: Punctuator): ForwardResult {
		this.context.enter(timestamp, {})
		let result: MaybePromise<void>
		try {
			result = punctuator(timestamp)
		} catch (error) {
			return this.fail(error, null)
		}
		return this.settle(result, null)
	}

	async close(): Promise<void> {
		try {
			await this.processor.close?.()
		} catch (error) {
			throw new StreamsError(`failed to close processor '${this.name}'`, { cause: error })
		}
	}

	private settle(result: MaybePromise<void>, record: StreamRecord<KIn, VIn> | null): ForwardResult {
		if (!isThenable(result)) {
			this.context.leave()
			return
		}
		return result.then(
			() => this.context.leave(),
			(error: unknown) => this.fail(error, record)
		)
	}

	private fail(error: unknown, record: StreamRecord<KIn, VIn> | null): void {
		try {
			this.onError(error, record as StreamRecord<unknown, unknown> | null, this.context.recordContext())
		} finally {
			this.context.exit()
		}
	}
}

/**
 * Errors that are runtime control, never seen by the processing handler and never caught by upstream nodes (KS
 * rethrows FailedProcessing, TaskCorrupted and TaskMigrated): flow's task errors, forward guards, topology errors
 * (fatal at any site, e.g. a store the processor is not connected to), and every Kafka or connection error
 * (classified by the runtime: fatal codes stop the app, the rest migrate).
 */
export function isRuntimeControl(error: unknown): boolean {
	return (
		error instanceof FailedProcessingError ||
		error instanceof TaskMigratedError ||
		error instanceof TaskCorruptedError ||
		error instanceof IllegalStateError ||
		error instanceof ForwardGuardError ||
		error instanceof TopologyError ||
		error instanceof KafkaError ||
		isConnectionFailure(error)
	)
}

/** What a handler sees of a failure at `context`'s node; `record` is null in init(), close() and punctuators. */
export function errorHandlerContext(
	context: Pick<HandlerScope, 'processorName' | 'taskId'>,
	record: RecordContext | null
): ErrorHandlerContext {
	return {
		topic: record?.topic ?? null,
		partition: record?.partition ?? -1,
		offset: record?.offset ?? -1n,
		headers: record?.headers ?? {},
		timestamp: record?.timestamp ?? -1,
		processorNodeId: context.processorName,
		taskId: context.taskId,
		sourceRawKey: record?.rawKey ?? null,
		sourceRawValue: record?.rawValue ?? null,
	}
}

/**
 * Applies an exception handler's answer (KS): returns on 'continue'; a 'fail', a throwing handler or any other answer
 * is a FailedProcessingError (fatal) whose cause is the original error, or the handler's own error.
 */
export function applyHandler(answer: () => HandlerResponse, error: unknown, failure: string, taskId: TaskId): void {
	let response: unknown
	try {
		response = answer()
	} catch (handlerError) {
		throw new FailedProcessingError(`${failure}; the exception handler threw`, { cause: handlerError, taskId })
	}
	if (response === 'continue') {
		return
	}
	const invalid =
		response === 'fail' ? '' : `; the exception handler answered ${String(response)}, not 'fail' or 'continue'`
	throw new FailedProcessingError(failure + invalid, { cause: error, taskId })
}

/** The error route of one processor node (4.6.4): 'continue' drops the record, keeping earlier nodes' effects. */
export function processingErrorRoute(handler: ProcessingExceptionHandler, context: HandlerScope): ProcessingErrorRoute {
	return (error, record, recordContext) => {
		if (isRuntimeControl(error)) {
			throw error
		}
		const ctx = errorHandlerContext(context, recordContext)
		applyHandler(
			() => handler(ctx, record, error),
			error,
			`processing failed in '${context.processorName}'`,
			context.taskId
		)
		context.recordDropped('processing', { error: error instanceof Error ? error.message : String(error) })
	}
}
