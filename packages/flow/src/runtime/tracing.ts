/**
 * The `kafkats:flow:process` tracing channel (6.26, SI16): one operation per record a task processes, from the moment
 * the record enters its source node until its subtree settled. The record's headers carry the trace context of the send
 * that produced it, and a subscriber may ask to process the record inside its own asynchronous context (`runIn`), so
 * that what the processors do, sends included, is attributed to it. That is how `@kafkats/opentelemetry` keeps the
 * records a topology produces in the trace of the record it consumed, as the client does for a handler call
 * (`ProcessDiagnostics.runHandlerIn`); flow itself imports no `async_hooks`. Without subscribers nothing is built and
 * nothing is published: the scheduler asks the channel once per slice.
 */
import { tracingChannel } from 'node:diagnostics_channel'
import type { Message } from '@kafkats/client'
import type { ForwardResult, TaskId } from '@/processor/api.js'

/** Runs a function, usually inside some asynchronous context, and returns what it returns. */
export type FlowProcessScope = <T>(run: () => T) => T

/** One record a task processes: the context of a `kafkats:flow:process` operation. */
export interface FlowProcessDiagnostics {
	readonly applicationId: string
	/** `"<subtopology>_<partition>"` */
	readonly taskId: string
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
	/** The record's headers as consumed (trace context). Not to be modified. */
	readonly headers: Readonly<Record<string, Buffer>>
	/**
	 * For a `start` subscriber: process the record inside `scope`, so that what the processors do is attributed to the
	 * subscriber's asynchronous context (an active span). Every subscriber may ask; the scopes nest in the order they
	 * were asked for.
	 */
	runIn(scope: FlowProcessScope): void
	/** Why processing failed, once it did. */
	error?: unknown
}

/** What flow publishes about its own work on `node:diagnostics_channel`, next to the client's `diagnostics`. */
export const flowDiagnostics = {
	/** `kafkats:flow:process`: one record through the processors of its task, until its subtree settled. */
	process: tracingChannel<unknown, FlowProcessDiagnostics>('kafkats:flow:process'),
} as const

type Traced = Pick<Message<unknown, unknown>, 'topic' | 'partition' | 'offset' | 'headers'>

class RecordProcessing implements FlowProcessDiagnostics {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
	readonly headers: Readonly<Record<string, Buffer>>
	error?: unknown
	/** What subscribers asked the processing to run inside; the first to ask is the outermost. */
	private scopes?: FlowProcessScope[]

	constructor(
		readonly applicationId: string,
		readonly taskId: string,
		record: Traced
	) {
		this.topic = record.topic
		this.partition = record.partition
		this.offset = record.offset
		this.headers = record.headers
	}

	runIn(scope: FlowProcessScope): void {
		;(this.scopes ??= []).push(scope)
	}

	/** Processes the record inside the scopes that were asked for. */
	run(process: () => ForwardResult): ForwardResult {
		if (!this.scopes) {
			return process()
		}
		return this.scopes.reduceRight<() => ForwardResult>((inner, scope) => () => scope(inner), process)()
	}
}

/**
 * `process` (the record through its task's processors) as one operation of `flowDiagnostics.process`. The result is
 * a promise while the channel has subscribers, also for a record processed synchronously, and `asyncEnd` is always the
 * operation's last event: a synchronous throw is turned into a rejection.
 */
export function traceProcess(
	applicationId: string,
	taskId: TaskId,
	record: Traced,
	process: () => ForwardResult
): ForwardResult {
	const processing = new RecordProcessing(applicationId, taskId.key, record)
	return flowDiagnostics.process.tracePromise(() => {
		try {
			return Promise.resolve(processing.run(process))
		} catch (error) {
			return Promise.reject(error)
		}
	}, processing)
}
