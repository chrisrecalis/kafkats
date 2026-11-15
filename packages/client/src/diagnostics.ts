import { channel, tracingChannel, type TracingChannel } from 'node:diagnostics_channel'

import type { Message, TopicPartition, TopicPartitionOffset } from '@/consumer/types.js'
import type { ProducerMessage, SendResult } from '@/producer/types.js'

/**
 * What the client tells the outside about its own work, published on `node:diagnostics_channel`.
 *
 * Only things a broker cannot see are published here: how long an operation took from the
 * application's side, what the application's handlers did, and what is waiting inside the client.
 * Topic offsets, consumer group lag and broker-side request rates are the business of broker
 * exporters, not of this module.
 *
 * Every operation is a tracing channel (`start`, `end`, `asyncStart`, `asyncEnd`, `error`) whose
 * events carry one of the context objects below; Node adds `result` or `error` to it when the
 * operation settles. Nothing is built and nothing is published for a channel without subscribers.
 * The client depends on no telemetry library: `@kafkats/opentelemetry` turns these events into
 * spans and metrics, and anything else can subscribe the same way.
 */

/** `producer.send()` / `tx.send()`: from the call until every message is acknowledged. */
export interface SendDiagnostics {
	clientId: string
	topic: string
	transactional: boolean
	/**
	 * The messages about to be sent, before they are encoded. A `start` subscriber may add headers
	 * (trace context); they are sent with the message. These are copies: the caller's objects are
	 * not modified.
	 */
	messages: Array<ProducerMessage<unknown, unknown> & { headers: Record<string, string | Buffer> }>
	result?: SendResult | SendResult[]
	error?: unknown
}

/** One Produce request to one broker: what batching and retries look like on the wire. */
export interface ProduceRequestDiagnostics {
	clientId: string
	brokerId: number
	/** Partitions (one batch each), records and encoded bytes in the request. */
	partitions: number
	records: number
	bytes: number
	error?: unknown
}

/** One call of the application's handler: a message (`runEach`) or a batch (`runBatch`). */
export interface ProcessDiagnostics {
	clientId: string
	groupId: string
	topic: string
	partition: number
	/** The messages handed to the handler, with their headers (trace context). */
	messages: Array<Message<unknown, unknown>>
	/** Whether the handler received them as one batch. */
	batch: boolean
	/**
	 * A `start` subscriber may set this to run the handler inside its own asynchronous context,
	 * so that what the handler does (an active span) is attributed to it.
	 */
	runInContext?: <T>(handler: () => T) => T
	error?: unknown
}

/** One OffsetCommit request of a consumer. */
export interface CommitDiagnostics {
	clientId: string
	groupId: string
	/** The offsets being committed (the next offset to consume per partition). */
	offsets: TopicPartitionOffset[]
	error?: unknown
}

/** One rebalance as this member went through it: from stopping work until it has its new assignment. */
export interface RebalanceDiagnostics {
	clientId: string
	groupId: string
	/** The assignment after the rebalance; set when it has completed. */
	assignment?: TopicPartition[]
	error?: unknown
}

/** A producer or consumer was created. Its `metrics()` can be read for as long as it is used. */
export interface InstanceDiagnostics {
	type: 'producer' | 'consumer'
	instance: { metrics(): unknown }
}

export const diagnostics = {
	send: tracingChannel<unknown, SendDiagnostics>('kafkats:producer:send'),
	produceRequest: tracingChannel<unknown, ProduceRequestDiagnostics>('kafkats:producer:request'),
	process: tracingChannel<unknown, ProcessDiagnostics>('kafkats:consumer:process'),
	commit: tracingChannel<unknown, CommitDiagnostics>('kafkats:consumer:commit'),
	rebalance: tracingChannel<unknown, RebalanceDiagnostics>('kafkats:consumer:rebalance'),
	instances: channel('kafkats:instance'),
} as const

/** Whether anything listens to a tracing channel. `TracingChannel.hasSubscribers` needs Node 20.13. */
export function hasSubscribers(traced: TracingChannel<unknown, object>): boolean {
	return (
		traced.start.hasSubscribers ||
		traced.end.hasSubscribers ||
		traced.asyncStart.hasSubscribers ||
		traced.asyncEnd.hasSubscribers ||
		traced.error.hasSubscribers
	)
}

/**
 * Run `operation` as one traced operation of `traced`. Without subscribers this is a plain call:
 * `describe` is not invoked and `operation` gets no context.
 */
export function trace<C extends object, T>(
	traced: TracingChannel<unknown, C>,
	describe: () => C,
	operation: (context?: C) => Promise<T>
): Promise<T> {
	if (!hasSubscribers(traced)) return operation()
	const context = describe()
	return traced.tracePromise(() => {
		const { runInContext } = context as { runInContext?: ProcessDiagnostics['runInContext'] }
		return runInContext ? runInContext(() => operation(context)) : operation(context)
	}, context)
}
