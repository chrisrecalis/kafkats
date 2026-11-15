import { tracingChannel, type TracingChannel } from 'node:diagnostics_channel'

import type { ConsumerMetrics, Message, TopicPartition, TopicPartitionOffset } from '@/consumer/types.js'
import type { ProducerMessage, ProducerMetrics, SendResult } from '@/producer/types.js'

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

/** Runs a function, usually inside some asynchronous context, and returns what it returns. */
export type Scope = <T>(run: () => T) => T

/** `producer.send()` / `tx.send()`: from the call until every message is acknowledged. */
export interface SendDiagnostics {
	clientId: string
	topic: string
	transactional: boolean
	/** The messages about to be sent, as the caller passed them. Not to be modified. */
	messages: ReadonlyArray<Readonly<ProducerMessage<unknown, unknown>>>
	/**
	 * For a `start` subscriber: send these headers (trace context) with the message at `index`, or
	 * with every message of the call when no index is given. The caller's objects are not touched.
	 */
	addHeaders(headers: Record<string, string | Buffer>, index?: number): void
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
	/**
	 * Partitions the broker answered with an error code, set once the response is in. Such a
	 * request did not fail as a whole (`error` stays unset), and its batches may be retried.
	 */
	rejected?: Array<{ topic: string; partition: number; errorCode: number }>
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
	 * For a `start` subscriber: run the handler inside `scope`, so that what the handler does is
	 * attributed to the subscriber's asynchronous context (an active span). Every subscriber may
	 * ask; the scopes nest in the order they were asked for.
	 */
	runHandlerIn(scope: Scope): void
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

/** A producer or consumer that is still in use, with what identifies it. */
export type InstanceDiagnostics =
	| { type: 'producer'; clientId: string; instance: { metrics(): ProducerMetrics } }
	| { type: 'consumer'; clientId: string; groupId: string; instance: { metrics(): ConsumerMetrics } }

type Identity = { type: 'producer'; clientId: string } | { type: 'consumer'; clientId: string; groupId: string }
type Instance = InstanceDiagnostics['instance']

/** The producers and consumers of this process, held weakly: one that is no longer used drops out. */
const registered = new Map<WeakRef<Instance>, Identity>()
const forget = new FinalizationRegistry<WeakRef<Instance>>(reference => {
	registered.delete(reference)
})

/** Make a producer or consumer visible to `diagnostics.instances()`. */
export function register(
	identity: { type: 'producer'; clientId: string },
	instance: { metrics(): ProducerMetrics }
): void
export function register(
	identity: { type: 'consumer'; clientId: string; groupId: string },
	instance: { metrics(): ConsumerMetrics }
): void
export function register(identity: Identity, instance: Instance): void {
	const reference = new WeakRef(instance)
	registered.set(reference, identity)
	forget.register(instance, reference)
}

export const diagnostics = {
	send: tracingChannel<unknown, SendDiagnostics>('kafkats:producer:send'),
	produceRequest: tracingChannel<unknown, ProduceRequestDiagnostics>('kafkats:producer:request'),
	process: tracingChannel<unknown, ProcessDiagnostics>('kafkats:consumer:process'),
	commit: tracingChannel<unknown, CommitDiagnostics>('kafkats:consumer:commit'),
	rebalance: tracingChannel<unknown, RebalanceDiagnostics>('kafkats:consumer:rebalance'),
	/**
	 * The producers and consumers of this process that are still in use, whenever they were
	 * created. Read their `metrics()` on whatever schedule a monitor collects.
	 */
	instances(): InstanceDiagnostics[] {
		const live: InstanceDiagnostics[] = []
		for (const [reference, identity] of registered) {
			const instance = reference.deref()
			if (instance) live.push({ ...identity, instance } as InstanceDiagnostics)
		}
		return live
	},
} as const

/**
 * Run `operation` as one traced operation of `traced`. Without subscribers this is a plain call:
 * `describe` is not invoked and `operation` gets no context.
 */
export function trace<C extends object, D extends C, T>(
	traced: TracingChannel<unknown, C>,
	describe: () => D,
	operation: (context?: D) => Promise<T>
): Promise<T> {
	if (!traced.hasSubscribers) return operation()
	const context = describe()
	// A synchronous throw of the operation is turned into a rejection, so that subscribers always
	// see `asyncEnd`, whichever way the operation ends. (Not an async function: that would put
	// three more microtask turns between every operation and its caller.)
	return traced.tracePromise(() => {
		try {
			return operation(context)
		} catch (error) {
			return Promise.reject(error)
		}
	}, context)
}

/**
 * The context of a send. One object per traced send and nothing more: the per-message work a
 * subscriber may ask for (headers) is only set up when it asks.
 */
export class SendOperation implements SendDiagnostics {
	result?: SendResult | SendResult[]
	error?: unknown
	/** Headers subscribers asked to add, by message index. */
	added?: Array<Record<string, string | Buffer> | undefined>

	constructor(
		readonly clientId: string,
		readonly topic: string,
		readonly transactional: boolean,
		readonly messages: ReadonlyArray<Readonly<ProducerMessage<unknown, unknown>>>
	) {}

	addHeaders(headers: Record<string, string | Buffer>, index?: number): void {
		this.added ??= Array.from({ length: this.messages.length })
		const indexes = index === undefined ? this.messages.keys() : [index]
		for (const i of indexes) this.added[i] = { ...this.added[i], ...headers }
	}
}

/** The context of a handler call. */
export class ProcessOperation implements ProcessDiagnostics {
	error?: unknown
	/** What subscribers asked the handler to run inside; the first to ask is the outermost. */
	private scopes?: Scope[]

	constructor(
		readonly clientId: string,
		readonly groupId: string,
		readonly topic: string,
		readonly partition: number,
		readonly messages: Array<Message<unknown, unknown>>,
		readonly batch: boolean
	) {}

	runHandlerIn(scope: Scope): void {
		;(this.scopes ??= []).push(scope)
	}

	/** Call the handler, inside the scopes that were asked for. */
	run<T>(handler: () => T): T {
		if (!this.scopes) return handler()
		return this.scopes.reduceRight<() => T>((inner, scope) => () => scope(inner), handler)()
	}
}
