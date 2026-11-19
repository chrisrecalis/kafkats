import {
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	TraceFlags,
	context,
	propagation,
	trace,
	type Attributes,
	type Context,
	type Link,
	type Span,
	type TextMapGetter,
	type TextMapSetter,
	type Tracer,
} from '@opentelemetry/api'
import { diagnostics, type Message } from '@kafkats/client'

import { ATTR, SYSTEM_KAFKA, errorType } from './attributes.js'
import { onOperation } from './operations.js'

type IncomingHeaders = Message<unknown, unknown>['headers']

const intoHeaders: TextMapSetter<Record<string, string>> = {
	set: (headers, key, value) => {
		headers[key] = value
	},
}

/** Reads trace context from the headers of a consumed message. */
export const fromHeaders: TextMapGetter<IncomingHeaders> = {
	keys: headers => Object.keys(headers),
	get: (headers, key) => headers[key]?.toString(),
}

/** Ends `span`, as failed when `error` is set. */
export function endSpan(span: Span, error: unknown): void {
	if (error !== undefined) {
		span.recordException(error instanceof Error ? error : String(error))
		span.setStatus({ code: SpanStatusCode.ERROR })
		span.setAttribute(ATTR.errorType, errorType(error))
	}
	span.end()
}

/**
 * Whether `parent` belongs to a trace that was decided not to be sampled. Such a trace gets no
 * spans here: with a small sampling rate that is nearly every message, and a span that records
 * nothing still costs as much to create as one that does. This follows the parent's decision, as
 * the SDK's default sampler does.
 */
export function notSampled(parent: Context): boolean {
	const origin = trace.getSpanContext(parent)
	return origin !== undefined && (origin.traceFlags & TraceFlags.SAMPLED) === 0
}

/**
 * Spans for what the application does with messages, connected across the topic.
 *
 * - A `send` span per `send()` call. Its context is written into the headers of every message of
 *   the call, in the format of the configured propagator (W3C `traceparent` by default).
 * - A `process` span per handler call. For a single message it continues the trace of the send
 *   that produced it. A batch has no single origin, so its span starts a trace and links to the
 *   sends of its messages. The handler runs with the span active, so spans it creates are children.
 * - No span for a send or a message inside a trace that is not sampled (see {@link notSampled}).
 */
export function traceMessages(tracer: Tracer): () => void {
	const stopSends = onOperation(diagnostics.send, {
		started(send) {
			const attributes: Attributes = {
				[ATTR.system]: SYSTEM_KAFKA,
				[ATTR.topic]: send.topic,
				[ATTR.operationName]: 'send',
				[ATTR.operationType]: 'send',
				[ATTR.clientId]: send.clientId,
			}
			if (send.messages.length > 1) attributes[ATTR.batchSize] = send.messages.length
			const caller = context.active()
			// A send inside a trace that is not sampled gets no span of its own; the caller's context
			// still travels with the messages, so the consumer knows the trace is not sampled either.
			const span = notSampled(caller)
				? undefined
				: tracer.startSpan(`send ${send.topic}`, { kind: SpanKind.PRODUCER, attributes }, caller)
			// One context for all the messages of the call.
			const headers: Record<string, string> = {}
			propagation.inject(span ? trace.setSpan(caller, span) : caller, headers, intoHeaders)
			send.addHeaders(headers)
			return span
		},
		finished(send, span, error) {
			if (!span) return
			// One message: where it ended up. A batch can be spread over partitions.
			if (send.result && !Array.isArray(send.result)) {
				span.setAttribute(ATTR.partition, String(send.result.partition))
				span.setAttribute(ATTR.offset, Number(send.result.offset))
			}
			endSpan(span, error)
		},
	})

	const stopProcessing = onOperation(diagnostics.process, {
		started(processing) {
			const attributes: Attributes = {
				[ATTR.system]: SYSTEM_KAFKA,
				[ATTR.topic]: processing.topic,
				[ATTR.partition]: String(processing.partition),
				[ATTR.operationName]: 'process',
				[ATTR.operationType]: 'process',
				[ATTR.consumerGroup]: processing.groupId,
				[ATTR.clientId]: processing.clientId,
			}
			let parent = ROOT_CONTEXT
			let links: Link[] = []
			if (processing.batch) {
				attributes[ATTR.batchSize] = processing.messages.length
				links = sendsOf(processing.messages)
			} else {
				const [message] = processing.messages
				if (message) parent = propagation.extract(ROOT_CONTEXT, message.headers, fromHeaders)
				attributes[ATTR.offset] = Number(message?.offset)
			}
			// A message of a trace that is not sampled: no span, but the handler still runs in that
			// trace's context, so what it sends on is not sampled either.
			const span = notSampled(parent)
				? undefined
				: tracer.startSpan(
						`process ${processing.topic}`,
						{ kind: SpanKind.CONSUMER, attributes, links },
						parent
					)
			const active = span ? trace.setSpan(parent, span) : parent
			processing.runHandlerIn(handler => context.with(active, handler))
			return span
		},
		finished(_, span, error) {
			if (span) endSpan(span, error)
		},
	})

	return () => {
		stopSends()
		stopProcessing()
	}
}

/**
 * A link to every send that produced one of `messages`. The messages of one `send()` call carry
 * the same trace headers, and a batch is often thousands of messages from a few calls, so the
 * headers are compared as bytes and each distinct set is parsed once.
 */
function sendsOf(messages: ReadonlyArray<Message<unknown, unknown>>): Link[] {
	const fields = propagation.fields()
	const links = new Map<string, Link | undefined>()
	for (const { headers } of messages) {
		const carried = fields.map(field => headers[field]?.toString('latin1') ?? '').join('\n')
		if (links.has(carried)) continue
		const origin = trace.getSpanContext(propagation.extract(ROOT_CONTEXT, headers, fromHeaders))
		links.set(carried, origin && { context: origin })
	}
	return [...links.values()].filter(link => link !== undefined)
}
