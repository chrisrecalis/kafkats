import {
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	context,
	propagation,
	trace,
	type Attributes,
	type Link,
	type Span,
	type TextMapGetter,
	type TextMapSetter,
	type Tracer,
} from '@opentelemetry/api'
import { diagnostics, type Message } from '@kafkats/client'

import { ATTR, SYSTEM_KAFKA, errorType } from './attributes.js'
import { onOperation } from './operations.js'

type OutgoingHeaders = Record<string, string | Buffer>
type IncomingHeaders = Message<unknown, unknown>['headers']

const intoHeaders: TextMapSetter<OutgoingHeaders> = {
	set: (headers, key, value) => {
		headers[key] = value
	},
}

const fromHeaders: TextMapGetter<IncomingHeaders> = {
	keys: headers => Object.keys(headers),
	get: (headers, key) => headers[key]?.toString(),
}

function end(span: Span, error: unknown): void {
	if (error !== undefined) {
		span.recordException(error instanceof Error ? error : String(error))
		span.setStatus({ code: SpanStatusCode.ERROR })
		span.setAttribute(ATTR.errorType, errorType(error))
	}
	span.end()
}

/**
 * Spans for what the application does with messages, connected across the topic.
 *
 * - A `send` span per `send()` call. Its context is written into the headers of every message of
 *   the call, in the format of the configured propagator (W3C `traceparent` by default).
 * - A `process` span per handler call. For a single message it continues the trace of the send
 *   that produced it. A batch has no single origin, so its span starts a trace and links to the
 *   sends of its messages. The handler runs with the span active, so spans it creates are children.
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
			const span = tracer.startSpan(`send ${send.topic}`, { kind: SpanKind.PRODUCER, attributes })
			const sending = trace.setSpan(context.active(), span)
			for (const message of send.messages) propagation.inject(sending, message.headers, intoHeaders)
			return span
		},
		finished(send, span, error) {
			// One message: where it ended up. A batch can be spread over partitions.
			if (send.result && !Array.isArray(send.result)) {
				span.setAttribute(ATTR.partition, String(send.result.partition))
				span.setAttribute(ATTR.offset, Number(send.result.offset))
			}
			end(span, error)
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
			const origins = processing.messages.map(message =>
				propagation.extract(ROOT_CONTEXT, message.headers, fromHeaders)
			)
			let parent = ROOT_CONTEXT
			let links: Link[] = []
			if (processing.batch) {
				attributes[ATTR.batchSize] = processing.messages.length
				links = distinctLinks(origins.map(origin => trace.getSpanContext(origin)))
			} else {
				parent = origins[0] ?? ROOT_CONTEXT
				attributes[ATTR.offset] = Number(processing.messages[0]?.offset)
			}
			const span = tracer.startSpan(
				`process ${processing.topic}`,
				{ kind: SpanKind.CONSUMER, attributes, links },
				parent
			)
			const active = trace.setSpan(parent, span)
			processing.runInContext = handler => context.with(active, handler)
			return span
		},
		finished(_, span, error) {
			end(span, error)
		},
	})

	return () => {
		stopSends()
		stopProcessing()
	}
}

/** One link per distinct sending span: the messages of one `send()` call share theirs. */
function distinctLinks(origins: Array<ReturnType<typeof trace.getSpanContext>>): Link[] {
	const links = new Map<string, Link>()
	for (const origin of origins) {
		if (origin) links.set(`${origin.traceId}:${origin.spanId}`, { context: origin })
	}
	return [...links.values()]
}
