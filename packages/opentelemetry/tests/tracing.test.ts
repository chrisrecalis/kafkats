import { SpanKind, SpanStatusCode, context, trace } from '@opentelemetry/api'
import { diagnostics, type Message, type ProcessDiagnostics, type SendDiagnostics } from '@kafkats/client'
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { ATTR, instrumentKafka } from '../src/index.js'
import { spanRecorder, useW3CContext } from './setup.js'

/** Run a send through the client's channel, the way the producer does. */
function send(messages: number, outcome: () => Promise<unknown> = async () => ({ partition: 2, offset: 40n })) {
	const sending: SendDiagnostics = {
		clientId: 'app',
		topic: 'orders',
		transactional: false,
		messages: Array.from({ length: messages }, () => ({ value: null, headers: {} })),
	}
	const done = diagnostics.send.tracePromise(async () => outcome() as never, sending)
	return { sending, done }
}

const message = (offset: bigint, headers: Record<string, string | Buffer>): Message<unknown, unknown> => ({
	topic: 'orders',
	partition: 1,
	offset,
	timestamp: 0n,
	key: null,
	value: null,
	headers: Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)])),
})

/** Run a handler through the client's channel, the way the consumer does. */
function process(messages: Array<Message<unknown, unknown>>, batch: boolean, handler: () => Promise<void>) {
	const processing: ProcessDiagnostics = {
		clientId: 'app',
		groupId: 'billing',
		topic: 'orders',
		partition: 1,
		messages,
		batch,
	}
	return diagnostics.process.tracePromise(
		async () => (processing.runInContext ? processing.runInContext(handler) : handler()),
		processing
	)
}

describe('tracing', () => {
	let recorder: ReturnType<typeof spanRecorder>
	let stop: () => void

	beforeAll(useW3CContext)
	beforeEach(() => {
		recorder = spanRecorder()
		stop = instrumentKafka({ tracerProvider: recorder.tracerProvider, metrics: false })
	})
	afterEach(() => stop())

	it('a send is a producer span whose context is written into the message headers', async () => {
		const { sending, done } = send(1)
		await done

		const [span] = recorder.spans()
		expect(span!.name).toBe('send orders')
		expect(span!.kind).toBe(SpanKind.PRODUCER)
		expect(span!.attributes).toMatchObject({
			[ATTR.system]: 'kafka',
			[ATTR.topic]: 'orders',
			[ATTR.operationName]: 'send',
			[ATTR.partition]: '2',
			[ATTR.offset]: 40,
		})
		const { traceId, spanId } = span!.spanContext()
		expect(sending.messages[0]!.headers.traceparent).toBe(`00-${traceId}-${spanId}-01`)
	})

	it('a send is a child of the span that was active when send() was called', async () => {
		const tracer = recorder.tracerProvider.getTracer('app')
		const request = tracer.startSpan('POST /orders')
		await context.with(trace.setSpan(context.active(), request), () => send(1).done)
		request.end()

		const sendSpan = recorder.spans().find(span => span.name === 'send orders')!
		expect(sendSpan.parentSpanContext?.spanId).toBe(request.spanContext().spanId)
		expect(sendSpan.spanContext().traceId).toBe(request.spanContext().traceId)
	})

	it('a failed send is an error span', async () => {
		await expect(
			send(1, async () => {
				throw new TypeError('too large')
			}).done
		).rejects.toThrow('too large')

		const [span] = recorder.spans()
		expect(span!.status.code).toBe(SpanStatusCode.ERROR)
		expect(span!.attributes[ATTR.errorType]).toBe('TypeError')
	})

	it('handling one message continues the trace of the send, and the handler runs inside the span', async () => {
		const { sending, done } = send(1)
		await done
		const sendSpan = recorder.spans()[0]!

		let activeInHandler: string | undefined
		await process([message(40n, sending.messages[0]!.headers)], false, async () => {
			await new Promise(resolve => setTimeout(resolve, 1))
			activeInHandler = trace.getActiveSpan()?.spanContext().spanId
		})

		const processSpan = recorder.spans().find(span => span.name === 'process orders')!
		expect(processSpan.kind).toBe(SpanKind.CONSUMER)
		expect(processSpan.spanContext().traceId).toBe(sendSpan.spanContext().traceId)
		expect(processSpan.parentSpanContext?.spanId).toBe(sendSpan.spanContext().spanId)
		expect(activeInHandler).toBe(processSpan.spanContext().spanId)
		expect(processSpan.attributes).toMatchObject({
			[ATTR.consumerGroup]: 'billing',
			[ATTR.partition]: '1',
			[ATTR.offset]: 40,
			[ATTR.operationName]: 'process',
		})
	})

	it('handling a batch starts a trace and links to the sends of its messages, once each', async () => {
		const first = send(2)
		const second = send(1)
		await Promise.all([first.done, second.done])
		const sendSpanIds = recorder.spans().map(span => span.spanContext().spanId)

		await process(
			[
				message(1n, first.sending.messages[0]!.headers),
				message(2n, first.sending.messages[1]!.headers),
				message(3n, second.sending.messages[0]!.headers),
				message(4n, {}), // produced by something that does not trace
			],
			true,
			async () => {}
		)

		const processSpan = recorder.spans().find(span => span.name === 'process orders')!
		expect(processSpan.parentSpanContext).toBeUndefined()
		expect(processSpan.links.map(link => link.context.spanId).sort()).toEqual([...sendSpanIds].sort())
		expect(processSpan.attributes[ATTR.batchSize]).toBe(4)
	})

	it('a failing handler is an error span, and the failure still reaches the caller', async () => {
		await expect(
			process([message(1n, {})], false, async () => {
				throw new Error('handler failed')
			})
		).rejects.toThrow('handler failed')

		const [span] = recorder.spans()
		expect(span!.status.code).toBe(SpanStatusCode.ERROR)
		expect(span!.events[0]!.name).toBe('exception')
	})

	it('stops when asked to', async () => {
		stop()
		const { sending, done } = send(1)
		await done
		expect(recorder.spans()).toEqual([])
		expect(sending.messages[0]!.headers).toEqual({})
	})
})
