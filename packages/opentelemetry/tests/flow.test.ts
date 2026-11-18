import {
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	TraceFlags,
	propagation,
	trace,
	type SpanContext,
} from '@opentelemetry/api'
import { TestDriver } from '@kafkats/flow/testing'
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { ATTR, instrumentKafka } from '../src/index.js'
import { spanRecorder, useW3CContext } from './setup.js'

/** The headers a record sent inside `origin` carries (W3C `traceparent`), as a consumer reads them. */
function sentFrom(origin: SpanContext): Record<string, Buffer> {
	const headers: Record<string, string> = {}
	propagation.inject(trace.setSpanContext(ROOT_CONTEXT, origin), headers)
	return Object.fromEntries(Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)]))
}

describe('tracing a flow application', () => {
	let recorder: ReturnType<typeof spanRecorder>
	let stop: () => void

	beforeAll(useW3CContext)
	beforeEach(() => {
		recorder = spanRecorder()
		stop = instrumentKafka({ tracerProvider: recorder.tracerProvider, metrics: false })
	})
	afterEach(() => stop())

	/** orders -> peek (records the span active in it, creates one of its own) -> billed */
	function billing() {
		const driver = new TestDriver({ applicationId: 'billing' })
		const tracer = recorder.tracerProvider.getTracer('app')
		const active: Array<SpanContext | undefined> = []
		driver
			.input('orders')
			.peek(() => {
				active.push(trace.getActiveSpan()?.spanContext())
				tracer.startSpan('charge card').end()
			})
			.to('billed')
		return { driver, tracer, active }
	}

	it('a record a task processes is a consumer span that continues the trace of its send; the processors run with it active', async () => {
		const { driver, tracer, active } = billing()
		const upstream = tracer.startSpan('send orders')
		upstream.end()

		await driver.run(async ({ send }) => {
			await send('orders', 'order-1', { headers: sentFrom(upstream.spanContext()) })
		})

		const byName = (name: string) => recorder.spans().find(span => span.name === name)!
		const processing = byName('process orders')
		const charge = byName('charge card')
		expect(processing.kind).toBe(SpanKind.CONSUMER)
		expect(processing.spanContext().traceId).toBe(upstream.spanContext().traceId)
		expect(processing.parentSpanContext?.spanId).toBe(upstream.spanContext().spanId)
		expect(processing.attributes).toEqual({
			[ATTR.system]: 'kafka',
			[ATTR.topic]: 'orders',
			[ATTR.partition]: '0',
			[ATTR.operationName]: 'process',
			[ATTR.operationType]: 'process',
			[ATTR.consumerGroup]: 'billing',
			[ATTR.offset]: 0,
			[ATTR.taskId]: '0_0',
		})
		expect(processing.status.code).toBe(SpanStatusCode.UNSET)
		expect(active).toEqual([processing.spanContext()])
		expect(charge.parentSpanContext?.spanId).toBe(processing.spanContext().spanId)
		// Ended once the record's processing settled: after the span created inside it.
		expect(recorder.spans().map(span => span.name)).toEqual(['send orders', 'charge card', 'process orders'])
	})

	it('a record without trace context starts a trace', async () => {
		const { driver } = billing()

		await driver.run(async ({ send }) => {
			await send('orders', 'order-1')
		})

		const processing = recorder.spans().find(span => span.name === 'process orders')!
		expect(processing.parentSpanContext).toBeUndefined()
	})

	it('a record of a trace that is not sampled gets no span, but is processed in that trace', async () => {
		const { driver, active } = billing()
		const unsampled: SpanContext = {
			traceId: '0af7651916cd43dd8448eb211c80319c',
			spanId: 'b7ad6b7169203331',
			traceFlags: TraceFlags.NONE,
		}

		await driver.run(async ({ send }) => {
			await send('orders', 'order-1', { headers: sentFrom(unsampled) })
		})

		expect(recorder.spans()).toEqual([])
		expect(active).toMatchObject([{ traceId: unsampled.traceId, spanId: unsampled.spanId }])
	})

	it('a record whose processing fails ends its span with the error', async () => {
		const driver = new TestDriver({ applicationId: 'billing' })
		driver.input('orders').foreach(() => {
			throw new TypeError('card declined')
		})

		await expect(
			driver.run(async ({ send }) => {
				await send('orders', 'order-1')
			})
		).rejects.toThrow()

		const [processing] = recorder.spans()
		expect(processing!.name).toBe('process orders')
		expect(processing!.status.code).toBe(SpanStatusCode.ERROR)
		expect(processing!.attributes[ATTR.errorType]).toBe('FailedProcessingError')
		expect(processing!.events.map(event => event.name)).toEqual(['exception'])
	})

	it('stops with the rest of the instrumentation', async () => {
		stop()
		const { driver, active } = billing()

		await driver.run(async ({ send }) => {
			await send('orders', 'order-1')
		})

		expect(active).toEqual([undefined])
		expect(recorder.spans().map(span => span.name)).toEqual(['charge card'])
	})
})
