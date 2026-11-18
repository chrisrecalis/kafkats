import { context, trace } from '@opentelemetry/api'
import { KafkaClient } from '@kafkats/client'
import { flow } from '@kafkats/flow'
import { afterEach, beforeAll, describe, expect, it } from 'vitest'

import { ATTR, KAFKATS_METRIC, METRIC, instrumentKafka } from '../src/index.js'
import { metricRecorder, spanRecorder, useW3CContext } from './setup.js'

const brokers = process.env.KAFKA_BROKERS?.split(',').map(broker => broker.trim())
const unique = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`

// Needs a broker: KAFKA_BROKERS=127.0.0.1:9092 pnpm test
describe.skipIf(!brokers)('a real producer and consumer', () => {
	let stop: (() => void) | undefined
	beforeAll(useW3CContext)
	afterEach(() => stop?.())

	it('one trace from the request that sends a message to the handler that processes it', async () => {
		const spans = spanRecorder()
		const metrics = metricRecorder()
		stop = instrumentKafka({ tracerProvider: spans.tracerProvider, meterProvider: metrics.meterProvider })

		const client = new KafkaClient({ brokers: brokers!, clientId: 'otel-e2e', logLevel: 'silent' })
		await client.connect()
		const topic = unique('otel-e2e')
		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])
		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: unique('otel-e2e-group'), autoOffsetReset: 'earliest' })
		try {
			const tracer = spans.tracerProvider.getTracer('app')
			const request = tracer.startSpan('POST /orders')
			await context.with(trace.setSpan(context.active(), request), () =>
				producer.send(topic, { key: 'order-1', value: Buffer.from('created') })
			)
			request.end()

			let traceparent: string | undefined
			await consumer.runEach(topic, async message => {
				traceparent = message.headers.traceparent?.toString()
				// Work the handler does belongs to the processing span.
				tracer.startSpan('charge card').end()
				consumer.stop()
			})

			const byName = (name: string) => spans.spans().find(span => span.name === name)!
			const send = byName(`send ${topic}`)
			const processing = byName(`process ${topic}`)
			const charge = byName('charge card')
			const traceId = request.spanContext().traceId
			expect([send, processing, charge].map(span => span.spanContext().traceId)).toEqual([
				traceId,
				traceId,
				traceId,
			])
			expect(send.parentSpanContext?.spanId).toBe(request.spanContext().spanId)
			expect(processing.parentSpanContext?.spanId).toBe(send.spanContext().spanId)
			expect(charge.parentSpanContext?.spanId).toBe(processing.spanContext().spanId)
			expect(traceparent).toBe(`00-${traceId}-${send.spanContext().spanId}-01`)

			const collected = await metrics.collect()
			expect(collected.get(METRIC.sentMessages)![0]!.value).toBe(1)
			expect(collected.get(METRIC.consumedMessages)![0]!.value).toBe(1)
			expect(collected.has(METRIC.processDuration)).toBe(true)
			expect(collected.has(KAFKATS_METRIC.requestRecords)).toBe(true)
			// The producer and consumer created after instrumentKafka() are observed.
			expect(collected.get(KAFKATS_METRIC.unacknowledgedSends)![0]!.value).toBe(0)
			expect(collected.has(KAFKATS_METRIC.assignedPartitions)).toBe(true)
		} finally {
			await producer.disconnect()
			await client.disconnect()
		}
	})

	it('one trace from the request through a flow application to the handler that consumes its output', async () => {
		const spans = spanRecorder()
		stop = instrumentKafka({ tracerProvider: spans.tracerProvider, metrics: false })

		const client = new KafkaClient({ brokers: brokers!, clientId: 'otel-e2e-flow', logLevel: 'silent' })
		await client.connect()
		const input = unique('otel-e2e-flow-in')
		const output = unique('otel-e2e-flow-out')
		await client.createTopics([
			{ name: input, numPartitions: 1, replicationFactor: 1 },
			{ name: output, numPartitions: 1, replicationFactor: 1 },
		])
		const app = flow({
			applicationId: unique('otel-e2e-flow-app'),
			client,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		})
		app.stream(input)
			.mapValues(value => Buffer.from(`${value?.toString()} and billed`))
			.to(output)
		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: unique('otel-e2e-flow-group'), autoOffsetReset: 'earliest' })
		const request = spans.tracerProvider.getTracer('app').startSpan('POST /orders')
		let traceparent: string | undefined
		try {
			await app.start()
			await context.with(trace.setSpan(context.active(), request), () =>
				producer.send(input, { key: 'order-1', value: Buffer.from('created') })
			)
			request.end()
			await consumer.runEach(output, async message => {
				traceparent = message.headers.traceparent?.toString()
				consumer.stop()
			})
		} finally {
			// Every send of the application is acknowledged: their spans ended.
			await app.close()
			await producer.disconnect()
			await client.disconnect()
		}

		const named = (name: string) => spans.spans().filter(span => span.name === name)
		const [sendInput] = named(`send ${input}`)
		// The flow task's span, not the one of the handler that hands the fetched batches to the tasks.
		const [processing] = named(`process ${input}`).filter(span => span.attributes[ATTR.taskId] === '0_0')
		const [sendOutput] = named(`send ${output}`)
		const [handler] = named(`process ${output}`)
		const traceId = request.spanContext().traceId
		expect([sendInput, processing, sendOutput, handler].map(span => span!.spanContext().traceId)).toEqual([
			traceId,
			traceId,
			traceId,
			traceId,
		])
		expect(sendInput!.parentSpanContext?.spanId).toBe(request.spanContext().spanId)
		expect(processing!.parentSpanContext?.spanId).toBe(sendInput!.spanContext().spanId)
		expect(sendOutput!.parentSpanContext?.spanId).toBe(processing!.spanContext().spanId)
		expect(handler!.parentSpanContext?.spanId).toBe(sendOutput!.spanContext().spanId)
		expect(traceparent).toBe(`00-${traceId}-${sendOutput!.spanContext().spanId}-01`)
	})
})
