import { context, trace } from '@opentelemetry/api'
import { KafkaClient } from '@kafkats/client'
import { afterEach, beforeAll, describe, expect, it } from 'vitest'

import { KAFKATS_METRIC, METRIC, instrumentKafka } from '../src/index.js'
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
})
