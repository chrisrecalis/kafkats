import { describe, expect, it, afterEach } from 'vitest'
import { codec, TimeWindows, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, sleep, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - stream-stream joins', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('joins two streams within a time window using inner join', async () => {
		const ordersTopic = uniqueName('it-ssj-orders')
		const paymentsTopic = uniqueName('it-ssj-payments')
		const outputTopic = uniqueName('it-ssj-output')

		client = createClient('it-stream-stream-join')
		await client.connect()
		await client.createTopics([
			{ name: ordersTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: paymentsTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Order = { orderId: string; amount: number }
		type Payment = { paymentId: string; method: string }
		type CompletedOrder = { orderId: string; amount: number; paymentId: string; method: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-ssj') })

		const ordersStream = app.stream(ordersTopic, { key: codec.string(), value: codec.json<Order>() })
		const paymentsStream = app.stream(paymentsTopic, { key: codec.string(), value: codec.json<Payment>() })

		ordersStream
			.join(
				paymentsStream,
				(order, payment) => ({
					orderId: order.orderId,
					amount: order.amount,
					paymentId: payment.paymentId,
					method: payment.method,
				}),
				{ within: TimeWindows.of('10s') }
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		await producer.send(ordersTopic, {
			key: Buffer.from('order1'),
			value: Buffer.from(JSON.stringify({ orderId: 'order1', amount: 100 })),
			timestamp: new Date(baseTime),
		})
		await producer.send(paymentsTopic, {
			key: Buffer.from('order1'),
			value: Buffer.from(JSON.stringify({ paymentId: 'pay1', method: 'credit' })),
			timestamp: new Date(baseTime + 1000),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-ssj'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<CompletedOrder>(consumer, outputTopic, { expectedCount: 1 })

		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({
			orderId: 'order1',
			amount: 100,
			paymentId: 'pay1',
			method: 'credit',
		})

		await producer.disconnect()
	})

	it('joins two streams using left join (emits regardless of match)', async () => {
		const clicksTopic = uniqueName('it-sslj-clicks')
		const impressionsTopic = uniqueName('it-sslj-impressions')
		const outputTopic = uniqueName('it-sslj-output')

		client = createClient('it-stream-stream-leftjoin')
		await client.connect()
		await client.createTopics([
			{ name: clicksTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: impressionsTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Click = { elementId: string }
		type Impression = { viewTime: number }
		type ClickWithImpression = { elementId: string; viewTime: number | null }

		app = createFlowApp({ applicationId: uniqueName('it-app-sslj') })

		const clicksStream = app.stream(clicksTopic, { key: codec.string(), value: codec.json<Click>() })
		const impressionsStream = app.stream(impressionsTopic, {
			key: codec.string(),
			value: codec.json<Impression>(),
		})

		clicksStream
			.leftJoin(
				impressionsStream,
				(click, impression) => ({
					elementId: click.elementId,
					viewTime: impression?.viewTime ?? null,
				}),
				{ within: TimeWindows.of('10s') }
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		// Send impression first for ad1 so it's available when click arrives
		await producer.send(impressionsTopic, {
			key: Buffer.from('ad1'),
			value: Buffer.from(JSON.stringify({ viewTime: 5000 })),
			timestamp: new Date(baseTime),
		})
		await producer.send(clicksTopic, {
			key: Buffer.from('ad1'),
			value: Buffer.from(JSON.stringify({ elementId: 'ad1' })),
			timestamp: new Date(baseTime + 500),
		})
		// ad2 has no impression - should join with null
		await producer.send(clicksTopic, {
			key: Buffer.from('ad2'),
			value: Buffer.from(JSON.stringify({ elementId: 'ad2' })),
			timestamp: new Date(baseTime + 1000),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-sslj'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<ClickWithImpression>(consumer, outputTopic, { expectedCount: 2 })

		expect(results).toHaveLength(2)
		expect(results.find(r => r.elementId === 'ad1')).toEqual({ elementId: 'ad1', viewTime: 5000 })
		expect(results.find(r => r.elementId === 'ad2')).toEqual({ elementId: 'ad2', viewTime: null })

		await producer.disconnect()
	})

	it('joins two streams using outer join', async () => {
		const leftTopic = uniqueName('it-ssoj-left')
		const rightTopic = uniqueName('it-ssoj-right')
		const outputTopic = uniqueName('it-ssoj-output')

		client = createClient('it-stream-stream-outerjoin')
		await client.connect()
		await client.createTopics([
			{ name: leftTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: rightTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type LeftEvent = { leftId: string }
		type RightEvent = { rightId: string }
		type Combined = { leftId: string | null; rightId: string | null }

		app = createFlowApp({ applicationId: uniqueName('it-app-ssoj') })

		const leftStream = app.stream(leftTopic, { key: codec.string(), value: codec.json<LeftEvent>() })
		const rightStream = app.stream(rightTopic, { key: codec.string(), value: codec.json<RightEvent>() })

		leftStream
			.outerJoin(
				rightStream,
				(left, right) => ({
					leftId: left?.leftId ?? null,
					rightId: right?.rightId ?? null,
				}),
				{ within: TimeWindows.of('10s') }
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		await producer.send(leftTopic, {
			key: Buffer.from('key1'),
			value: Buffer.from(JSON.stringify({ leftId: 'L1' })),
			timestamp: new Date(baseTime),
		})
		await producer.send(rightTopic, {
			key: Buffer.from('key1'),
			value: Buffer.from(JSON.stringify({ rightId: 'R1' })),
			timestamp: new Date(baseTime + 500),
		})
		await producer.send(leftTopic, {
			key: Buffer.from('key2'),
			value: Buffer.from(JSON.stringify({ leftId: 'L2' })),
			timestamp: new Date(baseTime + 1000),
		})
		await producer.send(rightTopic, {
			key: Buffer.from('key3'),
			value: Buffer.from(JSON.stringify({ rightId: 'R3' })),
			timestamp: new Date(baseTime + 1500),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-ssoj'), autoOffsetReset: 'earliest' })
		const results: Combined[] = []

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { value: Buffer }) => {
				results.push(JSON.parse(msg.value.toString()) as Combined)
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(results.length).toBeGreaterThanOrEqual(3)
		expect(results.some(r => r.leftId === 'L1' && r.rightId === 'R1')).toBe(true)
		expect(results.some(r => r.leftId === 'L2' && r.rightId === null)).toBe(true)
		expect(results.some(r => r.leftId === null && r.rightId === 'R3')).toBe(true)

		await producer.disconnect()
	})
})
