import { describe, expect, it, afterEach } from 'vitest'
import { codec, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, sleep } from '../helpers/testkit.js'

describe('Flow (integration) - aggregations', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('counts messages by key with groupByKey().count()', async () => {
		const inputTopic = uniqueName('it-count-in')
		const outputTopic = uniqueName('it-count-out')

		client = createClient('it-count')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Click = { userId: string; page: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-count') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() })
			.groupByKey()
			.count()
			.toStream()
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ userId: 'user1', page: '/home' })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ userId: 'user1', page: '/about' })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user2'),
			value: Buffer.from(JSON.stringify({ userId: 'user2', page: '/home' })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ userId: 'user1', page: '/contact' })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-count'), autoOffsetReset: 'earliest' })
		const counts = new Map<string, number>()

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const key = msg.key?.toString() ?? ''
				const count = JSON.parse(msg.value.toString()) as number
				counts.set(key, count)
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(counts.get('user1')).toBe(3)
		expect(counts.get('user2')).toBe(1)

		await producer.disconnect()
	})

	it('reduces values by key with groupByKey().reduce()', async () => {
		const inputTopic = uniqueName('it-reduce-in')
		const outputTopic = uniqueName('it-reduce-out')

		client = createClient('it-reduce')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Amount = { amount: number }

		app = createFlowApp({ applicationId: uniqueName('it-app-reduce') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Amount>() })
			.groupByKey()
			.reduce((agg, value) => ({ amount: agg.amount + value.amount }))
			.toStream()
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, {
			key: Buffer.from('account1'),
			value: Buffer.from(JSON.stringify({ amount: 100 })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('account1'),
			value: Buffer.from(JSON.stringify({ amount: 50 })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('account2'),
			value: Buffer.from(JSON.stringify({ amount: 200 })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('account1'),
			value: Buffer.from(JSON.stringify({ amount: 25 })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-reduce'), autoOffsetReset: 'earliest' })
		const totals = new Map<string, number>()

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const key = msg.key?.toString() ?? ''
				const value = JSON.parse(msg.value.toString()) as Amount
				totals.set(key, value.amount)
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(totals.get('account1')).toBe(175)
		expect(totals.get('account2')).toBe(200)

		await producer.disconnect()
	})

	it('aggregates with custom aggregator using groupByKey().aggregate()', async () => {
		const inputTopic = uniqueName('it-agg-in')
		const outputTopic = uniqueName('it-agg-out')

		client = createClient('it-aggregate')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Order = { product: string; quantity: number }
		type Stats = { count: number; totalQuantity: number }

		app = createFlowApp({ applicationId: uniqueName('it-app-agg') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Order>() })
			.groupByKey()
			.aggregate<Stats>(
				() => ({ count: 0, totalQuantity: 0 }),
				(_key, value, agg) => ({
					count: agg.count + 1,
					totalQuantity: agg.totalQuantity + value.quantity,
				}),
				{ value: codec.json<Stats>() }
			)
			.toStream()
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, {
			key: Buffer.from('customer1'),
			value: Buffer.from(JSON.stringify({ product: 'A', quantity: 5 })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('customer1'),
			value: Buffer.from(JSON.stringify({ product: 'B', quantity: 3 })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('customer2'),
			value: Buffer.from(JSON.stringify({ product: 'A', quantity: 10 })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-agg'), autoOffsetReset: 'earliest' })
		const stats = new Map<string, Stats>()

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const key = msg.key?.toString() ?? ''
				const value = JSON.parse(msg.value.toString()) as Stats
				stats.set(key, value)
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(stats.get('customer1')).toEqual({ count: 2, totalQuantity: 8 })
		expect(stats.get('customer2')).toEqual({ count: 1, totalQuantity: 10 })

		await producer.disconnect()
	})

	it('groups by custom key with groupBy()', async () => {
		const inputTopic = uniqueName('it-groupby-in')
		const outputTopic = uniqueName('it-groupby-out')

		client = createClient('it-groupby')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { category: string; name: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-groupby') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Event>() })
			.groupBy((_key, value) => value.category, { key: codec.string() })
			.count()
			.toStream()
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, {
			key: Buffer.from('e1'),
			value: Buffer.from(JSON.stringify({ category: 'sports', name: 'Goal scored' })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('e2'),
			value: Buffer.from(JSON.stringify({ category: 'music', name: 'Concert' })),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('e3'),
			value: Buffer.from(JSON.stringify({ category: 'sports', name: 'Match ended' })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-groupby'), autoOffsetReset: 'earliest' })
		const counts = new Map<string, number>()

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const key = msg.key?.toString() ?? ''
				const count = JSON.parse(msg.value.toString()) as number
				counts.set(key, count)
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(counts.get('sports')).toBe(2)
		expect(counts.get('music')).toBe(1)

		await producer.disconnect()
	})
})
