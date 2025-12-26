import { describe, expect, it, afterEach } from 'vitest'
import { codec, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, waitForAppReady, waitFor, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - basic streaming', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('processes map/filter/to pipeline end-to-end', async () => {
		const inputTopic = uniqueName('it-input')
		const outputTopic = uniqueName('it-output')

		client = createClient('it-basic-streaming')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Order = { id: string; total: number }

		app = createFlowApp({ applicationId: uniqueName('it-app') })
		app.stream(inputTopic, { value: codec.json<Order>() })
			.filter((_, v) => v.total > 100)
			.mapValues(v => ({ ...v, large: true }))
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'o1', total: 250 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'o2', total: 50 })) })
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-verify'), autoOffsetReset: 'earliest' })
		const received = await consumeWithTimeout(consumer, outputTopic, { expectedCount: 1 })

		expect(received).toHaveLength(1)
		expect(received[0]).toEqual({ id: 'o1', total: 250, large: true })

		await producer.disconnect()
	})

	it('transforms values with mapValues', async () => {
		const inputTopic = uniqueName('it-mapvalues-in')
		const outputTopic = uniqueName('it-mapvalues-out')

		client = createClient('it-mapvalues')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { type: string; data: string }
		type Enriched = Event & { processed: boolean }

		app = createFlowApp({ applicationId: uniqueName('it-app-mapvalues') })
		app.stream(inputTopic, { value: codec.json<Event>() })
			.mapValues(v => ({ ...v, processed: true }) as Enriched)
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ type: 'click', data: 'btn1' })) })
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-verify-mapvalues'), autoOffsetReset: 'earliest' })
		const received = await consumeWithTimeout<Enriched>(consumer, outputTopic, { expectedCount: 1 })

		expect(received[0]).toEqual({ type: 'click', data: 'btn1', processed: true })

		await producer.disconnect()
	})

	it('re-keys messages with selectKey', async () => {
		const inputTopic = uniqueName('it-selectkey-in')
		const outputTopic = uniqueName('it-selectkey-out')

		client = createClient('it-selectkey')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type User = { userId: string; name: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-selectkey') })
		app.stream(inputTopic, { value: codec.json<User>() })
			.selectKey(v => v.userId)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ userId: 'user123', name: 'Alice' })) })
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-verify-selectkey'), autoOffsetReset: 'earliest' })
		let receivedKey: string | null = null
		let receivedValue: User | null = null

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				receivedKey = msg.key?.toString() ?? null
				receivedValue = JSON.parse(msg.value.toString()) as User
				consumer.stop()
			},
			{ autoCommit: false }
		)

		await waitFor(
			() => {
				if (receivedValue === null) {
					throw new Error('No message received yet')
				}
			},
			{ timeout: 10000, interval: 100 }
		)
		await consumePromise.catch(() => {})

		expect(receivedKey).toBe('user123')
		expect(receivedValue).toEqual({ userId: 'user123', name: 'Alice' })

		await producer.disconnect()
	})

	it('creates intermediate topic with through', async () => {
		const inputTopic = uniqueName('it-through-in')
		const auditTopic = uniqueName('it-through-audit')
		const outputTopic = uniqueName('it-through-out')

		client = createClient('it-through')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: auditTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { id: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-through') })
		app.stream(inputTopic, { value: codec.json<Event>() })
			.through(auditTopic, { value: codec.json() })
			.mapValues(v => ({ ...v, audited: true }))
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e1' })) })
		await producer.flush()

		// Check audit topic
		const auditConsumer = client.consumer({ groupId: uniqueName('it-verify-audit'), autoOffsetReset: 'earliest' })
		const auditReceived = await consumeWithTimeout<Event>(auditConsumer, auditTopic, { expectedCount: 1 })

		expect(auditReceived[0]).toEqual({ id: 'e1' })

		// Check output topic
		const outputConsumer = client.consumer({ groupId: uniqueName('it-verify-output'), autoOffsetReset: 'earliest' })
		const outputReceived = await consumeWithTimeout(outputConsumer, outputTopic, { expectedCount: 1 })

		expect(outputReceived[0]).toEqual({ id: 'e1', audited: true })

		await producer.disconnect()
	})

	it('executes side effects with peek', async () => {
		const inputTopic = uniqueName('it-peek-in')
		const outputTopic = uniqueName('it-peek-out')

		client = createClient('it-peek')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Item = { name: string }
		const peekedValues: Item[] = []

		app = createFlowApp({ applicationId: uniqueName('it-app-peek') })
		app.stream(inputTopic, { value: codec.json<Item>() })
			.peek((_, v) => {
				peekedValues.push(v)
			})
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ name: 'item1' })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ name: 'item2' })) })
		await producer.flush()

		await waitFor(
			() => {
				if (peekedValues.length < 2) {
					throw new Error(`Only ${peekedValues.length}/2 messages peeked`)
				}
			},
			{ timeout: 10000, interval: 100 }
		)

		expect(peekedValues).toHaveLength(2)
		expect(peekedValues).toContainEqual({ name: 'item1' })
		expect(peekedValues).toContainEqual({ name: 'item2' })

		await producer.disconnect()
	})
})
