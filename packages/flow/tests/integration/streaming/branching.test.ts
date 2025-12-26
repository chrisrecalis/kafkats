import { describe, expect, it, afterEach } from 'vitest'
import { codec, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, waitForAppReady, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - branching', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('routes messages to correct topics based on predicates', async () => {
		const inputTopic = uniqueName('it-branch-in')
		const highPriorityTopic = uniqueName('it-branch-high')
		const lowPriorityTopic = uniqueName('it-branch-low')

		client = createClient('it-branching')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: highPriorityTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: lowPriorityTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Task = { id: string; priority: 'high' | 'low' }

		app = createFlowApp({ applicationId: uniqueName('it-app-branch') })

		const stream = app.stream(inputTopic, { value: codec.json<Task>() })
		const [highPriority, lowPriority] = stream.branch(
			(_, v) => v.priority === 'high',
			(_, v) => v.priority === 'low'
		)

		highPriority!.to(highPriorityTopic, { value: codec.json() })
		lowPriority!.to(lowPriorityTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 't1', priority: 'high' })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 't2', priority: 'low' })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 't3', priority: 'high' })) })
		await producer.flush()

		const highConsumer = client.consumer({ groupId: uniqueName('it-verify-high'), autoOffsetReset: 'earliest' })
		const highReceived = await consumeWithTimeout<Task>(highConsumer, highPriorityTopic, { expectedCount: 2 })

		const lowConsumer = client.consumer({ groupId: uniqueName('it-verify-low'), autoOffsetReset: 'earliest' })
		const lowReceived = await consumeWithTimeout<Task>(lowConsumer, lowPriorityTopic, { expectedCount: 1 })

		expect(highReceived).toHaveLength(2)
		expect(highReceived.map(t => t.id).sort()).toEqual(['t1', 't3'])

		expect(lowReceived).toHaveLength(1)
		expect(lowReceived[0]!.id).toBe('t2')

		await producer.disconnect()
	})

	it('supports multiple output destinations from same stream', async () => {
		const inputTopic = uniqueName('it-multi-in')
		const allTopic = uniqueName('it-multi-all')
		const filteredTopic = uniqueName('it-multi-filtered')

		client = createClient('it-multi-output')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: allTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: filteredTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { type: string; value: number }

		app = createFlowApp({ applicationId: uniqueName('it-app-multi') })

		const stream = app.stream(inputTopic, { value: codec.json<Event>() })
		stream.to(allTopic, { value: codec.json() })
		stream.filter((_, v) => v.value > 50).to(filteredTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ type: 'a', value: 100 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ type: 'b', value: 25 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ type: 'c', value: 75 })) })
		await producer.flush()

		const allConsumer = client.consumer({ groupId: uniqueName('it-verify-all'), autoOffsetReset: 'earliest' })
		const allReceived = await consumeWithTimeout<Event>(allConsumer, allTopic, { expectedCount: 3 })

		const filteredConsumer = client.consumer({
			groupId: uniqueName('it-verify-filtered'),
			autoOffsetReset: 'earliest',
		})
		const filteredReceived = await consumeWithTimeout<Event>(filteredConsumer, filteredTopic, { expectedCount: 2 })

		expect(allReceived).toHaveLength(3)
		expect(filteredReceived).toHaveLength(2)
		expect(filteredReceived.every(e => e.value > 50)).toBe(true)

		await producer.disconnect()
	})
})
