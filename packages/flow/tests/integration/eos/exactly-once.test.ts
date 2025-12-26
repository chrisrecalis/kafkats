import { describe, expect, it, afterEach } from 'vitest'
import { codec, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, waitForAppReady, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - exactly-once semantics', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('produces messages transactionally with processingGuarantee: exactly_once', async () => {
		const inputTopic = uniqueName('it-eos-in')
		const outputTopic = uniqueName('it-eos-out')

		client = createClient('it-eos')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { id: string; value: number }

		app = createFlowApp({
			applicationId: uniqueName('it-app-eos'),
			processingGuarantee: 'exactly_once',
		})

		app.stream(inputTopic, { value: codec.json<Event>() })
			.mapValues(v => ({ ...v, processed: true }))
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e1', value: 100 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e2', value: 200 })) })
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-verify-eos'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
		})
		const results = await consumeWithTimeout<{ id: string; value: number; processed: boolean }>(
			consumer,
			outputTopic,
			{ expectedCount: 2 }
		)

		expect(results).toHaveLength(2)
		expect(results.find(r => r.id === 'e1')).toEqual({ id: 'e1', value: 100, processed: true })
		expect(results.find(r => r.id === 'e2')).toEqual({ id: 'e2', value: 200, processed: true })

		await producer.disconnect()
	})

	it('commits consumer offsets with transaction', async () => {
		const inputTopic = uniqueName('it-eos-commit-in')
		const outputTopic = uniqueName('it-eos-commit-out')

		client = createClient('it-eos-commit')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Message = { seq: number }

		app = createFlowApp({
			applicationId: uniqueName('it-app-eos-commit'),
			processingGuarantee: 'exactly_once',
		})

		app.stream(inputTopic, { value: codec.json<Message>() })
			.mapValues(v => ({ ...v, doubled: v.seq * 2 }))
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ seq: 1 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ seq: 2 })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ seq: 3 })) })
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-verify-eos-commit'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
		})
		const results = await consumeWithTimeout<{ seq: number; doubled: number }>(consumer, outputTopic, {
			expectedCount: 3,
		})

		expect(results).toHaveLength(3)
		expect(results.map(r => r.doubled).sort((a, b) => a - b)).toEqual([2, 4, 6])

		await producer.disconnect()
	})

	it('works with at_least_once processing guarantee (default)', async () => {
		const inputTopic = uniqueName('it-alo-in')
		const outputTopic = uniqueName('it-alo-out')

		client = createClient('it-alo')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Data = { name: string }

		app = createFlowApp({
			applicationId: uniqueName('it-app-alo'),
			processingGuarantee: 'at_least_once',
		})

		app.stream(inputTopic, { value: codec.json<Data>() })
			.mapValues(v => ({ ...v, handled: true }))
			.to(outputTopic, { value: codec.json() })

		await app.start()
		await waitForAppReady(app)

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ name: 'test1' })) })
		await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ name: 'test2' })) })
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-verify-alo'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<{ name: string; handled: boolean }>(consumer, outputTopic, {
			expectedCount: 2,
		})

		expect(results).toHaveLength(2)
		expect(results.every(r => r.handled)).toBe(true)

		await producer.disconnect()
	})
})
