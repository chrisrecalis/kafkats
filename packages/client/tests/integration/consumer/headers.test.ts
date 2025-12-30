import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - headers', () => {
	it('receives message headers as Buffer values', async () => {
		const client = createClient('it-headers')
		await client.connect()

		const topicName = uniqueName('it-headers')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message with headers
		await producer.send(testTopic, {
			value: 'hello',
			headers: {
				'x-correlation-id': 'abc-123',
				'x-source': Buffer.from('test-producer'),
			},
		})
		await producer.flush()

		let receivedHeaders: Record<string, Buffer> | null = null

		const run = consumer.runEach(
			testTopic,
			async message => {
				receivedHeaders = message.headers
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await run

		expect(receivedHeaders).not.toBeNull()
		expect(receivedHeaders!['x-correlation-id']?.toString()).toBe('abc-123')
		expect(receivedHeaders!['x-source']?.toString()).toBe('test-producer')

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles messages with empty headers', async () => {
		const client = createClient('it-empty-headers')
		await client.connect()

		const topicName = uniqueName('it-empty-headers')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message without headers
		await producer.send(testTopic, { value: 'no-headers' })
		await producer.flush()

		let receivedHeaders: Record<string, Buffer> | null = null

		const run = consumer.runEach(
			testTopic,
			async message => {
				receivedHeaders = message.headers
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await run

		expect(receivedHeaders).not.toBeNull()
		expect(Object.keys(receivedHeaders!)).toHaveLength(0)

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles multiple headers with same key (last value wins)', async () => {
		const client = createClient('it-multi-headers')
		await client.connect()

		const topicName = uniqueName('it-multi-headers')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message with headers (Record only allows one value per key)
		await producer.send(testTopic, {
			value: 'test',
			headers: {
				'x-trace-id': 'trace-001',
				'x-span-id': 'span-002',
			},
		})
		await producer.flush()

		let receivedHeaders: Record<string, Buffer> | null = null

		const run = consumer.runEach(
			testTopic,
			async message => {
				receivedHeaders = message.headers
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await run

		expect(receivedHeaders!['x-trace-id']?.toString()).toBe('trace-001')
		expect(receivedHeaders!['x-span-id']?.toString()).toBe('span-002')

		await producer.disconnect()
		await client.disconnect()
	})
})
