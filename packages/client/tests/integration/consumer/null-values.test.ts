import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - null values', () => {
	it('handles messages with null keys', async () => {
		const client = createClient('it-null-key')
		await client.connect()

		const topicName = uniqueName('it-null-key')
		const testTopic = topic<string, string>(topicName, {
			key: string(),
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message without key (null key)
		await producer.send(testTopic, { value: 'value-without-key' })
		await producer.flush()

		let receivedKey: string | null = 'not-null'
		let receivedValue: string | null = null

		const run = consumer.runEach(
			testTopic,
			async message => {
				receivedKey = message.key
				receivedValue = message.value
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

		expect(receivedKey).toBeNull()
		expect(receivedValue).toBe('value-without-key')

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles empty string values', async () => {
		const client = createClient('it-empty-value')
		await client.connect()

		const topicName = uniqueName('it-empty-value')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message with empty string value
		await producer.send(testTopic, { value: '' })
		await producer.flush()

		let receivedValue: string | null = null

		const run = consumer.runEach(
			testTopic,
			async message => {
				receivedValue = message.value
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

		expect(receivedValue).toBe('')

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles empty buffer values', async () => {
		const client = createClient('it-empty-buffer')
		await client.connect()

		const topicName = uniqueName('it-empty-buffer')

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send message with empty buffer
		await producer.send(topicName, { value: Buffer.alloc(0) })
		await producer.flush()

		let receivedValue: Buffer | null = null

		const run = consumer.runEach(
			topicName,
			async message => {
				receivedValue = message.value
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

		expect(receivedValue).not.toBeNull()
		expect(receivedValue!.length).toBe(0)

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles mixed null and non-null keys in batch', async () => {
		const client = createClient('it-mixed-keys')
		await client.connect()

		const topicName = uniqueName('it-mixed-keys')
		const testTopic = topic<string, string>(topicName, {
			key: string(),
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send messages with and without keys
		await producer.send(testTopic, [
			{ key: 'key-1', value: 'with-key' },
			{ value: 'without-key' },
			{ key: 'key-2', value: 'with-key-2' },
		])
		await producer.flush()

		const received: Array<{ key: string | null; value: string }> = []

		const run = consumer.runEach(
			testTopic,
			async message => {
				received.push({ key: message.key, value: message.value })
				if (received.length >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await run

		expect(received).toHaveLength(3)
		expect(received[0]).toEqual({ key: 'key-1', value: 'with-key' })
		expect(received[1]).toEqual({ key: null, value: 'without-key' })
		expect(received[2]).toEqual({ key: 'key-2', value: 'with-key-2' })

		await producer.disconnect()
		await client.disconnect()
	})
})
