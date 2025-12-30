import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - timestamps', () => {
	it('uses current time as default timestamp', async () => {
		const client = createClient('it-timestamp-default')
		await client.connect()

		const topicName = uniqueName('it-timestamp-default')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({
			groupId: uniqueName('it-timestamp-default-group'),
			autoOffsetReset: 'earliest',
		})

		const beforeSend = Date.now()
		await producer.send(testTopic, { value: 'timestamp-test' })
		await producer.flush()
		const afterSend = Date.now()

		let receivedTimestamp: bigint | null = null
		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				receivedTimestamp = message.timestamp
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await runPromise

		expect(receivedTimestamp).not.toBeNull()
		const timestampMs = Number(receivedTimestamp!)

		// Timestamp should be within the window of when we sent the message
		expect(timestampMs).toBeGreaterThanOrEqual(beforeSend - 1000)
		expect(timestampMs).toBeLessThanOrEqual(afterSend + 1000)

		await producer.disconnect()
		await client.disconnect()
	})

	it('preserves custom timestamps when provided', async () => {
		const client = createClient('it-timestamp-custom')
		await client.connect()

		const topicName = uniqueName('it-timestamp-custom')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({
			groupId: uniqueName('it-timestamp-custom-group'),
			autoOffsetReset: 'earliest',
		})

		// Use a specific custom timestamp
		const customDate = new Date('2024-01-15T10:30:00.000Z')
		const customTimestampMs = customDate.getTime()

		await producer.send(testTopic, { value: 'custom-timestamp-test', timestamp: customDate })
		await producer.flush()

		let receivedTimestamp: bigint | null = null
		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				receivedTimestamp = message.timestamp
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await runPromise

		expect(receivedTimestamp).not.toBeNull()
		expect(Number(receivedTimestamp!)).toBe(customTimestampMs)

		await producer.disconnect()
		await client.disconnect()
	})
})
