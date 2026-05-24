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

	it('round-trips records whose timestampDelta exceeds 32 bits within one batch', async () => {
		const client = createClient('it-timestamp-large-delta')
		await client.connect()

		const topicName = uniqueName('it-timestamp-large-delta')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		// Both records go in ONE batch (same partition, single send), so the second
		// record's timestampDelta is measured from the first's timestamp. 30 days in ms
		// (2_592_000_000) exceeds the 32-bit zigzag range (2^31), which the RecordBatch v2
		// spec encodes as a VARLONG. Encoding/decoding it as a 32-bit varint throws or
		// truncates.
		const baseMs = Date.parse('2025-01-01T00:00:00.000Z')
		const laterMs = baseMs + 30 * 24 * 60 * 60 * 1000
		expect(laterMs - baseMs).toBeGreaterThan(0x7fffffff)

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({
			groupId: uniqueName('it-timestamp-large-delta-group'),
			autoOffsetReset: 'earliest',
		})

		await producer.send(testTopic, [
			{ value: 'base', timestamp: new Date(baseMs), partition: 0 },
			{ value: 'later', timestamp: new Date(laterMs), partition: 0 },
		])
		await producer.flush()

		const received: Array<{ value: string; timestampMs: number }> = []
		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				received.push({ value: message.value, timestampMs: Number(message.timestamp) })
				if (received.length >= 2) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await runPromise

		const byValue = new Map(received.map(r => [r.value, r.timestampMs]))
		expect(byValue.get('base')).toBe(baseMs)
		expect(byValue.get('later')).toBe(laterMs)

		await producer.disconnect()
		await client.disconnect()
	})
})
