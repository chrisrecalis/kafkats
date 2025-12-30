import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - ordering', () => {
	it('consumes messages in order within a partition', async () => {
		const client = createClient('it-ordering')
		await client.connect()

		const topicName = uniqueName('it-ordering')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send 100 numbered messages
		const messages = Array.from({ length: 100 }, (_, i) => ({ value: `msg-${String(i).padStart(3, '0')}` }))
		await producer.send(testTopic, messages)
		await producer.flush()

		const received: string[] = []

		const run = consumer.runEach(
			testTopic,
			async message => {
				received.push(message.value)
				if (received.length >= 100) {
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

		// Verify messages are in order
		expect(received).toHaveLength(100)
		for (let i = 0; i < 100; i++) {
			expect(received[i]).toBe(`msg-${String(i).padStart(3, '0')}`)
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('maintains order per partition when consuming multiple partitions', async () => {
		const client = createClient('it-multi-partition-order')
		await client.connect()

		const topicName = uniqueName('it-multi-partition-order')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send 10 messages to each partition with explicit partition assignment
		for (let partition = 0; partition < 3; partition++) {
			for (let i = 0; i < 10; i++) {
				await producer.send(testTopic, { value: `p${partition}-${i}`, partition })
			}
		}
		await producer.flush()

		const receivedByPartition = new Map<number, string[]>()

		const run = consumer.runEach(
			testTopic,
			async message => {
				const partition = message.partition
				if (!receivedByPartition.has(partition)) {
					receivedByPartition.set(partition, [])
				}
				receivedByPartition.get(partition)!.push(message.value)

				// Count total messages received
				let total = 0
				for (const msgs of receivedByPartition.values()) {
					total += msgs.length
				}
				if (total >= 30) {
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

		// Verify ordering within each partition
		for (let partition = 0; partition < 3; partition++) {
			const partitionMessages = receivedByPartition.get(partition) || []
			expect(partitionMessages).toHaveLength(10)
			for (let i = 0; i < 10; i++) {
				expect(partitionMessages[i]).toBe(`p${partition}-${i}`)
			}
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('preserves offset ordering', async () => {
		const client = createClient('it-offset-order')
		await client.connect()

		const topicName = uniqueName('it-offset-order')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		// Send messages
		await producer.send(testTopic, [{ value: 'a' }, { value: 'b' }, { value: 'c' }])
		await producer.flush()

		const offsets: bigint[] = []

		const run = consumer.runEach(
			testTopic,
			async message => {
				offsets.push(message.offset)
				if (offsets.length >= 3) {
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

		// Offsets should be sequential
		expect(offsets).toHaveLength(3)
		expect(offsets[1]).toBe(offsets[0]! + 1n)
		expect(offsets[2]).toBe(offsets[1]! + 1n)

		await producer.disconnect()
		await client.disconnect()
	})
})
