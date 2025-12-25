import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - batching', () => {
	it('batches multiple messages in a single send call', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-batch-multi')
			await client.connect()

			const topicName = uniqueName('it-batch-multi')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			const consumer = client.consumer({
				groupId: uniqueName('it-batch-multi-group'),
				autoOffsetReset: 'earliest',
			})

			// Send multiple messages in a single call - they should be batched together
			const messages = Array.from({ length: 100 }, (_, i) => ({ value: `msg-${i}` }))
			const results = await producer.send(testTopic, messages)
			await producer.flush()

			// All messages should have sequential offsets in the same partition
			expect(results).toHaveLength(100)
			for (let i = 0; i < 100; i++) {
				expect(results[i]!.partition).toBe(results[0]!.partition)
				expect(results[i]!.offset).toBe(results[0]!.offset + BigInt(i))
			}

			// Verify all messages are consumable
			const received: string[] = []
			consumer.subscribe(testTopic)
			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 100) {
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

			expect(received).toHaveLength(100)
			for (let i = 0; i < 100; i++) {
				expect(received).toContain(`msg-${i}`)
			}

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('flushes pending messages on producer.flush()', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-flush')
			await client.connect()

			const topicName = uniqueName('it-flush')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Producer with very long linger time - messages won't auto-send
			const producer = client.producer({ lingerMs: 60000 })
			const consumer = client.consumer({ groupId: uniqueName('it-flush-group'), autoOffsetReset: 'earliest' })

			// Send message (won't complete due to long linger)
			const sendPromise = producer.send(testTopic, { value: 'flush-test-message' })

			// Immediately flush - should send the message right away
			const beforeFlush = Date.now()
			await producer.flush()
			const afterFlush = Date.now()

			// Flush should complete quickly (not wait for full lingerMs)
			expect(afterFlush - beforeFlush).toBeLessThan(5000)

			// Wait for send to complete
			const result = await sendPromise
			expect(result.offset).toBeGreaterThanOrEqual(0n)

			// Verify message is consumable
			let received: string | null = null
			consumer.subscribe(testTopic)
			const runPromise = consumer.runEach(
				async message => {
					received = message.value as string
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

			expect(received).toBe('flush-test-message')

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('handles large messages near batch size limit', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-large-batch')
			await client.connect()

			const topicName = uniqueName('it-large-batch')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Producer with small batch size
			const producer = client.producer({ lingerMs: 0, maxBatchBytes: 16384 })
			const consumer = client.consumer({
				groupId: uniqueName('it-large-batch-group'),
				autoOffsetReset: 'earliest',
			})

			// Create messages that will nearly fill the batch
			const largeValue = Buffer.alloc(8000, 'X')
			const messages = [{ value: largeValue }, { value: largeValue }]

			// Should succeed even though combined size is close to batch limit
			const results = await producer.send(topicName, messages)
			await producer.flush()

			expect(results).toHaveLength(2)
			expect(results[0]!.offset).toBeGreaterThanOrEqual(0n)
			expect(results[1]!.offset).toBeGreaterThanOrEqual(0n)

			// Verify messages are consumable
			const received: Buffer[] = []
			consumer.subscribe(topicName)
			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as Buffer)
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

			expect(received).toHaveLength(2)
			expect(received[0]!.equals(largeValue)).toBe(true)
			expect(received[1]!.equals(largeValue)).toBe(true)

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
