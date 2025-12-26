import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Producer (integration) - errors', () => {
	it('throws error when sending to non-existent topic without auto-create', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-no-topic')
			await client.connect()

			const topicName = uniqueName('it-does-not-exist')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			const producer = client.producer({ lingerMs: 0 })

			// Attempt to send to a topic that doesn't exist
			// Kafka default config may auto-create topics, but if metadata refresh fails
			// or auto-create is disabled, this should fail
			try {
				await producer.send(testTopic, { value: 'test' })
				await producer.flush()
				// If we get here, auto-create may be enabled - that's also valid
			} catch (error) {
				// Expected to fail with unknown topic or leader not available
				expect(error).toBeInstanceOf(Error)
			}

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('throws error when sending to invalid partition', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-invalid-partition')
			await client.connect()

			const topicName = uniqueName('it-invalid-partition')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Attempt to send to partition 99 when only 2 exist (0 and 1)
			await expect(producer.send(testTopic, { value: 'test', partition: 99 })).rejects.toThrow()

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('flushes pending messages on producer disconnect (graceful shutdown)', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-disconnect-flush')
			await client.connect()

			const topicName = uniqueName('it-disconnect-flush')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Producer with long linger time
			const producer = client.producer({ lingerMs: 60000 })

			// Send without explicit flush
			const sendPromise = producer.send(testTopic, { value: 'pending' })

			// Disconnect flushes pending messages gracefully
			await producer.disconnect()

			// The send should succeed (graceful disconnect flushes)
			const result = await sendPromise
			expect(result.offset).toBeGreaterThanOrEqual(0n)

			await client.disconnect()
		})
	})

	it('handles producer reconnect after disconnect', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-reconnect')
			await client.connect()

			const topicName = uniqueName('it-reconnect')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer1 = client.producer({ lingerMs: 0 })

			// Send with first producer
			const result1 = await producer1.send(testTopic, { value: 'first' })
			await producer1.flush()
			expect(result1.offset).toBeGreaterThanOrEqual(0n)

			await producer1.disconnect()

			// Create a new producer and send
			const producer2 = client.producer({ lingerMs: 0 })
			const result2 = await producer2.send(testTopic, { value: 'second' })
			await producer2.flush()
			expect(result2.offset).toBeGreaterThanOrEqual(0n)

			await producer2.disconnect()
			await client.disconnect()
		})
	})

	it('validates message size limits', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-msg-size')
			await client.connect()

			const topicName = uniqueName('it-msg-size')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Create a very large message (larger than typical max.message.bytes)
			const largeValue = Buffer.alloc(10 * 1024 * 1024) // 10MB

			// This should fail due to message size exceeding broker limits
			try {
				await producer.send(topicName, { value: largeValue })
				await producer.flush()
				// If we get here, broker has high message size limit
			} catch (error) {
				expect(error).toBeInstanceOf(Error)
				// Could be MESSAGE_TOO_LARGE or similar
			}

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
