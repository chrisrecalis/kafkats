import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Producer (integration) - key partitioning', () => {
	it('sends messages with same key to same partition', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-key-partition')
			await client.connect()

			const topicName = uniqueName('it-key-partition')
			const testTopic = topic<string, string>(topicName, {
				key: string(),
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send multiple messages with the same key
			const results = await producer.send(testTopic, [
				{ key: 'user-123', value: 'event-1' },
				{ key: 'user-123', value: 'event-2' },
				{ key: 'user-123', value: 'event-3' },
			])
			await producer.flush()

			// All messages with same key should go to same partition
			expect(results).toHaveLength(3)
			expect(results[0]!.partition).toBe(results[1]!.partition)
			expect(results[1]!.partition).toBe(results[2]!.partition)
		})
	})

	it('distributes messages with different keys across partitions', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-key-distribution')
			await client.connect()

			const topicName = uniqueName('it-key-distribution')
			const testTopic = topic<string, string>(topicName, {
				key: string(),
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send messages with different keys
			const keys = ['user-a', 'user-b', 'user-c', 'user-d', 'user-e', 'user-f', 'user-g', 'user-h']
			const results = await producer.send(
				testTopic,
				keys.map(key => ({ key, value: `value-${key}` }))
			)
			await producer.flush()

			// Collect partitions used
			const partitionsUsed = new Set(results.map(r => r.partition))

			// With 8 different keys and 4 partitions, we should use multiple partitions
			// (statistically very unlikely all 8 keys hash to same partition)
			expect(partitionsUsed.size).toBeGreaterThan(1)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('maintains key-partition consistency across multiple sends', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-key-consistency')
			await client.connect()

			const topicName = uniqueName('it-key-consistency')
			const testTopic = topic<string, string>(topicName, {
				key: string(),
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send same key in separate calls
			const result1 = await producer.send(testTopic, { key: 'consistent-key', value: 'first' })
			await producer.flush()

			const result2 = await producer.send(testTopic, { key: 'consistent-key', value: 'second' })
			await producer.flush()

			const result3 = await producer.send(testTopic, { key: 'consistent-key', value: 'third' })
			await producer.flush()

			// Same key should always go to same partition
			expect(result1.partition).toBe(result2.partition)
			expect(result2.partition).toBe(result3.partition)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('consumer receives messages with keys correctly', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-consume-keys')
			await client.connect()

			const topicName = uniqueName('it-consume-keys')
			const testTopic = topic<string, string>(topicName, {
				key: string(),
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			await producer.send(testTopic, [
				{ key: 'key-a', value: 'value-a' },
				{ key: 'key-b', value: 'value-b' },
			])
			await producer.flush()

			const received: Array<{ key: string | null; value: string }> = []

			const run = consumer.runEach(
				testTopic,
				async message => {
					received.push({ key: message.key, value: message.value })
					if (received.length >= 2) {
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

			expect(received).toHaveLength(2)
			expect(received).toContainEqual({ key: 'key-a', value: 'value-a' })
			expect(received).toContainEqual({ key: 'key-b', value: 'value-b' })

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('handles null keys with round-robin partitioning', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-null-key')
			await client.connect()

			const topicName = uniqueName('it-null-key')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send multiple messages without keys (null keys)
			const results = await producer.send(testTopic, [
				{ value: 'no-key-1' },
				{ value: 'no-key-2' },
				{ value: 'no-key-3' },
				{ value: 'no-key-4' },
			])
			await producer.flush()

			expect(results).toHaveLength(4)
			// All should have valid partitions
			for (const result of results) {
				expect(result.partition).toBeGreaterThanOrEqual(0)
				expect(result.partition).toBeLessThan(4)
			}

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
