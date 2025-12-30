import { describe, expect, it, vi } from 'vitest'

import { murmur2Partitioner } from '@/producer/index.js'
import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - partitioning', () => {
	it('partitions messages round-robin', async () => {
		const client = createClient('it-round-robin')
		await client.connect()

		const topicName = uniqueName('it-round-robin')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0, partitioner: 'round-robin' })

		const results = await producer.send(
			testTopic,
			Array.from({ length: 9 }, (_, i) => ({ value: `m-${i}` }))
		)

		expect(results.map(r => r.partition)).toEqual([0, 1, 2, 0, 1, 2, 0, 1, 2])

		for (const partition of [0, 1, 2] as const) {
			const offsets = results.filter(r => r.partition === partition).map(r => r.offset)
			expect(offsets).toHaveLength(3)
			expect(offsets[1]! - offsets[0]!).toBe(1n)
			expect(offsets[2]! - offsets[1]!).toBe(1n)
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('partitions keyed messages using murmur2 (Java compatible)', async () => {
		const client = createClient('it-murmur2')
		await client.connect()

		const topicName = uniqueName('it-murmur2')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		const partitionCount = 6
		await client.createTopics([{ name: topicName, numPartitions: partitionCount, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0, partitioner: 'murmur2' })

		const results = await producer.send(testTopic, [
			{ key: Buffer.from('k1', 'utf-8'), value: 'v1' },
			{ key: Buffer.from('k2', 'utf-8'), value: 'v2' },
			{ key: Buffer.from('k1', 'utf-8'), value: 'v3' },
		])

		const expectedK1 = murmur2Partitioner(
			topicName,
			Buffer.from('k1', 'utf-8'),
			Buffer.from('v1', 'utf-8'),
			partitionCount
		)
		const expectedK2 = murmur2Partitioner(
			topicName,
			Buffer.from('k2', 'utf-8'),
			Buffer.from('v2', 'utf-8'),
			partitionCount
		)

		expect(results[0]!.partition).toBe(expectedK1)
		expect(results[1]!.partition).toBe(expectedK2)
		expect(results[2]!.partition).toBe(expectedK1)

		await producer.disconnect()
		await client.disconnect()
	})

	it('uses sticky partitioning for keyless messages (rotates between batches)', async () => {
		const client = createClient('it-sticky')
		await client.connect()

		const topicName = uniqueName('it-sticky')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		const partitionCount = 3
		await client.createTopics([{ name: topicName, numPartitions: partitionCount, replicationFactor: 1 }])

		await vi.waitFor(
			async () => {
				const meta = await client.getMetadata([topicName])
				expect(meta.topics.get(topicName)?.partitions.size).toBe(partitionCount)
			},
			{ timeout: 30_000 }
		)

		// Use lingerMs > 0 so the messages in each send() call share one batch.
		const producer = client.producer({ lingerMs: 50, partitioner: 'murmur2' })

		const batch1 = await producer.send(testTopic, [{ value: 'a' }, { value: 'b' }])
		expect(batch1[0]!.partition).toBe(batch1[1]!.partition)

		const batch1Partition = batch1[0]!.partition

		const batch2 = await producer.send(testTopic, [{ value: 'c' }, { value: 'd' }])
		expect(batch2[0]!.partition).toBe(batch2[1]!.partition)

		expect(batch2[0]!.partition).toBe((batch1Partition + 1) % partitionCount)

		await producer.disconnect()
		await client.disconnect()
	})

	it('honors explicit partition overrides', async () => {
		const client = createClient('it-explicit-partition')
		await client.connect()

		const topicName = uniqueName('it-explicit-partition')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const result = await producer.send(testTopic, { key: Buffer.from('k', 'utf-8'), value: 'v', partition: 2 })

		expect(result.partition).toBe(2)

		await producer.disconnect()
		await client.disconnect()
	})

	it('rejects invalid partition overrides', async () => {
		const client = createClient('it-invalid-partition')
		await client.connect()

		const topicName = uniqueName('it-invalid-partition')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })

		await expect(producer.send(testTopic, { value: 'x', partition: 99 })).rejects.toThrow(/Invalid partition 99/)

		await producer.disconnect()
		await client.disconnect()
	})
})
