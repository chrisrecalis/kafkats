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

	it('routes negative-hash keys to the Java-compatible partition (toPositive sign-bit masking)', async () => {
		const client = createClient('it-murmur2-negative')
		await client.connect()

		const topicName = uniqueName('it-murmur2-negative')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		// 6 partitions on purpose: 2^31 % 6 !== 0, so the sign-bit handling inside
		// toPositive is observable end-to-end. A power-of-2 partition count would make
		// the buggy `hash >>> 0` and the correct `hash & 0x7fffffff` agree and hide it.
		const partitionCount = 6
		await client.createTopics([{ name: topicName, numPartitions: partitionCount, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0, partitioner: 'murmur2' })

		// Independent oracle: partition = (murmur2(key) & 0x7fffffff) % partitionCount,
		// matching Kafka's org.apache.kafka.common.utils.Utils.toPositive. Every key here
		// has a NEGATIVE murmur2 hash, where the buggy full-unsigned `>>> 0` diverges from
		// the correct sign-bit-clear `& 0x7fffffff`.
		const cases: Array<{ key: string; expectedPartition: number; buggyPartition: number }> = [
			{ key: 'user-2', expectedPartition: 2, buggyPartition: 4 }, // murmur2 -324981792
			{ key: 'a', expectedPartition: 4, buggyPartition: 0 }, // murmur2 -1563381124
			{ key: 'b', expectedPartition: 2, buggyPartition: 4 }, // murmur2 -1853091852
			{ key: 'session-9', expectedPartition: 2, buggyPartition: 4 }, // murmur2 -1117894392
		]

		const results = await producer.send(
			testTopic,
			cases.map(c => ({ key: Buffer.from(c.key, 'utf-8'), value: `v-${c.key}` }))
		)

		for (let i = 0; i < cases.length; i++) {
			const { key, expectedPartition, buggyPartition } = cases[i]!
			// Guard the oracle: each case must actually distinguish the fix from the bug.
			expect(expectedPartition, `case "${key}" must differ between fix and bug`).not.toBe(buggyPartition)
			expect(
				results[i]!.partition,
				`key "${key}" must route to the Java-compatible partition ${expectedPartition}`
			).toBe(expectedPartition)
		}

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
