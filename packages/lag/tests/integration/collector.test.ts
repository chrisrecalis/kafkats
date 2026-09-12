import { describe, expect, it } from 'vitest'
import { KafkaClient, string, topic } from '@kafkats/client'
import { LagCollector } from '../../src/collector.js'

describe('lag collector', () => {
	it('measures the age of the first unconsumed record', async () => {
		const brokers = process.env.KAFKA_BROKERS?.split(',') ?? []
		expect(brokers.length).toBeGreaterThan(0)

		const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`
		const topicName = `lag-topic-${suffix}`
		const groupId = `lag-group-${suffix}`
		const client = new KafkaClient({ brokers, clientId: `lag-test-${suffix}`, logLevel: 'error' })
		await client.connect()

		try {
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
			const testTopic = topic<string>(topicName, { value: string() })
			const producer = client.producer()
			await producer.send(testTopic, { value: 'consumed', partition: 0 })

			const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			const run = consumer.runEach(testTopic, async () => consumer.stop(), { autoCommit: true })
			await run

			const recordTime = Date.now() - 5 * 60_000
			await producer.send(testTopic, { value: 'pending', partition: 0, timestamp: new Date(recordTime) })
			await producer.disconnect()

			const collector = new LagCollector({ client, groups: [groupId] })

			const snapshot = await collector.collect()
			expect(snapshot.errors).toEqual([])
			const partition = snapshot.groups.find(group => group.groupId === groupId)?.partitions[0]
			expect(partition?.timeLagSeconds).toBeGreaterThanOrEqual(300)
		} finally {
			await client.disconnect()
		}
	})

	it('estimates from the produce rate in auto mode and falls back to a fetch while warming up', async () => {
		const brokers = process.env.KAFKA_BROKERS?.split(',') ?? []
		const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`
		const topicName = `lag-rate-topic-${suffix}`
		const groupId = `lag-rate-group-${suffix}`
		const client = new KafkaClient({ brokers, clientId: `lag-rate-test-${suffix}`, logLevel: 'error' })
		await client.connect()

		try {
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
			const testTopic = topic<string>(topicName, { value: string() })
			const producer = client.producer()
			const batch = (n: number, timestamp: Date) =>
				Array.from({ length: n }, (_, i) => ({ value: `m-${i}`, partition: 0, timestamp }))

			// Consume the first record so the group has a committed offset of 1
			await producer.send(testTopic, { value: 'consumed', partition: 0 })
			const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
			await consumer.runEach(testTopic, async () => consumer.stop(), { autoCommit: true })

			let now = 1_000_000_000_000
			const collector = new LagCollector({ client, groups: [groupId], mode: 'auto', now: () => now })
			const lagOf = (snapshot: Awaited<ReturnType<LagCollector['collect']>>) =>
				snapshot.groups.find(g => g.groupId === groupId)?.partitions[0]?.timeLagSeconds

			// First collection: no rate history yet, so auto falls back to the record timestamp (5 minutes old)
			await producer.send(testTopic, batch(99, new Date(now - 5 * 60_000)))
			const first = await collector.collect()
			expect(first.errors).toEqual([])
			expect(lagOf(first)).toBeCloseTo(300, 0)
			expect(first.stats.fetchRequests).toBe(1)
			expect(first.stats.fetchBytes).toBeGreaterThan(0)
			expect(first.stats.partitions.fetched).toBe(1)

			// 30 s later, 300 more records: 10 records/s, 399 behind → ~40 s estimated, no fetch involved
			now += 30_000
			await producer.send(testTopic, batch(300, new Date(now)))
			const second = await collector.collect()
			expect(second.errors).toEqual([])
			expect(lagOf(second)).toBeCloseTo(39.9, 1)
			expect(second.stats.fetchRequests).toBe(0)
			expect(second.stats.partitions.estimated).toBe(1)

			await producer.disconnect()
		} finally {
			await client.disconnect()
		}
	})
})
