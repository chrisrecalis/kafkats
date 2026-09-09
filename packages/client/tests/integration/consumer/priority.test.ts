import { describe, expect, it } from 'vitest'
import { string } from '@/codec.js'
import { strict, weighted } from '@/consumer/priority.js'
import { topic } from '@/topic.js'
import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - topic priority', () => {
	it('serves live traffic promptly through a bulk backlog with strict priority', async () => {
		const client = createClient('it-strict-priority')
		await client.connect()
		const liveName = uniqueName('it-priority-live')
		const bulkName = uniqueName('it-priority-bulk')
		const live = topic(liveName, { value: string() })
		const bulk = topic(bulkName, { value: string() })
		await client.createTopics([
			{ name: liveName, numPartitions: 1, replicationFactor: 1 },
			{ name: bulkName, numPartitions: 1, replicationFactor: 1 },
		])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			bulk,
			Array.from({ length: 2000 }, (_, index) => ({ value: `bulk-${index}` }))
		)
		await producer.flush()

		const maxRecords = 50
		const consumer = client.consumer({
			groupId: uniqueName('it-priority-group'),
			autoOffsetReset: 'earliest',
			maxRecords,
		})
		let bulkCount = 0
		let bulkAtLiveStart = 0
		let bulkAtLiveEnd = 0
		let liveCount = 0
		const safetyTimer = setTimeout(() => consumer.stop(), 15000)

		await consumer.runEach(
			[live, bulk] as const,
			async message => {
				if (message.topic === bulkName) {
					bulkCount++
					if (bulkCount === 1) {
						bulkAtLiveStart = bulkCount
						await producer.send(
							live,
							Array.from({ length: 5 }, (_, index) => ({ value: `live-${index}` }))
						)
						await producer.flush()
					}
					await sleep(2)
					return
				}

				liveCount++
				if (liveCount === 5) {
					bulkAtLiveEnd = bulkCount
					consumer.stop()
				}
			},
			{ autoCommit: false, partitionConcurrency: 4, priority: strict({ order: [liveName, bulkName] }) }
		)
		clearTimeout(safetyTimer)

		expect(liveCount).toBe(5)
		expect(bulkAtLiveEnd - bulkAtLiveStart).toBeLessThanOrEqual(2 * maxRecords)
		await producer.disconnect()
		await client.disconnect()
	})

	it('approximates configured shares while both topics have demand', async () => {
		const client = createClient('it-weighted-priority')
		await client.connect()
		const liveName = uniqueName('it-weighted-live')
		const bulkName = uniqueName('it-weighted-bulk')
		const live = topic(liveName, { value: string() })
		const bulk = topic(bulkName, { value: string() })
		await client.createTopics([
			{ name: liveName, numPartitions: 1, replicationFactor: 1 },
			{ name: bulkName, numPartitions: 1, replicationFactor: 1 },
		])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			live,
			Array.from({ length: 200 }, (_, index) => ({ value: `live-${index}` }))
		)
		await producer.send(
			bulk,
			Array.from({ length: 200 }, (_, index) => ({ value: `bulk-${index}` }))
		)
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-weighted-group'),
			autoOffsetReset: 'earliest',
			maxRecords: 40,
		})
		const firstWindow: string[] = []
		let liveCount = 0
		let bulkWhenLiveFinished = 0
		let bulkCount = 0
		const safetyTimer = setTimeout(() => consumer.stop(), 15000)
		await consumer.runEach(
			[live, bulk] as const,
			async message => {
				if (firstWindow.length < 40) firstWindow.push(message.topic)
				if (message.topic === liveName) liveCount++
				else bulkCount++
				if (liveCount === 200) {
					bulkWhenLiveFinished = bulkCount
					consumer.stop()
				}
			},
			{ autoCommit: false, priority: weighted({ shares: { [liveName]: 3, [bulkName]: 1 } }) }
		)
		clearTimeout(safetyTimer)

		const firstLive = firstWindow.filter(name => name === liveName).length
		const firstBulk = firstWindow.filter(name => name === bulkName).length
		expect(firstLive).toBeGreaterThanOrEqual(28)
		expect(firstBulk).toBeGreaterThanOrEqual(8)
		expect(bulkWhenLiveFinished).toBeLessThan(200)
		await producer.disconnect()
		await client.disconnect()
	})
})
