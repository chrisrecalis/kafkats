import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - concurrency', () => {
	it('processes multiple partitions concurrently when partitionConcurrency > 1', async () => {
		const client = createClient('it-partition-concurrency')
		await client.connect()

		const topicName = uniqueName('it-partition-concurrency')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [
			{ value: 'p0', partition: 0 },
			{ value: 'p1', partition: 1 },
		])
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		const startedAt = new Map<number, number>()
		const finished = new Set<number>()
		const handlerSleepMs = 1200

		await consumer.runEach(
			testTopic,
			async message => {
				if (!startedAt.has(message.partition)) {
					startedAt.set(message.partition, Date.now())
				}

				await sleep(handlerSleepMs)
				finished.add(message.partition)

				if (finished.size >= 2) {
					consumer.stop()
				}
			},
			{ autoCommit: false, partitionConcurrency: 2 }
		)

		expect(startedAt.has(0)).toBe(true)
		expect(startedAt.has(1)).toBe(true)

		const deltaMs = Math.abs(startedAt.get(0)! - startedAt.get(1)!)
		expect(deltaMs).toBeLessThan(1000)

		await producer.disconnect()
		await client.disconnect()
	})

	it('continues an idle partition while another partition handler is blocked', async () => {
		const client = createClient('it-partition-pipeline')
		await client.connect()

		const topicName = uniqueName('it-partition-pipeline')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'slow', partition: 0 }])
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxRecords: 1,
		})
		let releaseSlowHandler!: () => void
		const slowHandler = new Promise<void>(resolve => {
			releaseSlowHandler = resolve
		})
		let signalSlowStarted!: () => void
		const slowStarted = new Promise<void>(resolve => {
			signalSlowStarted = resolve
		})
		let fastRecords = 0
		let releasedBySafetyTimer = false
		let fastPartitionAdvancedBeforeSlowRelease = false
		const safetyTimer = setTimeout(() => {
			releasedBySafetyTimer = true
			releaseSlowHandler()
		}, 2000)

		const run = consumer.runEach(
			testTopic,
			async message => {
				if (message.partition === 0) {
					signalSlowStarted()
					await slowHandler
					return
				}

				fastRecords++
				if (fastRecords === 2) {
					fastPartitionAdvancedBeforeSlowRelease = !releasedBySafetyTimer
					releaseSlowHandler()
					consumer.stop()
				}
			},
			{ autoCommit: false, partitionConcurrency: 2 }
		)

		await slowStarted
		await producer.send(testTopic, [
			{ value: 'fast-1', partition: 1 },
			{ value: 'fast-2', partition: 1 },
		])
		await producer.flush()
		await run
		clearTimeout(safetyTimer)

		expect(fastRecords).toBe(2)
		expect(fastPartitionAdvancedBeforeSlowRelease).toBe(true)

		await producer.disconnect()
		await client.disconnect()
	})
})
