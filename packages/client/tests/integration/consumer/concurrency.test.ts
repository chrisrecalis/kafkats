import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - concurrency', () => {
	it('processes multiple partitions concurrently when partitionConcurrency > 1', async () => {
		await withKafka(async ({ createClient }) => {
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

			consumer.subscribe(testTopic)
			await consumer.runEach(
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
	})
})
