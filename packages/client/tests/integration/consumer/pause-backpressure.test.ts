import { describe, expect, it, vi } from 'vitest'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer pause and resume under backpressure (integration)', () => {
	it('delivers nothing while paused and every record exactly once through 1000 pause/resume cycles', async () => {
		const client = createClient('it-pause-backpressure')
		await client.connect()
		const topicName = uniqueName('it-pause-backpressure')
		const partitionCount = 4
		const perPartition = 1000
		await client.createTopics([{ name: topicName, numPartitions: partitionCount, replicationFactor: 1 }])
		// Small record batches: a fetch returns an oversized first batch for its first partition only, so with small
		// ones each fetch carries records of every partition.
		const producer = client.producer({ lingerMs: 0 })
		await Promise.all(
			Array.from({ length: partitionCount }, async (_, partition) => {
				for (let start = 0; start < perPartition; start += 4) {
					const records = Array.from({ length: 4 }, (_, i) => ({
						value: Buffer.from(`${partition}-${start + i}`),
						partition,
					}))
					await producer.send(topicName, records)
				}
			})
		)
		await producer.disconnect()

		const consumer = client.consumer({
			groupId: uniqueName('it-pause-backpressure-group'),
			autoOffsetReset: 'earliest',
			maxWaitMs: 100,
			// About 4 records per batch, and a poll stops once it drained that much: it returns 2 batches, so the
			// second waits for the slot while the handler of the first pauses every partition.
			maxBytesPerPartition: 300,
		})
		const all = Array.from({ length: partitionCount }, (_, partition) => ({ topic: topicName, partition }))
		const deliveries = new Map<string, number>()
		const deliveredWhilePaused: string[] = []
		let paused = false
		let cycles = 0
		const run = consumer.runBatch(
			topicName,
			async messages => {
				for (const message of messages) {
					const key = `${message.partition}:${message.offset}`
					if (paused) deliveredWhilePaused.push(key)
					deliveries.set(key, (deliveries.get(key) ?? 0) + 1)
				}
				// Backpressure: the records were taken over; stop every partition until the next turn of the loop.
				if (cycles < 1000) {
					cycles++
					paused = true
					consumer.pause(all)
					setImmediate(() => {
						paused = false
						if (consumer.isRunning) consumer.resume(all)
					})
				}
			},
			{ autoCommit: false }
		)

		await vi.waitFor(() => expect(deliveries.size).toBe(partitionCount * perPartition), {
			timeout: 60_000,
			interval: 100,
		})
		await new Promise(resolve => setTimeout(resolve, 500))
		consumer.stop()
		await run

		expect(cycles).toBe(1000)
		expect(deliveredWhilePaused).toEqual([])
		expect([...deliveries.values()].filter(count => count !== 1)).toEqual([])
		expect(deliveries.size).toBe(partitionCount * perPartition)
		await client.disconnect()
	}, 90_000)
})
