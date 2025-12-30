import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - runBatch', () => {
	it('consumes messages in batches', async () => {
		const client = createClient('it-run-batch')
		await client.connect()

		const topicName = uniqueName('it-run-batch')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 10 }, (_, i) => ({ value: `m-${i}` }))
		)
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		const batches: string[][] = []
		await consumer.runBatch(
			testTopic,
			async batch => {
				batches.push(batch.map(m => m.value))
				const total = batches.reduce((sum, b) => sum + b.length, 0)
				if (total >= 10) {
					consumer.stop()
				}
			},
			{ autoCommit: false, maxBatchSize: 4, maxBatchWaitMs: 50 }
		)

		const all = batches.flat()
		expect(all).toEqual(Array.from({ length: 10 }, (_, i) => `m-${i}`))
		expect(batches.every(b => b.length <= 4)).toBe(true)

		await producer.disconnect()
		await client.disconnect()
	})

	it('does not commit a failed batch when handler throws', async () => {
		const client = createClient('it-runBatch-throws')
		await client.connect()

		const topicName = uniqueName('it-runBatch-throws')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 5 }, (_, i) => ({ value: `m-${i}` }))
		)
		await producer.flush()

		const groupId = uniqueName('it-group')
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const batches1: string[][] = []
		const run1 = consumer1.runBatch(
			testTopic,
			async batch => {
				batches1.push(batch.map(m => m.value))
				throw new Error('boom')
			},
			{ autoCommit: false, maxBatchSize: 3, maxBatchWaitMs: 50 }
		)
		void run1.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer1.once('running', () => resolve())
			consumer1.once('error', err => reject(err))
		})

		await expect(run1).rejects.toThrow(/boom/)
		expect(batches1[0]).toEqual(['m-0', 'm-1', 'm-2'])

		// Since the batch handler failed, offsets should not be committed and a new consumer should re-read from the start
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received2: string[] = []
		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				received2.push(message.value)
				if (received2.length >= 5) {
					consumer2.stop()
				}
			},
			{ autoCommit: false }
		)
		void run2.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer2.once('running', () => resolve())
			consumer2.once('error', err => reject(err))
		})

		await run2
		expect(received2).toEqual(Array.from({ length: 5 }, (_, i) => `m-${i}`))

		await producer.disconnect()
		await client.disconnect()
	})
})
