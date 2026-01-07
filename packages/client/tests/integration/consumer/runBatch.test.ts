import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'
import { setTimeout } from 'node:timers/promises'

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
			{ autoCommit: false }
		)

		const all = batches.flat()
		expect(all).toEqual(Array.from({ length: 10 }, (_, i) => `m-${i}`))
		// Batch size is determined by fetch, not a fixed max
		expect(batches.length).toBeGreaterThanOrEqual(1)

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
			{ autoCommit: false }
		)
		void run1.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer1.once('running', () => resolve())
			consumer1.once('error', err => reject(err))
		})

		await expect(run1).rejects.toThrow(/boom/)
		// The batch contains all messages from the fetch
		expect(batches1[0]).toEqual(Array.from({ length: 5 }, (_, i) => `m-${i}`))

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

	it('resolveOffset commits only resolved offsets on success', async () => {
		const client = createClient('it-runBatch-resolveOffset')
		await client.connect()

		const topicName = uniqueName('it-runBatch-resolveOffset')
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

		// Consume and resolve only first 3 messages
		await consumer1.runBatch(
			testTopic,
			async (batch, ctx) => {
				for (let i = 0; i < 3; i++) {
					ctx.resolveOffset(batch[i]!.offset)
				}
				consumer1.stop()
			},
			{ autoCommit: true, autoCommitIntervalMs: 100 }
		)

		// Wait for commit
		await setTimeout(200)

		// Second consumer should read from offset 3 (messages m-3 and m-4)
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received2: string[] = []
		await consumer2.runEach(
			testTopic,
			async message => {
				received2.push(message.value)
				if (received2.length >= 2) {
					consumer2.stop()
				}
			},
			{ autoCommit: false }
		)

		expect(received2).toEqual(['m-3', 'm-4'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('resolveOffset commits partial progress on handler failure', async () => {
		const client = createClient('it-runBatch-resolveOffset-fail')
		await client.connect()

		const topicName = uniqueName('it-runBatch-resolveOffset-fail')
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

		// Consume, resolve 2 messages, then throw
		const run1 = consumer1.runBatch(
			testTopic,
			async (batch, ctx) => {
				// Resolve first 2 messages
				ctx.resolveOffset(batch[0]!.offset)
				ctx.resolveOffset(batch[1]!.offset)
				// Then fail
				throw new Error('partial-fail')
			},
			{ autoCommit: true, autoCommitIntervalMs: 100 }
		)
		void run1.catch(() => {})

		await expect(run1).rejects.toThrow(/partial-fail/)

		// Wait for commit of resolved offsets
		await setTimeout(200)

		// Second consumer should read from offset 2 (messages m-2, m-3, m-4)
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received2: string[] = []
		await consumer2.runEach(
			testTopic,
			async message => {
				received2.push(message.value)
				if (received2.length >= 3) {
					consumer2.stop()
				}
			},
			{ autoCommit: false }
		)

		expect(received2).toEqual(['m-2', 'm-3', 'm-4'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('batch context provides firstOffset and lastOffset', async () => {
		const client = createClient('it-runBatch-offsets')
		await client.connect()

		const topicName = uniqueName('it-runBatch-offsets')
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

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		let capturedFirstOffset: bigint | undefined
		let capturedLastOffset: bigint | undefined

		await consumer.runBatch(
			testTopic,
			async (batch, ctx) => {
				capturedFirstOffset = ctx.firstOffset
				capturedLastOffset = ctx.lastOffset
				consumer.stop()
			},
			{ autoCommit: false }
		)

		expect(capturedFirstOffset).toBe(0n)
		expect(capturedLastOffset).toBe(4n)

		await producer.disconnect()
		await client.disconnect()
	})
})
