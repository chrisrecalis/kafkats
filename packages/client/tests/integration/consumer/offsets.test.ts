import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - offsets', () => {
	it('commits offsets and resumes from last committed', async () => {
		const client = createClient('it-offset-commit')
		await client.connect()

		const topicName = uniqueName('it-offset-commit')
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
		const received1: string[] = []
		await consumer1.runEach(
			testTopic,
			async message => {
				received1.push(message.value)
				if (received1.length >= 5) {
					consumer1.stop()
				}
			},
			{ autoCommit: true, autoCommitIntervalMs: 200 }
		)

		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received2: string[] = []
		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				received2.push(message.value)
				consumer2.stop()
			},
			{ autoCommit: false }
		)
		void run2.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer2.once('running', () => resolve())
			consumer2.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'm-5' })
		await producer.flush()

		await run2

		expect(received1).toEqual(['m-0', 'm-1', 'm-2', 'm-3', 'm-4'])
		expect(received2).toEqual(['m-5'])

		await producer.disconnect()
		await client.disconnect()
	})

	it("autoOffsetReset='latest' starts at end for new group", async () => {
		const client = createClient('it-offset-latest')
		await client.connect()

		const topicName = uniqueName('it-offset-latest')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'old-1' }, { value: 'old-2' }])
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'latest' })
		const received: string[] = []

		const run = consumer.runEach(
			testTopic,
			async message => {
				received.push(message.value)
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'new-1' })
		await producer.flush()

		await run

		expect(received).toEqual(['new-1'])

		await producer.disconnect()
		await client.disconnect()
	})

	it("autoOffsetReset='none' throws when no committed offset exists", async () => {
		const client = createClient('it-offset-none')
		await client.connect()

		const topicName = uniqueName('it-offset-none')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'none' })

		await expect(
			consumer.runEach(
				testTopic,
				async () => {
					// Should never be called
				},
				{ autoCommit: false }
			)
		).rejects.toThrow(/No committed offset/)

		await client.disconnect()
	})
})
