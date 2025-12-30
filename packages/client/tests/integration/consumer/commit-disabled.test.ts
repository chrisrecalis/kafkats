import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - commit disabled', () => {
	it('does not commit offsets when commitOffsets is false', async () => {
		const client = createClient('it-no-commit')
		await client.connect()

		const topicName = uniqueName('it-no-commit')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'm-0' }, { value: 'm-1' }, { value: 'm-2' }])
		await producer.flush()

		const groupId = uniqueName('it-group')

		// First consumer with commitOffsets: false
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const seen1: string[] = []

		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				seen1.push(message.value)
				if (seen1.length >= 3) {
					consumer1.stop()
				}
			},
			{ commitOffsets: false, autoCommit: true, autoCommitIntervalMs: 100 }
		)
		void run1.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer1.once('running', () => resolve())
			consumer1.once('error', err => reject(err))
		})

		await run1

		expect(seen1).toEqual(['m-0', 'm-1', 'm-2'])

		// Second consumer should see all messages again since first didn't commit
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const seen2: string[] = []

		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				seen2.push(message.value)
				if (seen2.length >= 3) {
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

		// Should see all messages again since first consumer didn't commit
		expect(seen2).toEqual(['m-0', 'm-1', 'm-2'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('commits offsets normally when commitOffsets is true (default)', async () => {
		const client = createClient('it-commit-enabled')
		await client.connect()

		const topicName = uniqueName('it-commit-enabled')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'm-0' }, { value: 'm-1' }, { value: 'm-2' }])
		await producer.flush()

		const groupId = uniqueName('it-group')

		// First consumer with commitOffsets: true (default)
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const seen1: string[] = []

		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				seen1.push(message.value)
				if (seen1.length >= 3) {
					consumer1.stop()
				}
			},
			{ commitOffsets: true, autoCommit: true, autoCommitIntervalMs: 100 }
		)
		void run1.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer1.once('running', () => resolve())
			consumer1.once('error', err => reject(err))
		})

		await run1

		expect(seen1).toEqual(['m-0', 'm-1', 'm-2'])

		// Add a new message after first consumer stopped
		await producer.send(testTopic, { value: 'm-3' })
		await producer.flush()

		// Second consumer should only see new message
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const seen2: string[] = []

		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				seen2.push(message.value)
				consumer2.stop()
			},
			{ autoCommit: false }
		)
		void run2.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer2.once('running', () => resolve())
			consumer2.once('error', err => reject(err))
		})

		await run2

		// Should only see new message since first consumer committed
		expect(seen2).toEqual(['m-3'])

		await producer.disconnect()
		await client.disconnect()
	})
})
