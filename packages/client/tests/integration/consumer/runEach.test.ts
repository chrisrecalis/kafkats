import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - runEach', () => {
	it('produces and consumes messages', async () => {
		const client = createClient('it-produce-consume')
		await client.connect()

		const topicName = uniqueName('it-produce-consume')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		const received: string[] = []

		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				received.push(message.value)
				if (received.length >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await producer.send(testTopic, [{ value: 'a' }, { value: 'b' }, { value: 'c' }])
		await producer.flush()

		await runPromise

		expect(received).toEqual(['a', 'b', 'c'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('does not commit a failed message when handler throws', async () => {
		const client = createClient('it-runEach-throws')
		await client.connect()

		const topicName = uniqueName('it-runEach-throws')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'm-0' }, { value: 'm-1' }, { value: 'm-2' }])
		await producer.flush()

		const groupId = uniqueName('it-group')
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const seen1: string[] = []
		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				seen1.push(message.value)
				if (message.value === 'm-1') {
					throw new Error('boom')
				}
			},
			{ autoCommit: false }
		)
		void run1.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer1.once('running', () => resolve())
			consumer1.once('error', err => reject(err))
		})

		await expect(run1).rejects.toThrow(/boom/)
		expect(seen1).toEqual(['m-0', 'm-1'])

		// A second consumer in the same group should resume at the failed message (at-least-once semantics)
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const seen2: string[] = []
		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				seen2.push(message.value)
				if (seen2.length >= 2) {
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
		expect(seen2).toEqual(['m-1', 'm-2'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('supports manual partition assignment without group coordination', async () => {
		const client = createClient('it-runEach-manual-assign')
		await client.connect()

		const topicName = uniqueName('it-runEach-manual-assign')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'x' }])
		await producer.flush()

		const groupId = uniqueName('it-group')
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const seen1: string[] = []
		const seen2: string[] = []

		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				seen1.push(message.value)
				consumer1.stop()
			},
			{
				autoCommit: false,
				commitOffsets: false,
				assignment: [{ topic: topicName, partition: 0 }],
			}
		)
		void run1.catch(() => {})

		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				seen2.push(message.value)
				consumer2.stop()
			},
			{
				autoCommit: false,
				commitOffsets: false,
				assignment: [{ topic: topicName, partition: 0 }],
			}
		)
		void run2.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			let running = 0
			const onRunning = () => {
				running++
				if (running >= 2) resolve()
			}
			consumer1.once('running', onRunning)
			consumer2.once('running', onRunning)
			consumer1.once('error', err => reject(err))
			consumer2.once('error', err => reject(err))
		})

		await run1
		await run2

		expect(seen1).toEqual(['x'])
		expect(seen2).toEqual(['x'])

		await producer.disconnect()
		await client.disconnect()
	})
})
