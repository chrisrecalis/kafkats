import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - lifecycle', () => {
	it('stops runEach when AbortSignal aborts', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-runEach-abort')
			await client.connect()

			const topicName = uniqueName('it-runEach-abort')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })
			const abortController = new AbortController()

			let called = false
			const run = consumer.runEach(
				testTopic,
				async () => {
					called = true
				},
				{ autoCommit: false, signal: abortController.signal }
			)
			void run.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			abortController.abort()
			await run

			expect(called).toBe(false)

			await client.disconnect()
		})
	})

	it('rejects starting runEach while already running', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-runEach-double-start')
			await client.connect()

			const topicName = uniqueName('it-runEach-double-start')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })
			const abortController = new AbortController()

			const run1 = consumer.runEach(testTopic, async () => {}, {
				autoCommit: false,
				signal: abortController.signal,
			})
			void run1.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			await expect(consumer.runEach(testTopic, async () => {}, { autoCommit: false })).rejects.toThrow(
				/already running/i
			)

			abortController.abort()
			await run1

			await client.disconnect()
		})
	})
})
