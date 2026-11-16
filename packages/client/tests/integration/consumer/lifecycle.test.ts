import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - lifecycle', () => {
	it('stops runEach when AbortSignal aborts', async () => {
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

	it('rejects starting runEach while already running', async () => {
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

	it('keeps its group membership while a handler finishes after stop()', async () => {
		const client = createClient('it-stop-slow-handler')
		await client.connect()

		const topicName = uniqueName('it-stop-slow-handler')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer()
		await producer.send(testTopic, { value: 'first' })

		const groupId = uniqueName('it-group')
		// The session outlives the test, so only a LeaveGroup can empty the group in time.
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest', sessionTimeoutMs: 45_000 })
		await consumer.runEach(testTopic, async () => {
			consumer.stop()
			// Longer than the consumer's shutdown deadline for an unresponsive broker (5s).
			await sleep(7_000)
		})

		const [group] = await client.admin().describeGroups([groupId])
		expect(group!.state).toBe('Empty')

		// The record the handler finished was committed: the next member starts after it.
		await producer.send(testTopic, { value: 'second' })
		const seen: string[] = []
		const next = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		await next.runEach(testTopic, async message => {
			seen.push(message.value)
			next.stop()
		})
		expect(seen).toEqual(['second'])

		await producer.disconnect()
		await client.disconnect()
	}, 60_000)
})
