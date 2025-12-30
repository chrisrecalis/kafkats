import { describe, expect, it, vi } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - pause/resume', () => {
	it('pauses and resumes fetching for an assigned partition (backpressure)', async () => {
		const client = createClient('it-consumer-pause-resume')
		await client.connect()

		const topicName = uniqueName('it-consumer-pause-resume')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		expect(() => consumer.pause([{ topic: topicName, partition: 0 }])).toThrow(/Consumer is not running/)
		expect(() => consumer.resume([{ topic: topicName, partition: 0 }])).toThrow(/Consumer is not running/)

		const received: string[] = []

		let pauseResolve!: () => void
		const paused = new Promise<void>(resolve => {
			pauseResolve = resolve
		})

		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				received.push(message.value)

				if (received.length === 1) {
					consumer.pause([{ topic: message.topic, partition: message.partition }])
					// Idempotent pause
					consumer.pause([{ topic: message.topic, partition: message.partition }])
					pauseResolve()
				}

				if (received.length >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		try {
			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			await producer.send(testTopic, { value: 'm-0' })
			await producer.flush()

			await paused

			await producer.send(testTopic, [{ value: 'm-1' }, { value: 'm-2' }])
			await producer.flush()

			// While paused, no further messages should be fetched/processed.
			await sleep(1000)
			expect(received).toEqual(['m-0'])

			consumer.resume([{ topic: topicName, partition: 0 }])
			// Idempotent resume
			consumer.resume([{ topic: topicName, partition: 0 }])

			await runPromise
			expect(received).toEqual(['m-0', 'm-1', 'm-2'])
		} finally {
			consumer.stop()
			await runPromise.catch(() => {})
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('pausing one partition does not block other assigned partitions', async () => {
		const client = createClient('it-consumer-pause-one-partition')
		await client.connect()

		const topicName = uniqueName('it-consumer-pause-one-partition')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		const p0: string[] = []
		const p1: string[] = []

		let pauseResolve!: () => void
		const paused = new Promise<void>(resolve => {
			pauseResolve = resolve
		})
		let pausedPartition0 = false

		const runPromise = consumer.runEach(
			testTopic,
			async message => {
				if (message.partition === 0) {
					p0.push(message.value)
					if (!pausedPartition0) {
						pausedPartition0 = true
						consumer.pause([{ topic: message.topic, partition: 0 }])
						pauseResolve()
					}
				} else if (message.partition === 1) {
					p1.push(message.value)
				}

				if (p0.length >= 3 && p1.length >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false, partitionConcurrency: 2 }
		)
		void runPromise.catch(() => {})

		try {
			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			// Trigger pause by sending a single record to partition 0.
			await producer.send(testTopic, { value: 'p0-0', partition: 0 })
			await producer.flush()

			await paused

			// While partition 0 is paused, partition 1 should continue consuming.
			await producer.send(testTopic, [
				{ value: 'p1-0', partition: 1 },
				{ value: 'p1-1', partition: 1 },
				{ value: 'p1-2', partition: 1 },
				{ value: 'p0-1', partition: 0 },
				{ value: 'p0-2', partition: 0 },
			])
			await producer.flush()

			await vi.waitFor(
				() => {
					expect(p1).toEqual(['p1-0', 'p1-1', 'p1-2'])
				},
				{ timeout: 30_000 }
			)

			await sleep(1000)
			expect(p0).toEqual(['p0-0'])

			consumer.resume([{ topic: topicName, partition: 0 }])

			await vi.waitFor(
				() => {
					expect(p0).toEqual(['p0-0', 'p0-1', 'p0-2'])
				},
				{ timeout: 30_000 }
			)

			await runPromise

			expect(p1).toEqual(['p1-0', 'p1-1', 'p1-2'])
		} finally {
			consumer.stop()
			await runPromise.catch(() => {})
		}

		await producer.disconnect()
		await client.disconnect()
	})
})
