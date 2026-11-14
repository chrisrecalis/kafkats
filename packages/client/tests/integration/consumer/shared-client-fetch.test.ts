import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - consumers sharing a client', () => {
	// A broker answers the requests of one connection one at a time. If consumers shared a fetch
	// connection, every fetch of the busy consumer would queue behind the idle one's long poll.
	it("does not hold one consumer's fetches behind another consumer's long poll", async () => {
		const client = createClient('it-shared-client-fetch')
		await client.connect()

		const idleTopic = uniqueName('it-shared-fetch-idle')
		const busyTopicName = uniqueName('it-shared-fetch-busy')
		const busyTopic = topic<string>(busyTopicName, { value: string() })
		await client.createTopics([
			{ name: idleTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: busyTopicName, numPartitions: 1, replicationFactor: 1 },
		])

		const maxWaitMs = 5000
		const idle = client.consumer({ groupId: uniqueName('it-idle'), autoOffsetReset: 'earliest', maxWaitMs })
		const busy = client.consumer({ groupId: uniqueName('it-busy'), autoOffsetReset: 'earliest', maxWaitMs })
		const producer = client.producer({ lingerMs: 0 })

		const idleRun = idle.runEach(idleTopic, async () => {})
		let delivered: (() => void) | null = null
		const busyRun = busy.runEach(busyTopic, async () => {
			delivered?.()
		})
		await Promise.all(
			[idle, busy].map(consumer => new Promise<void>(resolve => consumer.once('running', () => resolve())))
		)

		try {
			const latencies: number[] = []
			for (let round = 0; round < 3; round++) {
				const received = new Promise<void>(resolve => {
					delivered = resolve
				})
				const sentAt = Date.now()
				await producer.send(busyTopic, [{ value: `m-${round}` }])
				await received
				latencies.push(Date.now() - sentAt)
			}
			// Behind the idle consumer's long poll a record would take up to maxWaitMs to arrive.
			expect(Math.max(...latencies)).toBeLessThan(maxWaitMs / 2)
		} finally {
			idle.stop()
			busy.stop()
			await Promise.allSettled([idleRun, busyRun])
			await producer.disconnect()
			await client.disconnect()
		}
	}, 60_000)
})
