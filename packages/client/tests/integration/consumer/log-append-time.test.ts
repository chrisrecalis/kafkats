import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - LogAppendTime', () => {
	it('reports the broker append time, not the producer CreateTime', async () => {
		const client = createClient('it-log-append-time')
		await client.connect()

		const topicName = uniqueName('it-log-append-time')
		const testTopic = topic<string, string>(topicName, {
			key: string(),
			value: string(),
		})

		await client.createTopics([
			{
				name: topicName,
				numPartitions: 1,
				replicationFactor: 1,
				configs: { 'message.timestamp.type': 'LogAppendTime' },
			},
		])

		const producer = client.producer({ lingerMs: 0 })

		// A CreateTime far in the past. Under LogAppendTime the broker discards it and stamps every
		// record with the batch append time, so a correct consumer must report the append time and
		// never this value.
		const staleCreateTime = new Date('2000-01-01T00:00:00.000Z')

		const before = Date.now()
		await producer.send(testTopic, { key: 'k', value: 'v', timestamp: staleCreateTime })
		await producer.flush()
		const after = Date.now()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

		let timestamp: bigint | null = null

		await consumer.runEach(
			testTopic,
			async message => {
				timestamp = message.timestamp
				consumer.stop()
			},
			{ autoCommit: false }
		)

		// The append time the broker assigned, within the send window — not the stale CreateTime.
		expect(timestamp).not.toBe(BigInt(staleCreateTime.getTime()))
		expect(timestamp).toBeGreaterThanOrEqual(BigInt(before))
		expect(timestamp).toBeLessThanOrEqual(BigInt(after))

		await producer.disconnect()
		await client.disconnect()
	})
})
