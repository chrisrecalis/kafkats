import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - lifecycle', () => {
	it('rejects send() after disconnect', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-producer-after-disconnect')
			await client.connect()

			const topicName = uniqueName('it-producer-after-disconnect')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			await producer.disconnect()

			await expect(producer.send(testTopic, { value: 'x' })).rejects.toThrow(/Producer is not running/)

			await client.disconnect()
		})
	})

	it('validates idempotent producer configuration', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-producer-idempotent-config')
			await client.connect()

			expect(() => client.producer({ idempotent: true, acks: 'leader' })).toThrow(/acks="all"/i)
			expect(() => client.producer({ idempotent: true, retries: 0 })).toThrow(/retries >= 1/i)
			expect(() => client.producer({ idempotent: true, maxInFlight: 6 })).toThrow(/maxInFlight <= 5/i)

			await client.disconnect()
		})
	})
})
