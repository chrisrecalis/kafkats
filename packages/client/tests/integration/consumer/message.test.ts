import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - message shape', () => {
	it('round-trips key, headers, and timestamp', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-message-shape')
			await client.connect()

			const topicName = uniqueName('it-message-shape')
			const testTopic = topic<string, string>(topicName, {
				key: string(),
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			const sentAt = new Date('2020-01-01T00:00:00.000Z')

			await producer.send(testTopic, {
				key: 'k',
				value: 'v',
				headers: { h1: 'v1', h2: Buffer.from('v2', 'utf-8') },
				timestamp: sentAt,
			})
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			type Received = {
				key: string | null
				headers: Record<string, Buffer>
				timestamp: bigint
				value: string
			}

			let received: Received | null = null as Received | null

			await consumer.runEach(
				testTopic,
				async message => {
					received = {
						key: message.key,
						headers: message.headers,
						timestamp: message.timestamp,
						value: message.value,
					}
					consumer.stop()
				},
				{ autoCommit: false }
			)

			expect(received?.value).toBe('v')
			expect(received?.key).toBe('k')
			expect(received?.headers.h1?.toString('utf-8')).toBe('v1')
			expect(received?.headers.h2?.toString('utf-8')).toBe('v2')
			expect(received?.timestamp).toBe(BigInt(sentAt.getTime()))

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
