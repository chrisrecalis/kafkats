import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - stream', () => {
	it('streams messages via async iterator', async () => {
		const client = createClient('it-stream')
		await client.connect()

		const topicName = uniqueName('it-stream')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'a' }, { value: 'b' }, { value: 'c' }])
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })
		const received: string[] = []

		for await (const { message } of consumer.stream(testTopic)) {
			received.push(message.value)
			if (received.length >= 3) {
				break
			}
		}

		expect(received).toEqual(['a', 'b', 'c'])

		await producer.disconnect()
		await client.disconnect()
	})
})
