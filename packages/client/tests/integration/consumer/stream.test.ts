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

	it('supports manual partition assignment without group coordination', async () => {
		const client = createClient('it-stream-manual-assign')
		await client.connect()

		const topicName = uniqueName('it-stream-manual-assign')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'x' }])
		await producer.flush()

		const groupId = uniqueName('it-group')
		// Two consumers with same groupId but manual assignment - both should see the message
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const seen1: string[] = []
		const seen2: string[] = []

		// Start both consumers with manual assignment to the same partition
		const stream1 = consumer1.stream(testTopic, {
			commitOffsets: false,
			assignment: [{ topic: topicName, partition: 0 }],
		})

		const stream2 = consumer2.stream(testTopic, {
			commitOffsets: false,
			assignment: [{ topic: topicName, partition: 0 }],
		})

		// Consume from both streams
		for await (const { message } of stream1) {
			seen1.push(message.value)
			break
		}

		for await (const { message } of stream2) {
			seen2.push(message.value)
			break
		}

		// Both consumers should see the same message since they're manually assigned
		expect(seen1).toEqual(['x'])
		expect(seen2).toEqual(['x'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('supports manual assignment with specific starting offset', async () => {
		const client = createClient('it-stream-manual-offset')
		await client.connect()

		const topicName = uniqueName('it-stream-manual-offset')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'a' }, { value: 'b' }, { value: 'c' }])
		await producer.flush()

		const groupId = uniqueName('it-group')
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const received: string[] = []

		// Start from offset 1 (skipping 'a')
		for await (const { message } of consumer.stream(testTopic, {
			commitOffsets: false,
			assignment: [{ topic: topicName, partition: 0, offset: 1n }],
		})) {
			received.push(message.value)
			if (received.length >= 2) {
				break
			}
		}

		// Should only see 'b' and 'c' since we started at offset 1
		expect(received).toEqual(['b', 'c'])

		await producer.disconnect()
		await client.disconnect()
	})
})
