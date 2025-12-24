import { describe, expect, expectTypeOf, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - topic ergonomics', () => {
	it('runEach() accepts a string topic and defaults to Buffer values', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-consumer-string-topic-each')
			await client.connect()

			const topicName = uniqueName('it-consumer-string-topic-each')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(topicName, { value: Buffer.from('v', 'utf-8') })
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			let received: Buffer | null = null as Buffer | null

			await consumer.runEach(
				topicName,
				async message => {
					expectTypeOf(message.value).toEqualTypeOf<Buffer>()
					expect(Buffer.isBuffer(message.value)).toBe(true)
					received = message.value
					consumer.stop()
				},
				{ autoCommit: false }
			)

			expect(received?.toString('utf-8')).toBe('v')

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('runBatch() accepts a string topic and defaults to Buffer values', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-consumer-string-topic-batch')
			await client.connect()

			const topicName = uniqueName('it-consumer-string-topic-batch')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(topicName, [
				{ value: Buffer.from('a', 'utf-8') },
				{ value: Buffer.from('b', 'utf-8') },
				{ value: Buffer.from('c', 'utf-8') },
			])
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			const received: string[] = []

			await consumer.runBatch(
				topicName,
				async batch => {
					if (batch.length === 0) {
						return
					}

					const first = batch[0]
					if (!first) {
						return
					}

					expectTypeOf(first.value).toEqualTypeOf<Buffer>()

					for (const msg of batch) {
						expect(Buffer.isBuffer(msg.value)).toBe(true)
						received.push(msg.value.toString('utf-8'))
					}

					if (received.length >= 3) {
						consumer.stop()
					}
				},
				{ autoCommit: false, maxBatchSize: 3, maxBatchWaitMs: 1000 }
			)

			expect(received.slice(0, 3)).toEqual(['a', 'b', 'c'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('stream() accepts a string topic and defaults to Buffer values', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-consumer-string-topic-stream')
			await client.connect()

			const topicName = uniqueName('it-consumer-string-topic-stream')
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(topicName, [{ value: Buffer.from('x', 'utf-8') }, { value: Buffer.from('y', 'utf-8') }])
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })
			const received: string[] = []

			for await (const { message } of consumer.stream(topicName)) {
				expectTypeOf(message.value).toEqualTypeOf<Buffer>()
				expect(Buffer.isBuffer(message.value)).toBe(true)
				received.push(message.value.toString('utf-8'))
				if (received.length >= 2) {
					break
				}
			}

			expect(received).toEqual(['x', 'y'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('supports mixed topic definitions and string topics in one subscription', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-consumer-mixed-subscription')
			await client.connect()

			const bufferTopicName = uniqueName('it-consumer-buffer-topic')
			const stringTopicName = uniqueName('it-consumer-string-topic')
			const stringTopic = topic<string>(stringTopicName, {
				value: string(),
			})

			await client.createTopics([
				{ name: bufferTopicName, numPartitions: 1, replicationFactor: 1 },
				{ name: stringTopicName, numPartitions: 1, replicationFactor: 1 },
			])

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(bufferTopicName, { value: Buffer.from('buf', 'utf-8') })
			await producer.send(stringTopic, { value: 'str' })
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			let gotBuffer = false
			let gotString = false

			await consumer.runEach(
				[bufferTopicName, stringTopic] as const,
				async message => {
					expectTypeOf(message.value).toEqualTypeOf<Buffer | string>()

					if (message.topic === bufferTopicName) {
						expect(Buffer.isBuffer(message.value)).toBe(true)
						expect((message.value as Buffer).toString('utf-8')).toBe('buf')
						gotBuffer = true
					}
					if (message.topic === stringTopicName) {
						expect(typeof message.value).toBe('string')
						expect(message.value).toBe('str')
						gotString = true
					}

					if (gotBuffer && gotString) {
						consumer.stop()
					}
				},
				{ autoCommit: false }
			)

			expect(gotBuffer).toBe(true)
			expect(gotString).toBe(true)

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
