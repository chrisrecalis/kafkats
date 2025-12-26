import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Producer (integration) - compression', () => {
	it('compresses and decompresses messages with gzip', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-gzip-basic')
			await client.connect()

			const topicName = uniqueName('it-gzip-basic')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0, compression: 'gzip' })
			const consumer = client.consumer({ groupId: uniqueName('it-gzip-group'), autoOffsetReset: 'earliest' })

			const messages = Array.from({ length: 10 }, (_, i) => ({
				value: `message-${i}-${'x'.repeat(100)}`,
			}))

			await producer.send(testTopic, messages)
			await producer.flush()

			const received: string[] = []
			consumer.subscribe(testTopic)
			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 10) {
						consumer.stop()
					}
				},
				{ autoCommit: false }
			)
			void runPromise.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			await runPromise

			expect(received).toHaveLength(10)
			for (let i = 0; i < 10; i++) {
				expect(received[i]).toBe(`message-${i}-${'x'.repeat(100)}`)
			}

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('produces and consumes large compressed batches', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-gzip-large')
			await client.connect()

			const topicName = uniqueName('it-gzip-large')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0, compression: 'gzip' })
			const consumer = client.consumer({
				groupId: uniqueName('it-gzip-large-group'),
				autoOffsetReset: 'earliest',
			})

			// Create a 100KB payload - gzip should compress this well
			const largePayload = Buffer.alloc(100 * 1024, 'A')

			await producer.send(topicName, [{ value: largePayload }])
			await producer.flush()

			let receivedValue: Buffer | null = null
			consumer.subscribe(topicName)
			const runPromise = consumer.runEach(
				async message => {
					receivedValue = message.value as Buffer
					consumer.stop()
				},
				{ autoCommit: false }
			)
			void runPromise.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			await runPromise

			expect(receivedValue).not.toBeNull()
			expect(Buffer.isBuffer(receivedValue)).toBe(true)
			expect(receivedValue!.length).toBe(100 * 1024)
			expect(receivedValue!.equals(largePayload)).toBe(true)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('consumer reads gzip-compressed messages transparently', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-gzip-transparent')
			await client.connect()

			const topicName = uniqueName('it-gzip-transparent')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Producer uses gzip compression
			const producer = client.producer({ lingerMs: 0, compression: 'gzip' })

			// Consumer has no compression config - should decompress transparently
			const consumer = client.consumer({
				groupId: uniqueName('it-gzip-transparent-group'),
				autoOffsetReset: 'earliest',
			})

			const testData = 'Hello, gzip compressed world! '.repeat(50)
			await producer.send(testTopic, [{ value: testData }])
			await producer.flush()

			let received: string | null = null
			consumer.subscribe(testTopic)
			const runPromise = consumer.runEach(
				async message => {
					received = message.value as string
					consumer.stop()
				},
				{ autoCommit: false }
			)
			void runPromise.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			await runPromise

			expect(received).toBe(testData)

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
