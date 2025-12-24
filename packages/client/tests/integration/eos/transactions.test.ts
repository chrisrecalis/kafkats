import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { withKafka } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('EOS (integration) - transactions', () => {
	it('requires transaction() for transactional producers', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-tx-send-forbidden')
			await client.connect()

			const topicName = uniqueName('it-tx-send-forbidden')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({
				transactionalId: uniqueName('tx'),
				retries: 3,
				retryBackoffMs: 100,
			})

			await expect(producer.send(testTopic, { value: 'nope' })).rejects.toThrow(/transactional producer/i)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('aborts and rejects when transaction times out', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-tx-timeout')
			await client.connect()

			const producer = client.producer({
				transactionalId: uniqueName('tx'),
				retries: 10,
				retryBackoffMs: 250,
				maxRetryBackoffMs: 1000,
			})

			let signalAborted = false

			await expect(
				producer.transaction(
					async tx => {
						tx.signal.addEventListener('abort', () => {
							signalAborted = true
						})
						await sleep(500)
					},
					{ timeoutMs: 100 }
				)
			).rejects.toThrow(/Transaction timeout/i)

			expect(signalAborted).toBe(true)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('committed transactions are visible to read_committed consumers', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-tx-commit')
			await client.connect()

			const topicName = uniqueName('it-tx-commit')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({
				transactionalId: uniqueName('tx'),
				retries: 10,
				retryBackoffMs: 250,
				maxRetryBackoffMs: 1000,
			})

			await producer.transaction(async tx => {
				await tx.send(testTopic, { value: 'committed' })
			})

			const consumer = client.consumer({
				groupId: uniqueName('it-group'),
				autoOffsetReset: 'earliest',
				isolationLevel: 'read_committed',
			})

			const received: string[] = []
			await consumer.runEach(
				testTopic,
				async message => {
					received.push(message.value)
					consumer.stop()
				},
				{ autoCommit: false }
			)

			expect(received).toEqual(['committed'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('aborted transactions are hidden from read_committed consumers', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-tx-abort')
			await client.connect()

			const topicName = uniqueName('it-tx-abort')
			const testTopic = topic<string>(topicName, { value: codec.string() })

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({
				transactionalId: uniqueName('tx'),
				retries: 10,
				retryBackoffMs: 250,
				maxRetryBackoffMs: 1000,
			})

			await expect(
				producer.transaction(async tx => {
					await tx.send(testTopic, { value: 'aborted' })
					throw new Error('abort')
				})
			).rejects.toThrow()

			// read_uncommitted should see the aborted record
			const ru = client.consumer({
				groupId: uniqueName('it-group'),
				autoOffsetReset: 'earliest',
				isolationLevel: 'read_uncommitted',
			})

			const ruReceived: string[] = []
			await ru.runEach(
				testTopic,
				async message => {
					ruReceived.push(message.value)
					ru.stop()
				},
				{ autoCommit: false }
			)

			expect(ruReceived).toEqual(['aborted'])

			// read_committed should see nothing
			const rc = client.consumer({
				groupId: uniqueName('it-group'),
				autoOffsetReset: 'earliest',
				isolationLevel: 'read_committed',
			})

			const rcReceived: string[] = []
			const abortController = new AbortController()
			const rcRun = rc.runEach(
				testTopic,
				async message => {
					rcReceived.push(message.value)
					rc.stop()
				},
				{ autoCommit: false, signal: abortController.signal }
			)

			await new Promise<void>((resolve, reject) => {
				rc.once('running', () => resolve())
				rc.once('error', err => reject(err))
			})

			await sleep(1000)
			abortController.abort()
			await rcRun

			expect(rcReceived).toEqual([])

			await producer.disconnect()
			await client.disconnect()
		})
	})
})
