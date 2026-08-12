import { describe, expect, it } from 'vitest'

import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

describe.concurrent('EOS (integration) - transactions', () => {
	it('requires transaction() for transactional producers', async () => {
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

	it('aborts and rejects when transaction times out', async () => {
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

	it('committed transactions are visible to read_committed consumers', async () => {
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

	it('commits consumed offsets with the membership carried by ConsumeContext', async () => {
		const client = createClient('it-tx-consume-context')
		await client.connect()

		const inputName = uniqueName('tx-context-input')
		const outputName = uniqueName('tx-context-output')
		const input = topic<string>(inputName, { value: codec.string() })
		const output = topic<string>(outputName, { value: codec.string() })
		const groupId = uniqueName('tx-context-group')

		await client.createTopics([
			{ name: inputName, numPartitions: 1, replicationFactor: 1 },
			{ name: outputName, numPartitions: 1, replicationFactor: 1 },
		])

		const seedProducer = client.producer()
		await seedProducer.send(input, { value: 'one' })

		const transactionProducer = client.producer({
			transactionalId: uniqueName('tx-context'),
			retries: 10,
			retryBackoffMs: 250,
			maxRetryBackoffMs: 1000,
		})
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		await consumer.runEach(
			input,
			async (message, context) => {
				expect(context.groupId).toBe(groupId)
				expect(context.consumerGroupMetadata).toEqual(
					expect.objectContaining({
						groupId,
						generationId: expect.any(Number),
						memberId: expect.any(String),
					})
				)
				await transactionProducer.transaction(async tx => {
					await tx.send(output, { value: `processed-${message.value}` })
					await tx.sendOffsets(context)
				})
				consumer.stop()
			},
			{ autoCommit: false, commitOffsets: false }
		)

		const outputConsumer = client.consumer({
			groupId: uniqueName('tx-context-output-group'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
		})
		const outputValues: string[] = []
		await outputConsumer.runEach(
			output,
			async message => {
				outputValues.push(message.value)
				outputConsumer.stop()
			},
			{ autoCommit: false }
		)
		expect(outputValues).toEqual(['processed-one'])

		const resumedConsumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const replayedValues: string[] = []
		const abortController = new AbortController()
		const resumedRun = resumedConsumer.runEach(
			input,
			async message => {
				replayedValues.push(message.value)
			},
			{ autoCommit: false, signal: abortController.signal }
		)
		await new Promise<void>((resolve, reject) => {
			resumedConsumer.once('running', resolve)
			resumedConsumer.once('error', reject)
		})
		await sleep(500)
		abortController.abort()
		await resumedRun
		expect(replayedValues).toEqual([])

		await seedProducer.disconnect()
		await transactionProducer.disconnect()
		await client.disconnect()
	})

	it('aborts instead of committing when a transactional send fails definitively', async () => {
		const client = createClient('it-tx-failed-send')
		await client.connect()

		const topicName = uniqueName('it-tx-failed-send')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({
			transactionalId: uniqueName('tx'),
			retries: 10,
			retryBackoffMs: 250,
			maxRetryBackoffMs: 1000,
		})

		// Larger than the broker default message.max.bytes (~1MiB) — the broker rejects
		// the batch with MessageTooLarge, a definitive non-retriable error.
		const oversized = 'x'.repeat(2 * 1024 * 1024)

		await expect(
			producer.transaction(async tx => {
				await tx.send(testTopic, { value: 'normal' })
				// Fire-and-forget: the failure must still poison the transaction so
				// commit refuses to expose the partial transaction.
				void tx.send(testTopic, { value: oversized }).catch(() => {})
			})
		).rejects.toThrow(/Transaction aborted/i)

		// read_committed must see NO records from the aborted transaction.
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

	it('aborted transactions are hidden from read_committed consumers', async () => {
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

	it('runs parallel transactions when transactionConcurrency > 1', async () => {
		const client = createClient('it-tx-concurrency')
		await client.connect()

		const topicName = uniqueName('it-tx-concurrency')
		const testTopic = topic<string>(topicName, { value: codec.string() })

		await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

		const producer = client.producer({
			transactionalId: uniqueName('tx'),
			transactionConcurrency: 3,
			retries: 10,
			retryBackoffMs: 250,
			maxRetryBackoffMs: 1000,
		})

		// Hold one transaction open while another begins and commits. With a
		// single transactional ID the second call would queue forever behind the
		// parked one and the test would time out.
		let releaseParked!: () => void
		const parkedGate = new Promise<void>(resolve => {
			releaseParked = resolve
		})

		const parked = producer.transaction(async tx => {
			await tx.send(testTopic, { value: 'parked' })
			await parkedGate
		})

		await producer.transaction(async tx => {
			await tx.send(testTopic, { value: 'overlapping' })
		})

		releaseParked()
		await parked

		// An aborted transaction on one lane stays invisible and leaves the pool usable.
		await expect(
			producer.transaction(async tx => {
				await tx.send(testTopic, { value: 'aborted' })
				throw new Error('user abort')
			})
		).rejects.toThrow()

		// A burst beyond the pool size: callers queue FIFO and all commit.
		await Promise.all(
			Array.from({ length: 10 }, (_, i) =>
				producer.transaction(async tx => {
					await tx.send(testTopic, { value: `burst-${i}` })
				})
			)
		)

		const expected = ['parked', 'overlapping', ...Array.from({ length: 10 }, (_, i) => `burst-${i}`)]

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
				if (received.length >= expected.length) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)

		expect([...received].sort()).toEqual([...expected].sort())
		expect(received).not.toContain('aborted')

		await producer.disconnect()
		await client.disconnect()
	})
})
