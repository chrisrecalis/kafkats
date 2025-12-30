import { describe, expect, it, vi } from 'vitest'

import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - static membership', () => {
	it('replaces a crashed member with the same groupInstanceId without waiting for session timeout', async () => {
		const groupId = uniqueName('it-group')
		const instanceId = uniqueName('it-instance')

		const clientOld = createClient('it-static-old')
		const clientNew = createClient('it-static-new')
		await clientOld.connect()
		await clientNew.connect()

		const topicName = uniqueName('it-static-restart')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})
		await clientNew.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = clientNew.producer({ lingerMs: 0 })

		const oldConsumer = clientOld.consumer({
			groupId,
			groupInstanceId: instanceId,
			autoOffsetReset: 'earliest',
		})

		const oldSeen: string[] = []
		const oldRun = oldConsumer.runEach(
			testTopic,
			async message => {
				oldSeen.push(message.value)
			},
			{ autoCommit: false }
		)
		void oldRun.catch(() => {})

		let oldRunning = false
		let oldError: Error | null = null
		oldConsumer.once('running', () => {
			oldRunning = true
		})
		oldConsumer.once('error', err => {
			oldError = err
		})

		await vi.waitFor(
			() => {
				if (oldError) throw oldError
				expect(oldRunning).toBe(true)
			},
			{ timeout: 30_000 }
		)

		await producer.send(testTopic, { value: 'm-0' })
		await producer.flush()

		await vi.waitFor(
			() => {
				expect(oldSeen).toContain('m-0')
			},
			{ timeout: 30_000 }
		)

		// Simulate a crash: stop the consumer first to prevent fetch loop, then sever the network.
		// With static membership, the broker keeps the membership alive for the session timeout.
		oldConsumer.stop()
		await oldRun.catch(() => {})
		await clientOld.disconnect()

		const newConsumer = clientNew.consumer({
			groupId,
			groupInstanceId: instanceId,
			autoOffsetReset: 'earliest',
		})

		const newSeen: string[] = []
		const newRun = newConsumer.runEach(
			testTopic,
			async message => {
				newSeen.push(message.value)
				if (message.value === 'm-1') {
					newConsumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void newRun.catch(() => {})

		let newRunning = false
		let newError: Error | null = null
		newConsumer.once('running', () => {
			newRunning = true
		})
		newConsumer.once('error', err => {
			newError = err
		})

		// If static membership replacement doesn't work, this join can stall until the old member times out.
		await vi.waitFor(
			() => {
				if (newError) throw newError
				expect(newRunning).toBe(true)
			},
			{ timeout: 10_000 }
		)

		await producer.send(testTopic, { value: 'm-1' })
		await producer.flush()

		await vi.waitFor(
			() => {
				expect(newSeen).toContain('m-1')
			},
			{ timeout: 30_000 }
		)

		await newRun

		await producer.disconnect()
		await clientNew.disconnect()
	})

	it('fences the old consumer when a second consumer joins with the same groupInstanceId', async () => {
		const groupId = uniqueName('it-group')
		const instanceId = uniqueName('it-instance')

		const clientA = createClient('it-static-a')
		const clientB = createClient('it-static-b')
		await clientA.connect()
		await clientB.connect()

		const topicName = uniqueName('it-static-fence')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})
		await clientA.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = clientA.producer({ lingerMs: 0 })

		const consumerA = clientA.consumer({
			groupId,
			groupInstanceId: instanceId,
			autoOffsetReset: 'earliest',
			heartbeatIntervalMs: 500,
		})

		const aSeen: string[] = []
		let aLost: Array<{ topic: string; partition: number }> | null = null
		let aError: Error | null = null

		consumerA.on('partitionsLost', partitions => {
			aLost = partitions
		})
		consumerA.on('error', err => {
			aError = err
		})

		const runA = consumerA.runEach(
			testTopic,
			async message => {
				aSeen.push(message.value)
			},
			{ autoCommit: false }
		)
		void runA.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumerA.once('running', () => resolve())
			consumerA.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'a-0' })
		await producer.flush()

		await vi.waitFor(
			() => {
				expect(aSeen).toContain('a-0')
			},
			{ timeout: 30_000 }
		)

		const consumerB = clientB.consumer({
			groupId,
			groupInstanceId: instanceId,
			autoOffsetReset: 'earliest',
			heartbeatIntervalMs: 500,
		})

		const bSeen: string[] = []
		const runB = consumerB.runEach(
			testTopic,
			async message => {
				bSeen.push(message.value)
				if (message.value === 'b-0') {
					consumerB.stop()
				}
			},
			{ autoCommit: false }
		)
		void runB.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumerB.once('running', () => resolve())
			consumerB.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'b-0' })
		await producer.flush()

		await vi.waitFor(
			() => {
				expect(bSeen).toContain('b-0')
			},
			{ timeout: 30_000 }
		)

		// The original consumer should be fenced and lose its partitions.
		await vi.waitFor(
			() => {
				expect(aLost).not.toBeNull()
				expect(aLost).toContainEqual({ topic: topicName, partition: 0 })
			},
			{ timeout: 30_000 }
		)

		await vi.waitFor(
			() => {
				expect(aError).not.toBeNull()
				expect(aError).toBeInstanceOf(KafkaProtocolError)
				expect((aError as KafkaProtocolError).errorCode).toBe(ErrorCode.FencedInstanceId)
			},
			{ timeout: 30_000 }
		)

		consumerA.stop()
		await runA.catch(() => {})

		await runB

		await producer.disconnect()
		await clientA.disconnect()
		await clientB.disconnect()
	})
})
