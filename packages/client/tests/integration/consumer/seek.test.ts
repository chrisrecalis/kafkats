import { describe, expect, it } from 'vitest'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Consumer (integration) - seek', () => {
	it('throws when seeking while not running', async () => {
		const client = createClient('it-seek-not-running')
		await client.connect()

		const groupId = uniqueName('it-group')
		const consumer = client.consumer({ groupId })

		expect(() => consumer.seek('some-topic', 0, 0n)).toThrow('Consumer is not running')

		await client.disconnect()
	})

	it('can seek during message processing without error', async () => {
		const client = createClient('it-seek-during-processing')
		await client.connect()

		const topic = uniqueName('seek-processing')
		const groupId = uniqueName('it-group')

		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		let seekCalled = false
		let messageCount = 0

		const runPromise = consumer.runEach(
			topic,
			async () => {
				messageCount++

				// Seek can be called during processing without throwing
				if (!seekCalled) {
					seekCalled = true
					// This should not throw - seek to beginning
					consumer.seek(topic, 0, 0n)
				}

				if (messageCount >= 2) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		try {
			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', reject)
			})

			await producer.send(topic, [{ value: Buffer.from('msg-0') }, { value: Buffer.from('msg-1') }])
			await producer.flush()

			await runPromise

			expect(seekCalled).toBe(true)
			expect(messageCount).toBeGreaterThanOrEqual(2)
		} finally {
			consumer.stop()
			await runPromise.catch(() => {})
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('handles seek on unassigned partition gracefully', async () => {
		const client = createClient('it-seek-unassigned')
		await client.connect()

		const topic = uniqueName('seek-unassigned')
		const groupId = uniqueName('it-group')

		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		const runPromise = consumer.runEach(
			topic,
			async () => {
				// Seek on partition 99 which is not assigned - should not throw
				consumer.seek(topic, 99, 0n)
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		try {
			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', reject)
			})

			await producer.send(topic, [{ value: Buffer.from('msg') }])
			await producer.flush()

			await runPromise
			// Test passes if no error was thrown
		} finally {
			consumer.stop()
			await runPromise.catch(() => {})
		}

		await producer.disconnect()
		await client.disconnect()
	})

	it('seek with pause/resume pattern works without error', async () => {
		const client = createClient('it-seek-pause-resume')
		await client.connect()

		const topic = uniqueName('seek-pause-resume')
		const groupId = uniqueName('it-group')

		await client.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

		let patternExecuted = false
		let messageCount = 0

		const runPromise = consumer.runEach(
			topic,
			async () => {
				messageCount++

				// Execute the pause/seek/resume pattern
				if (!patternExecuted) {
					patternExecuted = true
					consumer.pause([{ topic, partition: 0 }])
					consumer.seek(topic, 0, 0n) // Seek to beginning
					consumer.resume([{ topic, partition: 0 }])
				}

				if (messageCount >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		void runPromise.catch(() => {})

		try {
			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', reject)
			})

			await producer.send(topic, [
				{ value: Buffer.from('msg-0') },
				{ value: Buffer.from('msg-1') },
				{ value: Buffer.from('msg-2') },
			])
			await producer.flush()

			await runPromise

			expect(patternExecuted).toBe(true)
			// Due to seek back to 0, we may receive more messages
			expect(messageCount).toBeGreaterThanOrEqual(3)
		} finally {
			consumer.stop()
			await runPromise.catch(() => {})
		}

		await producer.disconnect()
		await client.disconnect()
	})
})
