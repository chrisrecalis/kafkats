import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe('Consumer (integration) - assign (manual partition assignment)', () => {
	it('consumes from a single manually assigned partition', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-single')
			await client.connect()

			const topicName = uniqueName('it-assign-single')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send messages to partition 1 specifically
			await producer.send(testTopic, [
				{ value: 'p1-a', partition: 1 },
				{ value: 'p1-b', partition: 1 },
				{ value: 'p1-c', partition: 1 },
			])
			await producer.flush()

			// Also send to partition 0 (should NOT be consumed)
			await producer.send(testTopic, [{ value: 'p0-a', partition: 0 }])
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			const received: string[] = []

			// Manually assign only partition 1
			consumer.assign([{ topic: topicName, partition: 1, offset: -1n }], { subscription: testTopic })

			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 3) {
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

			// Should only receive messages from partition 1
			expect(received).toEqual(['p1-a', 'p1-b', 'p1-c'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('starts from a specific offset when provided', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-offset')
			await client.connect()

			const topicName = uniqueName('it-assign-offset')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send 5 messages
			await producer.send(testTopic, [
				{ value: 'msg-0' },
				{ value: 'msg-1' },
				{ value: 'msg-2' },
				{ value: 'msg-3' },
				{ value: 'msg-4' },
			])
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			const received: string[] = []

			// Start from offset 2 (should skip msg-0 and msg-1)
			consumer.assign([{ topic: topicName, partition: 0, offset: 2n }], { subscription: testTopic })

			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 3) {
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

			// Should only receive messages from offset 2 onwards
			expect(received).toEqual(['msg-2', 'msg-3', 'msg-4'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('uses autoOffsetReset when offset is -1', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-auto-offset')
			await client.connect()

			const topicName = uniqueName('it-assign-auto-offset')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send messages before consumer starts
			await producer.send(testTopic, [{ value: 'before-1' }, { value: 'before-2' }])
			await producer.flush()

			// Consumer with autoOffsetReset: 'latest' should skip existing messages
			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'latest' })

			const received: string[] = []

			// Use -1n to use autoOffsetReset setting
			consumer.assign([{ topic: topicName, partition: 0, offset: -1n }], { subscription: testTopic })

			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 2) {
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

			// Send new messages after consumer starts (these should be received)
			await producer.send(testTopic, [{ value: 'after-1' }, { value: 'after-2' }])
			await producer.flush()

			await runPromise

			// Should only receive messages sent after consumer started (due to 'latest')
			expect(received).toEqual(['after-1', 'after-2'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('consumes from multiple manually assigned partitions', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-multi')
			await client.connect()

			const topicName = uniqueName('it-assign-multi')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send messages to all partitions
			await producer.send(testTopic, [
				{ value: 'p0-msg', partition: 0 },
				{ value: 'p1-msg', partition: 1 },
				{ value: 'p2-msg', partition: 2 },
				{ value: 'p3-msg', partition: 3 },
			])
			await producer.flush()

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			const received: string[] = []

			// Manually assign partitions 0 and 2 only (skip 1 and 3)
			consumer.assign(
				[
					{ topic: topicName, partition: 0, offset: -1n },
					{ topic: topicName, partition: 2, offset: -1n },
				],
				{ subscription: testTopic }
			)

			const runPromise = consumer.runEach(
				async message => {
					received.push(message.value as string)
					if (received.length >= 2) {
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

			// Should receive messages from partitions 0 and 2 only
			expect(received.sort()).toEqual(['p0-msg', 'p2-msg'])

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('does not trigger rebalance when another consumer joins with assign', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-no-rebalance')
			await client.connect()

			const topicName = uniqueName('it-assign-no-rebalance')
			const testTopic = topic<string>(topicName, {
				value: string(),
			})

			await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

			const producer = client.producer({ lingerMs: 0 })

			// Send messages to both partitions
			await producer.send(testTopic, [
				{ value: 'p0-1', partition: 0 },
				{ value: 'p0-2', partition: 0 },
				{ value: 'p1-1', partition: 1 },
				{ value: 'p1-2', partition: 1 },
			])
			await producer.flush()

			// Use different group IDs (or same - doesn't matter for assign)
			const consumer1 = client.consumer({ groupId: uniqueName('it-group-1'), autoOffsetReset: 'earliest' })
			const consumer2 = client.consumer({ groupId: uniqueName('it-group-2'), autoOffsetReset: 'earliest' })

			const received1: string[] = []
			const received2: string[] = []
			let rebalanceCount1 = 0
			let rebalanceCount2 = 0

			// Track rebalance events
			consumer1.on('rebalance', () => rebalanceCount1++)
			consumer2.on('rebalance', () => rebalanceCount2++)

			// Consumer 1 assigns partition 0
			consumer1.assign([{ topic: topicName, partition: 0, offset: -1n }], { subscription: testTopic })

			const run1 = consumer1.runEach(
				async message => {
					received1.push(message.value as string)
					if (received1.length >= 2) {
						consumer1.stop()
					}
				},
				{ autoCommit: false }
			)
			void run1.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer1.once('running', () => resolve())
				consumer1.once('error', err => reject(err))
			})

			// Consumer 2 assigns partition 1 (should NOT cause rebalance on consumer 1)
			consumer2.assign([{ topic: topicName, partition: 1, offset: -1n }], { subscription: testTopic })

			const run2 = consumer2.runEach(
				async message => {
					received2.push(message.value as string)
					if (received2.length >= 2) {
						consumer2.stop()
					}
				},
				{ autoCommit: false }
			)
			void run2.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer2.once('running', () => resolve())
				consumer2.once('error', err => reject(err))
			})

			await Promise.all([run1, run2])

			// Each consumer should receive its partition's messages
			expect(received1.sort()).toEqual(['p0-1', 'p0-2'])
			expect(received2.sort()).toEqual(['p1-1', 'p1-2'])

			// No rebalances should have occurred (manual assignment bypasses group protocol)
			expect(rebalanceCount1).toBe(0)
			expect(rebalanceCount2).toBe(0)

			await producer.disconnect()
			await client.disconnect()
		})
	})

	it('emits partitionsAssigned event with manual assignment', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-event')
			await client.connect()

			const topicName = uniqueName('it-assign-event')

			await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			let assignedPartitions: Array<{ topic: string; partition: number }> = []

			consumer.on('partitionsAssigned', partitions => {
				assignedPartitions = partitions
			})

			// Assign partitions 0 and 2
			consumer.assign([
				{ topic: topicName, partition: 0, offset: 0n },
				{ topic: topicName, partition: 2, offset: 0n },
			])

			const runPromise = consumer.runEach(async () => {}, { autoCommit: false })
			void runPromise.catch(() => {})

			await new Promise<void>((resolve, reject) => {
				consumer.once('running', () => resolve())
				consumer.once('error', err => reject(err))
			})

			// Stop immediately after checking
			consumer.stop()
			await runPromise

			// Should have received the assigned partitions
			expect(assignedPartitions).toHaveLength(2)
			expect(assignedPartitions.map(p => p.partition).sort()).toEqual([0, 2])

			await client.disconnect()
		})
	})

	it('throws when calling assign after subscribe', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-assign-after-subscribe')
			await client.connect()

			const topicName = uniqueName('it-assign-after-subscribe')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			// Subscribe first
			consumer.subscribe(topicName)

			// Assign should throw
			expect(() => {
				consumer.assign([{ topic: topicName, partition: 0, offset: 0n }])
			}).toThrow(/Cannot assign after subscribe/)

			await client.disconnect()
		})
	})

	it('throws when calling subscribe after assign', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('it-subscribe-after-assign')
			await client.connect()

			const topicName = uniqueName('it-subscribe-after-assign')

			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'earliest' })

			// Assign first
			consumer.assign([{ topic: topicName, partition: 0, offset: 0n }])

			// Subscribe should throw
			expect(() => {
				consumer.subscribe(topicName)
			}).toThrow(/Cannot subscribe after assign/)

			await client.disconnect()
		})
	})
})
