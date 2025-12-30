import { describe, expect, it, vi } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

function waitUntilRunning(
	consumer: {
		once(event: 'running', cb: () => void): unknown
		once(event: 'error', cb: (err: Error) => void): unknown
	},
	run: Promise<void>
): Promise<void> {
	return Promise.race([
		new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		}),
		run.then(
			() => {
				throw new Error('consumer stopped before reaching running state')
			},
			err => {
				throw err
			}
		),
	])
}

describe.concurrent('Consumer (integration) - groups', () => {
	it('distributes partitions across consumers in the same group', async () => {
		const client1 = createClient('it-group-two-consumers-1')
		const client2 = createClient('it-group-two-consumers-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-group-two-consumers')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client1.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

		const producer = client1.producer({ lingerMs: 0 })

		const groupId = uniqueName('it-group')
		const consumer1 = client1.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
		})
		const consumer2 = client2.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
		})

		let assignment1: number[] = []
		let assignment2: number[] = []
		consumer1.on('partitionsAssigned', parts => {
			assignment1 = parts.map(p => p.partition).sort((a, b) => a - b)
		})
		consumer2.on('partitionsAssigned', parts => {
			assignment2 = parts.map(p => p.partition).sort((a, b) => a - b)
		})

		const consumed1: Array<{ partition: number; value: string }> = []
		const consumed2: Array<{ partition: number; value: string }> = []
		const seenPartitions = new Set<number>()

		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				consumed1.push({ partition: message.partition, value: message.value })
				seenPartitions.add(message.partition)
			},
			{ autoCommit: false }
		)

		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				consumed2.push({ partition: message.partition, value: message.value })
				seenPartitions.add(message.partition)
			},
			{ autoCommit: false }
		)

		await Promise.all([waitUntilRunning(consumer1, run1), waitUntilRunning(consumer2, run2)])

		await vi.waitFor(
			() => {
				expect(assignment1.length + assignment2.length).toBe(4)
			},
			{ timeout: 10_000 }
		)

		await producer.send(testTopic, [
			{ value: 'p0', partition: 0 },
			{ value: 'p1', partition: 1 },
			{ value: 'p2', partition: 2 },
			{ value: 'p3', partition: 3 },
		])
		await producer.flush()

		await vi.waitFor(
			() => {
				expect(seenPartitions.size).toBe(4)
			},
			{ timeout: 30_000 }
		)

		consumer1.stop()
		consumer2.stop()
		await Promise.all([run1, run2])

		const partitions1 = new Set(consumed1.map(m => m.partition))
		const partitions2 = new Set(consumed2.map(m => m.partition))
		const allPartitions = new Set([...partitions1, ...partitions2])

		// No overlap: each partition should be owned by exactly one consumer
		for (const p of partitions1) {
			expect(partitions2.has(p)).toBe(false)
		}

		expect(allPartitions).toEqual(new Set([0, 1, 2, 3]))

		// Each partition should deliver the expected message
		const allValues = new Map([...consumed1, ...consumed2].map(m => [m.partition, m.value]))
		expect(allValues.get(0)).toBe('p0')
		expect(allValues.get(1)).toBe('p1')
		expect(allValues.get(2)).toBe('p2')
		expect(allValues.get(3)).toBe('p3')

		await producer.disconnect()
		await Promise.all([client1.disconnect(), client2.disconnect()])
	})

	it('eager rebalance revokes all partitions (range)', async () => {
		const client1 = createClient('it-eager-rebalance-1')
		const client2 = createClient('it-eager-rebalance-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-eager-rebalance')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client1.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

		const groupId = uniqueName('it-group')

		const consumer1 = client1.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
			sessionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 20_000,
			heartbeatIntervalMs: 1000,
		})

		const consumer2 = client2.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
			sessionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 20_000,
			heartbeatIntervalMs: 1000,
		})

		const assigned1 = new Set<number>()
		const assigned2 = new Set<number>()
		let firstRevokedCount: number | null = null

		consumer1.on('partitionsAssigned', parts => {
			for (const p of parts) assigned1.add(p.partition)
		})
		consumer1.on('partitionsRevoked', parts => {
			if (firstRevokedCount === null) {
				firstRevokedCount = parts.length
			}
			for (const p of parts) assigned1.delete(p.partition)
		})

		consumer2.on('partitionsAssigned', parts => {
			for (const p of parts) assigned2.add(p.partition)
		})
		consumer2.on('partitionsRevoked', parts => {
			for (const p of parts) assigned2.delete(p.partition)
		})

		const run1 = consumer1.runEach(testTopic, async () => {}, { autoCommit: false })
		await waitUntilRunning(consumer1, run1)
		await vi.waitFor(
			() => {
				expect(assigned1.size).toBe(4)
			},
			{ timeout: 20_000 }
		)

		const run2 = consumer2.runEach(testTopic, async () => {}, { autoCommit: false })
		await waitUntilRunning(consumer2, run2)

		await vi.waitFor(
			() => {
				expect(firstRevokedCount).toBe(4)
			},
			{ timeout: 20_000 }
		)

		await vi.waitFor(
			() => {
				const union = new Set<number>([...assigned1, ...assigned2])
				const overlap = [...assigned1].filter(p => assigned2.has(p))
				expect(union.size).toBe(4)
				expect(overlap).toHaveLength(0)
				expect(assigned1.size).toBeGreaterThan(0)
				expect(assigned2.size).toBeGreaterThan(0)
			},
			{ timeout: 20_000 }
		)

		consumer1.stop()
		consumer2.stop()
		await Promise.all([run1, run2])

		await Promise.all([client1.disconnect(), client2.disconnect()])
	})

	it('cooperative rebalance revokes only moved partitions (cooperative-sticky)', async () => {
		const client1 = createClient('it-cooperative-rebalance-1')
		const client2 = createClient('it-cooperative-rebalance-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-cooperative-rebalance')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client1.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		const producer = client1.producer({ lingerMs: 0 })

		const groupId = uniqueName('it-group')

		const consumer1 = client1.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'cooperative-sticky',
			sessionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 20_000,
			heartbeatIntervalMs: 1000,
		})

		const consumer2 = client2.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'cooperative-sticky',
			sessionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 20_000,
			heartbeatIntervalMs: 1000,
		})

		const assigned1 = new Set<number>()
		const assigned2 = new Set<number>()
		let firstRevoked: number[] | null = null

		consumer1.on('partitionsAssigned', parts => {
			for (const p of parts) assigned1.add(p.partition)
		})
		consumer1.on('partitionsRevoked', parts => {
			if (firstRevoked === null) {
				firstRevoked = parts.map(p => p.partition).sort((a, b) => a - b)
			}
			for (const p of parts) assigned1.delete(p.partition)
		})

		consumer2.on('partitionsAssigned', parts => {
			for (const p of parts) assigned2.add(p.partition)
		})
		consumer2.on('partitionsRevoked', parts => {
			for (const p of parts) assigned2.delete(p.partition)
		})

		const afterByPartition = new Map<number, string>()
		const stopIfDone = () => {
			if (afterByPartition.size >= 4) {
				consumer1.stop()
				consumer2.stop()
			}
		}

		const run1 = consumer1.runEach(
			testTopic,
			async message => {
				if (!message.value.startsWith('after-')) {
					return
				}
				if (afterByPartition.has(message.partition)) {
					throw new Error(`duplicate after-message for partition ${message.partition}`)
				}
				afterByPartition.set(message.partition, message.value)
				stopIfDone()
			},
			{ autoCommit: false }
		)

		await waitUntilRunning(consumer1, run1)
		await vi.waitFor(
			() => {
				expect(assigned1.size).toBe(4)
			},
			{ timeout: 20_000 }
		)

		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				if (!message.value.startsWith('after-')) {
					return
				}
				if (afterByPartition.has(message.partition)) {
					throw new Error(`duplicate after-message for partition ${message.partition}`)
				}
				afterByPartition.set(message.partition, message.value)
				stopIfDone()
			},
			{ autoCommit: false }
		)

		await waitUntilRunning(consumer2, run2)

		await vi.waitFor(
			() => {
				expect(firstRevoked).not.toBeNull()
				expect(firstRevoked!.length).toBeGreaterThan(0)
				expect(firstRevoked!.length).toBeLessThan(4)
			},
			{ timeout: 20_000 }
		)

		await vi.waitFor(
			() => {
				const union = new Set<number>([...assigned1, ...assigned2])
				const overlap = [...assigned1].filter(p => assigned2.has(p))
				expect(union.size).toBe(4)
				expect(overlap).toHaveLength(0)
				expect(assigned1.size).toBeGreaterThan(0)
				expect(assigned2.size).toBeGreaterThan(0)
			},
			{ timeout: 20_000 }
		)

		await producer.send(testTopic, [
			{ value: 'after-p0', partition: 0 },
			{ value: 'after-p1', partition: 1 },
			{ value: 'after-p2', partition: 2 },
			{ value: 'after-p3', partition: 3 },
		])
		await producer.flush()

		await Promise.all([run1, run2])

		expect(afterByPartition.get(0)).toBe('after-p0')
		expect(afterByPartition.get(1)).toBe('after-p1')
		expect(afterByPartition.get(2)).toBe('after-p2')
		expect(afterByPartition.get(3)).toBe('after-p3')

		await producer.disconnect()
		await Promise.all([client1.disconnect(), client2.disconnect()])
	})
})
