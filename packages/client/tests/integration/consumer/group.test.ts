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

	it('cooperative rebalance discards buffered records for revoked partitions', async () => {
		const client1 = createClient('it-coop-buffer-discard-1')
		const client2 = createClient('it-coop-buffer-discard-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-coop-buffer-discard')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client1.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		const producer = client1.producer({ lingerMs: 0 })

		const groupId = uniqueName('it-group')

		// Track which consumer processes which partition's messages
		const processedBy = new Map<string, string[]>() // message -> [consumer ids]

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
		let consumer1Revoked = false

		consumer1.on('partitionsAssigned', parts => {
			for (const p of parts) assigned1.add(p.partition)
		})
		consumer1.on('partitionsRevoked', parts => {
			consumer1Revoked = true
			for (const p of parts) assigned1.delete(p.partition)
		})

		consumer2.on('partitionsAssigned', parts => {
			for (const p of parts) assigned2.add(p.partition)
		})
		consumer2.on('partitionsRevoked', parts => {
			for (const p of parts) assigned2.delete(p.partition)
		})

		const recordProcessor = (consumerId: string) => async (message: { value: string; partition: number }) => {
			const key = `${message.value}`
			const consumers = processedBy.get(key) ?? []
			consumers.push(consumerId)
			processedBy.set(key, consumers)
		}

		// Start consumer1 - it will get all 4 partitions
		// Enable auto-commit so offsets are committed during rebalance
		const run1 = consumer1.runEach(testTopic, recordProcessor('consumer1'), { autoCommit: true })
		await waitUntilRunning(consumer1, run1)

		await vi.waitFor(
			() => {
				expect(assigned1.size).toBe(4)
			},
			{ timeout: 20_000 }
		)

		// Produce initial messages - consumer1 will process these
		await producer.send(testTopic, [
			{ value: 'before-p0', partition: 0 },
			{ value: 'before-p1', partition: 1 },
			{ value: 'before-p2', partition: 2 },
			{ value: 'before-p3', partition: 3 },
		])
		await producer.flush()

		// Wait for consumer1 to process initial messages
		await vi.waitFor(
			() => {
				expect(processedBy.size).toBe(4)
			},
			{ timeout: 10_000 }
		)

		// Produce more messages - these may be buffered by consumer1 when rebalance happens
		await producer.send(testTopic, [
			{ value: 'during-p0', partition: 0 },
			{ value: 'during-p1', partition: 1 },
			{ value: 'during-p2', partition: 2 },
			{ value: 'during-p3', partition: 3 },
		])
		await producer.flush()

		// Start consumer2 - triggers cooperative rebalance
		// Enable auto-commit so offsets are committed during rebalance
		const run2 = consumer2.runEach(testTopic, recordProcessor('consumer2'), { autoCommit: true })
		await waitUntilRunning(consumer2, run2)

		// Wait for rebalance to complete
		await vi.waitFor(
			() => {
				expect(consumer1Revoked).toBe(true)
				const union = new Set<number>([...assigned1, ...assigned2])
				const overlap = [...assigned1].filter(p => assigned2.has(p))
				expect(union.size).toBe(4)
				expect(overlap).toHaveLength(0)
				expect(assigned1.size).toBeGreaterThan(0)
				expect(assigned2.size).toBeGreaterThan(0)
			},
			{ timeout: 20_000 }
		)

		// Wait for all "during" messages to be processed
		await vi.waitFor(
			() => {
				expect(processedBy.has('during-p0')).toBe(true)
				expect(processedBy.has('during-p1')).toBe(true)
				expect(processedBy.has('during-p2')).toBe(true)
				expect(processedBy.has('during-p3')).toBe(true)
			},
			{ timeout: 15_000 }
		)

		consumer1.stop()
		consumer2.stop()
		await Promise.all([run1, run2])

		// Verify: each "during" message should be processed by exactly one consumer
		// With offset commits, rebalance should not cause duplicate processing
		for (let p = 0; p < 4; p++) {
			const msg = `during-p${p}`
			const consumers = processedBy.get(msg) ?? []

			// Should be processed exactly once (no duplicates from stale buffer or re-reading)
			expect(consumers).toHaveLength(1)
		}

		await producer.disconnect()
		await Promise.all([client1.disconnect(), client2.disconnect()])
	})

	it('poll/stream returns records only from owned partitions during cooperative rebalance', async () => {
		const client1 = createClient('it-coop-poll-rebalance-1')
		const client2 = createClient('it-coop-poll-rebalance-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-coop-poll-rebalance')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client1.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		const producer = client1.producer({ lingerMs: 0 })

		const groupId = uniqueName('it-group')

		// Track which consumer processes which partition's messages via stream/poll
		const processedBy = new Map<string, string[]>()

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
		let consumer1Revoked = false

		consumer1.on('partitionsAssigned', parts => {
			for (const p of parts) assigned1.add(p.partition)
		})
		consumer1.on('partitionsRevoked', parts => {
			consumer1Revoked = true
			for (const p of parts) assigned1.delete(p.partition)
		})

		consumer2.on('partitionsAssigned', parts => {
			for (const p of parts) assigned2.add(p.partition)
		})
		consumer2.on('partitionsRevoked', parts => {
			for (const p of parts) assigned2.delete(p.partition)
		})

		const recordProcessor = (consumerId: string, value: string) => {
			const consumers = processedBy.get(value) ?? []
			consumers.push(consumerId)
			processedBy.set(value, consumers)
		}

		// Start consumer1's stream - it will get all 4 partitions initially
		// Enable offset commits so offsets are committed during rebalance
		const stream1Promise = (async () => {
			for await (const { message } of consumer1.stream(testTopic, { commitOffsets: true })) {
				recordProcessor('consumer1', message.value)
				// Stop when we've processed all 'after-' messages from our partitions
				const afterCount = [...processedBy.entries()].filter(
					([k, v]) => k.startsWith('after-') && v.includes('consumer1')
				).length
				if (afterCount >= assigned1.size && consumer1Revoked) {
					break
				}
			}
		})()

		// Wait for consumer1 to be assigned all partitions
		await vi.waitFor(
			() => {
				expect(assigned1.size).toBe(4)
			},
			{ timeout: 20_000 }
		)

		// Produce initial messages - consumer1 will buffer/process these
		await producer.send(testTopic, [
			{ value: 'before-p0', partition: 0 },
			{ value: 'before-p1', partition: 1 },
			{ value: 'before-p2', partition: 2 },
			{ value: 'before-p3', partition: 3 },
		])
		await producer.flush()

		// Wait for consumer1 to process initial messages
		await vi.waitFor(
			() => {
				expect(processedBy.size).toBe(4)
			},
			{ timeout: 10_000 }
		)

		// Produce more messages that may be buffered when rebalance happens
		await producer.send(testTopic, [
			{ value: 'during-p0', partition: 0 },
			{ value: 'during-p1', partition: 1 },
			{ value: 'during-p2', partition: 2 },
			{ value: 'during-p3', partition: 3 },
		])
		await producer.flush()

		// Start consumer2's stream - triggers cooperative rebalance
		// Enable offset commits so offsets are committed during rebalance
		const stream2Promise = (async () => {
			for await (const { message } of consumer2.stream(testTopic, { commitOffsets: true })) {
				recordProcessor('consumer2', message.value)
				// Stop when we've processed all 'after-' messages from our partitions
				const afterCount = [...processedBy.entries()].filter(
					([k, v]) => k.startsWith('after-') && v.includes('consumer2')
				).length
				if (afterCount >= assigned2.size) {
					break
				}
			}
		})()

		// Wait for rebalance to complete
		await vi.waitFor(
			() => {
				expect(consumer1Revoked).toBe(true)
				const union = new Set<number>([...assigned1, ...assigned2])
				const overlap = [...assigned1].filter(p => assigned2.has(p))
				expect(union.size).toBe(4)
				expect(overlap).toHaveLength(0)
				expect(assigned1.size).toBeGreaterThan(0)
				expect(assigned2.size).toBeGreaterThan(0)
			},
			{ timeout: 20_000 }
		)

		// Wait for all "during" messages to be processed
		await vi.waitFor(
			() => {
				expect(processedBy.has('during-p0')).toBe(true)
				expect(processedBy.has('during-p1')).toBe(true)
				expect(processedBy.has('during-p2')).toBe(true)
				expect(processedBy.has('during-p3')).toBe(true)
			},
			{ timeout: 15_000 }
		)

		// Produce "after" messages to allow streams to complete
		await producer.send(testTopic, [
			{ value: 'after-p0', partition: 0 },
			{ value: 'after-p1', partition: 1 },
			{ value: 'after-p2', partition: 2 },
			{ value: 'after-p3', partition: 3 },
		])
		await producer.flush()

		// Wait for streams to complete
		await Promise.all([stream1Promise, stream2Promise])

		// Verify: each "during" message should be processed by exactly one consumer
		// With offset commits, rebalance should not cause duplicate processing
		for (let p = 0; p < 4; p++) {
			const msg = `during-p${p}`
			const consumers = processedBy.get(msg) ?? []

			// Should be processed exactly once (no duplicates from stale buffer via poll)
			expect(consumers).toHaveLength(1)
		}

		await producer.disconnect()
		await Promise.all([client1.disconnect(), client2.disconnect()])
	})
})
