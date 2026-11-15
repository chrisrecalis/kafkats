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

describe.concurrent('Consumer (integration) - offsets', () => {
	it('commits offsets and resumes from last committed', async () => {
		const client = createClient('it-offset-commit')
		await client.connect()

		const topicName = uniqueName('it-offset-commit')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 5 }, (_, i) => ({ value: `m-${i}` }))
		)
		await producer.flush()

		const groupId = uniqueName('it-group')

		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received1: string[] = []
		await consumer1.runEach(
			testTopic,
			async message => {
				received1.push(message.value)
				if (received1.length >= 5) {
					consumer1.stop()
				}
			},
			{ autoCommit: true, autoCommitIntervalMs: 200 }
		)

		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received2: string[] = []
		const run2 = consumer2.runEach(
			testTopic,
			async message => {
				received2.push(message.value)
				consumer2.stop()
			},
			{ autoCommit: false }
		)
		void run2.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer2.once('running', () => resolve())
			consumer2.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'm-5' })
		await producer.flush()

		await run2

		expect(received1).toEqual(['m-0', 'm-1', 'm-2', 'm-3', 'm-4'])
		expect(received2).toEqual(['m-5'])

		await producer.disconnect()
		await client.disconnect()
	})

	it("autoOffsetReset='latest' starts at end for new group", async () => {
		const client = createClient('it-offset-latest')
		await client.connect()

		const topicName = uniqueName('it-offset-latest')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(testTopic, [{ value: 'old-1' }, { value: 'old-2' }])
		await producer.flush()

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'latest' })
		const received: string[] = []

		const run = consumer.runEach(
			testTopic,
			async message => {
				received.push(message.value)
				consumer.stop()
			},
			{ autoCommit: false }
		)
		void run.catch(() => {})

		await new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		})

		await producer.send(testTopic, { value: 'new-1' })
		await producer.flush()

		await run

		expect(received).toEqual(['new-1'])

		await producer.disconnect()
		await client.disconnect()
	})

	it("autoOffsetReset='none' throws when no committed offset exists", async () => {
		const client = createClient('it-offset-none')
		await client.connect()

		const topicName = uniqueName('it-offset-none')
		const testTopic = topic<string>(topicName, {
			value: string(),
		})

		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const consumer = client.consumer({ groupId: uniqueName('it-group'), autoOffsetReset: 'none' })

		await expect(
			consumer.runEach(
				testTopic,
				async () => {
					// Should never be called
				},
				{ autoCommit: false }
			)
		).rejects.toThrow(/No committed offset/)

		await client.disconnect()
	})

	it('commits revoked partitions before releasing them (no reprocessing after rebalance)', async () => {
		const client1 = createClient('it-revoke-commit-1')
		const client2 = createClient('it-revoke-commit-2')
		await Promise.all([client1.connect(), client2.connect()])

		const topicName = uniqueName('it-revoke-commit')
		const testTopic = topic<string>(topicName, { value: string() })

		await client1.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client1.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 5 }, (_, i) => ({ value: `m-${i}` }))
		)
		await producer.flush()

		const groupId = uniqueName('it-revoke-commit-group')
		// Huge auto-commit interval so the periodic timer never fires: the only commit
		// that can persist offsets is the one fired when partition 0 is revoked during
		// the rebalance triggered by the second consumer joining.
		const consumerOpts = {
			groupId,
			autoOffsetReset: 'earliest' as const,
			partitionAssignmentStrategy: 'range' as const,
		}
		const runOpts = { autoCommit: true, autoCommitIntervalMs: 600_000 }

		// Aggregated across both consumers; duplicates are allowed so we can detect them.
		const received: string[] = []
		const handler = async (message: { value: string }) => {
			received.push(message.value)
		}

		const consumer1 = client1.consumer(consumerOpts)
		const run1 = consumer1.runEach(testTopic, handler, runOpts)
		void run1.catch(() => {})
		await waitUntilRunning(consumer1, run1)

		// consumer1 processes all 5 (consumed offset = 5) but does NOT commit yet.
		await vi.waitFor(() => expect(received.filter(v => v.startsWith('m-')).length).toBeGreaterThanOrEqual(5), {
			timeout: 30_000,
		})

		// Second consumer joins → rebalance → partition 0 revoked from consumer1. The
		// revoke handler must commit offset 5 before removing the partition.
		const consumer2 = client2.consumer(consumerOpts)
		const run2 = consumer2.runEach(testTopic, handler, runOpts)
		void run2.catch(() => {})
		await waitUntilRunning(consumer2, run2)

		// Sentinel proves the rebalance settled and the post-rebalance owner resumed.
		await producer.send(testTopic, { value: 'sentinel' })
		await producer.flush()
		await vi.waitFor(() => expect(received).toContain('sentinel'), { timeout: 30_000 })

		// If the revoke commit persisted offset 5, the new owner resumes there and never
		// re-delivers m-0..m-4. Dropping the revoke commit leaves the group with no
		// committed offset, so the owner resets to earliest and reprocesses them.
		const counts = new Map<string, number>()
		for (const v of received) counts.set(v, (counts.get(v) ?? 0) + 1)
		for (let i = 0; i < 5; i++) {
			expect(counts.get(`m-${i}`), `m-${i} should be delivered exactly once`).toBe(1)
		}

		consumer1.stop()
		consumer2.stop()
		await Promise.allSettled([run1, run2])
		await producer.disconnect()
		await Promise.all([client1.disconnect(), client2.disconnect()])
	})

	it('runBatch commits what the handler resolved when it then fails', async () => {
		const client = createClient('it-batch-resolve')
		await client.connect()

		const topicName = uniqueName('it-batch-resolve')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 10 }, (_, i) => ({ value: `m-${i}` }))
		)
		await producer.flush()

		const groupId = uniqueName('it-group')

		// Works through the batch, resolves the first 4 records, then fails.
		const consumer1 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		await expect(
			consumer1.runBatch(testTopic, async (batch, ctx) => {
				expect(batch.length).toBe(10)
				ctx.resolveOffset(batch[3]!.offset)
				throw new Error('handler failed')
			})
		).rejects.toThrow('handler failed')

		// The next consumer of the group resumes after the resolved records.
		const consumer2 = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const received: string[] = []
		await consumer2.runBatch(testTopic, async batch => {
			received.push(...batch.map(m => m.value))
			consumer2.stop()
		})
		expect(received[0]).toBe('m-4')
		expect(received).toHaveLength(6)

		await producer.disconnect()
		await client.disconnect()
	})

	it('commitStartPosition commits where a partition starts, and refuses once records were fetched', async () => {
		const client = createClient('it-start-position')
		await client.connect()

		const topicName = uniqueName('it-start-position')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const producer = client.producer({ lingerMs: 0 })
		await producer.send(
			testTopic,
			Array.from({ length: 3 }, (_, i) => ({ value: `before-${i}` }))
		)
		await producer.flush()

		const groupId = uniqueName('it-group')
		const partition = { topic: topicName, partition: 0 }

		// 'latest' with no committed offset starts at the end (3). Commit that as the start.
		const first = client.consumer({ groupId, autoOffsetReset: 'latest' })
		let committed: Promise<void> | undefined
		first.on('partitionsAssigned', () => {
			committed = first.commitStartPosition([partition])
		})
		const firstRun = first.runEach(testTopic, async () => {})
		await vi.waitFor(() => expect(committed).toBeDefined(), { timeout: 15_000 })
		await committed
		first.stop()
		await firstRun

		// Arrives while nobody consumes: a restart resolving 'latest' again would skip it.
		await producer.send(testTopic, { value: 'in-between' })
		await producer.flush()

		const second = client.consumer({ groupId, autoOffsetReset: 'latest' })
		const received: string[] = []
		let refused: unknown
		await second.runEach(testTopic, async message => {
			received.push(message.value)
			// Records were fetched by now: the position is past what handlers have seen.
			refused = await second.commitStartPosition([partition]).catch(error => error)
			second.stop()
		})
		expect(received).toEqual(['in-between'])
		expect((refused as Error).message).toMatch(/already fetched records/)

		await producer.disconnect()
		await client.disconnect()
	})
})
