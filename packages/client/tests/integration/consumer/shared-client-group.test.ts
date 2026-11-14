import { describe, expect, it, vi } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

type RunningEmitter = {
	once(event: 'running', cb: () => void): unknown
	once(event: 'error', cb: (err: Error) => void): unknown
}

function waitUntilRunning(consumer: RunningEmitter, run: Promise<void>): Promise<void> {
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

describe('Consumer (integration) - shared client group membership', () => {
	it('lets several consumers of the same group share one KafkaClient', async () => {
		const client = createClient('it-shared-client-group')
		await client.connect()

		const topicName = uniqueName('it-shared-client-group')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])

		const groupId = uniqueName('it-group')
		const mk = () =>
			client.consumer({
				groupId,
				autoOffsetReset: 'earliest',
				partitionAssignmentStrategy: 'range',
				sessionTimeoutMs: 10_000,
				rebalanceTimeoutMs: 20_000,
				heartbeatIntervalMs: 1000,
			})

		const consumers = [mk(), mk(), mk()]
		const owned = consumers.map(() => new Set<number>())
		consumers.forEach((c, i) => {
			c.on('partitionsAssigned', parts => parts.forEach(p => owned[i]!.add(p.partition)))
			c.on('partitionsRevoked', parts => parts.forEach(p => owned[i]!.delete(p.partition)))
		})

		const received: string[] = []
		const handler = async (m: { value: string }) => {
			received.push(m.value)
		}
		const runs: Promise<void>[] = []
		const start = (i: number) => {
			const run = consumers[i]!.runEach(testTopic, handler, { autoCommit: false })
			runs[i] = run
			return waitUntilRunning(consumers[i]!, run)
		}

		const totalOwned = (...idx: number[]) => idx.reduce((n, i) => n + owned[i]!.size, 0)
		const union = (...idx: number[]) => new Set(idx.flatMap(i => [...owned[i]!]))

		// Two consumers, one shared client: both must reach running and cover all partitions
		await Promise.all([start(0), start(1)])
		await vi.waitFor(
			() => {
				expect(totalOwned(0, 1)).toBe(4)
				expect(union(0, 1).size).toBe(4)
			},
			{ timeout: 15_000 }
		)

		// Third joins
		await start(2)
		await vi.waitFor(
			() => {
				expect(totalOwned(0, 1, 2)).toBe(4)
				expect(union(0, 1, 2).size).toBe(4)
				expect(owned[2]!.size).toBeGreaterThan(0)
			},
			{ timeout: 20_000 }
		)

		// One leaves, assignment converges on the remaining two
		consumers[0]!.stop()
		await runs[0]
		await vi.waitFor(
			() => {
				expect(totalOwned(1, 2)).toBe(4)
				expect(union(1, 2).size).toBe(4)
			},
			{ timeout: 20_000 }
		)

		const producer = client.producer({ lingerMs: 0 })
		const values = ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7']
		await producer.send(
			testTopic,
			values.map((value, i) => ({ value, partition: i % 4 }))
		)
		await producer.flush()

		await vi.waitFor(() => expect(received.length).toBeGreaterThanOrEqual(values.length), { timeout: 30_000 })
		expect([...received].sort()).toEqual([...values].sort())

		consumers[1]!.stop()
		consumers[2]!.stop()
		await Promise.all([runs[1], runs[2]])
		await producer.disconnect()
		await client.disconnect()
	}, 90_000)
})
