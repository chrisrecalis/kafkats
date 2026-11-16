import { describe, expect, it, vi } from 'vitest'

import type { KafkaClient } from '@/client/index.js'
import type { Consumer } from '@/consumer/consumer.js'
import type { AssignedPartition, ConsumerConfig, TopicPartition } from '@/consumer/types.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

const fast: Partial<ConsumerConfig> = {
	autoOffsetReset: 'earliest',
	partitionAssignmentStrategy: 'range',
	sessionTimeoutMs: 10_000,
	rebalanceTimeoutMs: 20_000,
	heartbeatIntervalMs: 500,
	maxWaitMs: 200,
}

async function topicWithRecords(client: KafkaClient, prefix: string, partitions: number, perPartition: number) {
	const name = uniqueName(prefix)
	await client.createTopics([{ name, numPartitions: partitions, replicationFactor: 1 }])
	const producer = client.producer({ lingerMs: 0 })
	for (let partition = 0; partition < partitions; partition++) {
		const records = Array.from({ length: perPartition }, (_, i) => ({
			value: Buffer.from(`${partition}-${i}`),
			partition,
		}))
		await producer.send(name, records)
	}
	await producer.disconnect()
	return name
}

/** Run a consumer that commits nothing by itself; `delivered` collects `partition:offset` of every record. */
function run(consumer: Consumer, topicName: string) {
	const delivered: string[] = []
	const done = consumer.runEach(
		topicName,
		async message => {
			delivered.push(`${message.partition}:${message.offset}`)
		},
		{ commitOffsets: false }
	)
	return { done, delivered }
}

describe.concurrent('Consumer rebalance hooks (integration)', () => {
	it('the next owner of a partition starts at the offset the revoking member committed in onPartitionsRevoked', async () => {
		const client = createClient('it-hooks-revoke-commit')
		await client.connect()
		const topicName = await topicWithRecords(client, 'it-hooks-revoke-commit', 2, 5)
		const groupId = uniqueName('it-hooks-group')

		const revoked: TopicPartition[] = []
		const first: Consumer = client.consumer({
			groupId,
			...fast,
			onPartitionsRevoked: async partitions => {
				revoked.push(...partitions)
				// Everything delivered so far is processed: commit up to the next offset to deliver.
				await first.commit(
					partitions.map(tp => ({
						...tp,
						offset: first.nextOffset(tp.topic, tp.partition)!,
						metadata: `first-${tp.partition}`,
					}))
				)
			},
		})
		const firstRun = run(first, topicName)
		await vi.waitFor(() => expect(firstRun.delivered).toHaveLength(10), { timeout: 15_000 })

		const assigned: AssignedPartition[] = []
		const second = client.consumer({
			groupId,
			...fast,
			onPartitionsAssigned: async partitions => {
				assigned.push(...partitions)
			},
		})
		const secondRun = run(second, topicName)
		await vi.waitFor(() => expect(assigned.length).toBeGreaterThan(0), { timeout: 20_000 })

		// Eager: the first member gave up both partitions, and committed both before it rejoined.
		expect(revoked.map(tp => tp.partition).sort()).toEqual([0, 1])
		expect(assigned).toHaveLength(1)
		const [moved] = assigned
		expect(moved).toEqual({
			topic: topicName,
			partition: moved!.partition,
			offset: 5n,
			committed: { offset: 5n, metadata: `first-${moved!.partition}` },
		})
		await sleep(1000)
		expect(secondRun.delivered).toEqual([])

		first.stop()
		second.stop()
		await Promise.all([firstRun.done, secondRun.done])
		await client.disconnect()
	})

	it('delivers nothing of a partition paused in onPartitionsAssigned until it is resumed', async () => {
		const client = createClient('it-hooks-assign-pause')
		await client.connect()
		const topicName = await topicWithRecords(client, 'it-hooks-assign-pause', 2, 3)
		const paused = { topic: topicName, partition: 1 }

		const consumer: Consumer = client.consumer({
			groupId: uniqueName('it-hooks-group'),
			...fast,
			onPartitionsAssigned: async () => {
				consumer.pause([paused])
			},
		})
		const { done, delivered } = run(consumer, topicName)
		await vi.waitFor(() => expect(delivered).toHaveLength(3), { timeout: 15_000 })
		await sleep(1500)
		expect(delivered).toEqual(['0:0', '0:1', '0:2'])

		consumer.resume([paused])
		await vi.waitFor(() => expect(delivered).toHaveLength(6), { timeout: 15_000 })
		expect(delivered.slice(3)).toEqual(['1:0', '1:1', '1:2'])

		consumer.stop()
		await done
		await client.disconnect()
	})
})
