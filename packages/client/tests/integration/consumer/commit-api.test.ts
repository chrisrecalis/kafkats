import { describe, expect, it } from 'vitest'

import type { KafkaClient } from '@/client/index.js'
import type { Consumer } from '@/consumer/consumer.js'
import type { AssignedPartition } from '@/consumer/types.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

/** Start a group run that commits nothing by itself; resolves with its first assignment. */
async function runGroupMember(consumer: Consumer, topicName: string) {
	const assigned = new Promise<AssignedPartition[]>(resolve => consumer.once('partitionsAssigned', resolve))
	const run = consumer.runBatch(topicName, async () => {}, { commitOffsets: false })
	return { run, assigned: await assigned }
}

async function createTopic(client: KafkaClient, prefix: string, numPartitions: number): Promise<string> {
	const name = uniqueName(prefix)
	await client.createTopics([{ name, numPartitions, replicationFactor: 1 }])
	return name
}

describe.concurrent('Consumer commit() and committed() (integration)', () => {
	it('commits offsets with metadata, reads them back, and hands them to the next assignment', async () => {
		const client = createClient('it-commit-api')
		await client.connect()
		const topicName = await createTopic(client, 'it-commit-api', 2)
		const groupId = uniqueName('it-commit-api-group')
		const partitions = [0, 1].map(partition => ({ topic: topicName, partition }))

		const first = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const { run } = await runGroupMember(first, topicName)
		await first.commit([
			{ topic: topicName, partition: 0, offset: 2n, metadata: 'AgAAAAAAAAAB' },
			{ topic: topicName, partition: 1, offset: 1n },
		])

		expect(await first.committed(partitions)).toEqual(
			new Map([
				[`${topicName}:0`, { offset: 2n, metadata: 'AgAAAAAAAAAB' }],
				[`${topicName}:1`, { offset: 1n, metadata: null }],
			])
		)
		first.stop()
		await run

		const next = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const { run: nextRun, assigned } = await runGroupMember(next, topicName)

		expect([...assigned].sort((a, b) => a.partition - b.partition)).toEqual([
			{ topic: topicName, partition: 0, offset: 2n, committed: { offset: 2n, metadata: 'AgAAAAAAAAAB' } },
			{ topic: topicName, partition: 1, offset: 1n, committed: { offset: 1n, metadata: null } },
		])
		next.stop()
		await nextRun
		await client.disconnect()
	})

	it('reads back offsets and metadata committed in a transaction', async () => {
		const client = createClient('it-commit-api-txn')
		await client.connect()
		const topicName = await createTopic(client, 'it-commit-api-txn', 1)
		const groupId = uniqueName('it-commit-api-txn-group')

		const producer = client.producer({ transactionalId: uniqueName('it-commit-api-tx'), lingerMs: 0 })
		await producer.transaction(async tx => {
			await tx.sendOffsets({
				groupId,
				offsets: [{ topic: topicName, partition: 0, offset: 7n, metadata: 'txn' }],
			})
		})

		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const { run, assigned } = await runGroupMember(consumer, topicName)

		expect(assigned).toEqual([
			{ topic: topicName, partition: 0, offset: 7n, committed: { offset: 7n, metadata: 'txn' } },
		])
		expect(await consumer.committed([{ topic: topicName, partition: 0 }])).toEqual(
			new Map([[`${topicName}:0`, { offset: 7n, metadata: 'txn' }]])
		)

		consumer.stop()
		await run
		await producer.disconnect()
		await client.disconnect()
	})

	it('commits with a manual assignment, outside any group generation', async () => {
		const client = createClient('it-commit-api-manual')
		await client.connect()
		const topicName = await createTopic(client, 'it-commit-api-manual', 1)
		const groupId = uniqueName('it-commit-api-manual-group')
		const partition = { topic: topicName, partition: 0 }

		const consumer = client.consumer({ groupId })
		const running = new Promise<void>(resolve => consumer.once('running', () => resolve()))
		const run = consumer.runBatch(topicName, async () => {}, {
			assignment: [{ ...partition, offset: 0n }],
			commitOffsets: false,
		})
		await running
		await consumer.commit([{ ...partition, offset: 4n, metadata: 'manual' }])

		expect(consumer.groupMetadata()).toBeUndefined()
		expect(await consumer.committed([partition])).toEqual(
			new Map([[`${topicName}:0`, { offset: 4n, metadata: 'manual' }]])
		)

		consumer.stop()
		await run
		await client.disconnect()
	})
})
