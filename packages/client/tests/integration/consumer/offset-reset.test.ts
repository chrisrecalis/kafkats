import { describe, expect, it, vi } from 'vitest'

import type { KafkaClient } from '@/client/index.js'
import type { Consumer } from '@/consumer/consumer.js'
import type { AssignedPartition, OffsetResetEvent, TopicPartitionOffset } from '@/consumer/types.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

const records = (count: number) => Array.from({ length: count }, (_, i) => ({ value: Buffer.from(`v${i}`) }))

/** Commit `offset` for partition 0 of `topicName` in `groupId`, as a consumer with a manual assignment does. */
async function commitAs(client: KafkaClient, groupId: string, topicName: string, offset: bigint): Promise<void> {
	const consumer = client.consumer({ groupId })
	const running = new Promise<void>(resolve => consumer.once('running', () => resolve()))
	const run = consumer.runBatch(topicName, async () => {}, {
		assignment: [{ topic: topicName, partition: 0, offset }],
		commitOffsets: false,
	})
	await running
	await consumer.commit([{ topic: topicName, partition: 0, offset }])
	consumer.stop()
	await run
}

/**
 * A topic of 10 records whose group committed offset 2, after which the records below 5 were deleted: the committed
 * position is out of range when the group's next consumer fetches it.
 */
async function deletedBeforeRead(client: KafkaClient, prefix: string) {
	const topicName = uniqueName(prefix)
	const groupId = uniqueName(`${prefix}-group`)
	await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
	const producer = client.producer({ lingerMs: 0 })
	await producer.send(topicName, records(10))
	await producer.disconnect()
	await commitAs(client, groupId, topicName, 2n)
	await client.admin().deleteRecords([{ topic: topicName, partition: 0, offset: 5n }])
	return { topicName, groupId }
}

/** Run a consumer that commits nothing by itself; `delivered` collects the offsets of every record. */
function run(consumer: Consumer, topicName: string) {
	const delivered: bigint[] = []
	const done = consumer.runEach(
		topicName,
		async message => {
			delivered.push(message.offset)
		},
		{ commitOffsets: false }
	)
	return { done, delivered }
}

describe.concurrent('Consumer offset reset (integration)', () => {
	it("pauses a partition whose records were deleted before it read them under 'none', and reports it", async () => {
		const client = createClient('it-reset-none')
		await client.connect()
		const { topicName, groupId } = await deletedBeforeRead(client, 'it-reset-none')

		const consumer = client.consumer({ groupId, autoOffsetReset: 'none' })
		const outOfRange: TopicPartitionOffset[] = []
		consumer.on('offsetOutOfRange', position => outOfRange.push(position))
		const { done, delivered } = run(consumer, topicName)

		await vi.waitFor(() => expect(outOfRange).toEqual([{ topic: topicName, partition: 0, offset: 2n }]), {
			timeout: 15_000,
		})
		expect(consumer.metrics().partitions[0]).toMatchObject({ paused: true })
		expect(consumer.isRunning).toBe(true)

		consumer.seek(topicName, 0, 5n)
		consumer.resume([{ topic: topicName, partition: 0 }])
		await vi.waitFor(() => expect(delivered).toEqual([5n, 6n, 7n, 8n, 9n]), { timeout: 15_000 })

		consumer.stop()
		await done
		await client.disconnect()
	})

	it("moves a partition whose records were deleted before it read them under 'earliest', and reports it", async () => {
		const client = createClient('it-reset-earliest')
		await client.connect()
		const { topicName, groupId } = await deletedBeforeRead(client, 'it-reset-earliest')

		const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })
		const resets: OffsetResetEvent[] = []
		consumer.on('offsetReset', reset => resets.push(reset))
		const { done, delivered } = run(consumer, topicName)

		await vi.waitFor(() => expect(delivered).toEqual([5n, 6n, 7n, 8n, 9n]), { timeout: 15_000 })
		expect(resets).toEqual([{ topic: topicName, partition: 0, from: 2n, to: 5n, reason: 'out-of-range' }])

		consumer.stop()
		await done
		await client.disconnect()
	})

	it("starts a partition without a committed offset under 'latest' at the last stable offset while a transaction is open", async () => {
		const client = createClient('it-reset-latest-lso')
		await client.connect()
		const topicName = uniqueName('it-reset-latest-lso')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const plain = client.producer({ lingerMs: 0 })
		await plain.send(topicName, records(3))
		const transactional = client.producer({ transactionalId: uniqueName('it-reset-latest-lso-tx'), lingerMs: 0 })
		const transaction = await transactional.beginTransaction()
		// Offsets 3 and 4, not committed yet: the last stable offset stays at 3, the high watermark is 5.
		await transaction.send(topicName, records(2))

		const consumer = client.consumer({
			groupId: uniqueName('it-reset-latest-lso-group'),
			autoOffsetReset: 'latest',
		})
		const resets: OffsetResetEvent[] = []
		consumer.on('offsetReset', reset => resets.push(reset))
		const assigned = new Promise<AssignedPartition[]>(resolve => consumer.once('partitionsAssigned', resolve))
		const { done, delivered } = run(consumer, topicName)

		expect(await assigned).toEqual([{ topic: topicName, partition: 0, offset: 3n, committed: null }])
		expect(resets).toEqual([{ topic: topicName, partition: 0, from: null, to: 3n, reason: 'no-commit' }])

		// What the transaction wrote is delivered once it commits.
		await transaction.commit()
		await vi.waitFor(() => expect(delivered).toEqual([3n, 4n]), { timeout: 15_000 })

		consumer.stop()
		await done
		await Promise.all([plain.disconnect(), transactional.disconnect()])
		await client.disconnect()
	})
})
