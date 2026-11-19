import { describe, expect, it } from 'vitest'

import { KafkaProtocolError, TimeoutError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { topic } from '@/topic.js'
import { codec } from '@/codec.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

/** A topic with `partitions` partitions and `perPartition` records in each. */
async function topicWithRecords(client: ReturnType<typeof createClient>, partitions: number, perPartition: number) {
	const name = uniqueName('admin-delete-records')
	await client.createTopics([{ name, numPartitions: partitions, replicationFactor: 1 }])
	const producer = client.producer({ lingerMs: 0 })
	const records = Array.from({ length: partitions }, (_, partition) =>
		Array.from({ length: perPartition }, (_, i) => ({ value: `p${partition}-${i}`, partition }))
	).flat()
	await producer.send(topic<string>(name, { value: codec.string() }), records)
	await producer.disconnect()
	return name
}

describe.concurrent('Admin - deleteRecords (integration)', () => {
	it('deletes the records before the offset and moves the earliest offset there', async () => {
		const client = createClient('admin-delete-records')
		await client.connect()
		const admin = client.admin()
		const name = await topicWithRecords(client, 1, 100)

		const results = await admin.deleteRecords([{ topic: name, partition: 0, offset: 50n }])

		expect(results).toEqual([{ topic: name, partition: 0, lowWatermark: 50n }])
		expect(await admin.fetchTopicOffsets(name, [0], 'earliest')).toEqual(new Map([[0, 50n]]))
		expect(await admin.fetchTopicOffsets(name, [0], 'latest')).toEqual(new Map([[0, 100n]]))

		await client.disconnect()
	})

	it('deletes several partitions in one call, -1 meaning up to the high watermark', async () => {
		const client = createClient('admin-delete-records-many')
		await client.connect()
		const admin = client.admin()
		const name = await topicWithRecords(client, 3, 20)

		const results = await admin.deleteRecords([
			{ topic: name, partition: 2, offset: 5n },
			{ topic: name, partition: 0, offset: -1n },
			{ topic: name, partition: 1, offset: 20n },
		])

		expect(results).toEqual([
			{ topic: name, partition: 2, lowWatermark: 5n },
			{ topic: name, partition: 0, lowWatermark: 20n },
			{ topic: name, partition: 1, lowWatermark: 20n },
		])
		expect(await admin.fetchTopicOffsets(name, [0, 1, 2], 'earliest')).toEqual(
			new Map([
				[0, 20n],
				[1, 20n],
				[2, 5n],
			])
		)

		await client.disconnect()
	})

	it('returns per-partition errors: an offset beyond the high watermark, an unknown topic', async () => {
		const client = createClient('admin-delete-records-errors')
		await client.connect()
		const admin = client.admin()
		const name = await topicWithRecords(client, 1, 10)
		const missing = uniqueName('admin-delete-records-missing')

		const [beyond, unknown] = await admin.deleteRecords(
			[
				{ topic: name, partition: 0, offset: 11n },
				{ topic: missing, partition: 0, offset: 1n },
			],
			{ timeoutMs: 1500 }
		)

		expect(beyond!.error).toBeInstanceOf(KafkaProtocolError)
		expect(beyond!.error!.errorCode).toBe(ErrorCode.OffsetOutOfRange)
		expect(unknown!.error).toBeInstanceOf(TimeoutError)
		expect((unknown!.error!.cause as KafkaProtocolError).errorCode).toBe(ErrorCode.UnknownTopicOrPartition)
		expect(await admin.fetchTopicOffsets(name, [0], 'earliest')).toEqual(new Map([[0, 0n]]))

		await client.disconnect()
	})
})
