import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { ACK_GAP } from '@/share-consumer/ack-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumer(): any {
	const cluster = { getLogger: () => null }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const consumer = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
	consumer.abortController = new AbortController()
	consumer.state = 'running'
	consumer.topicNameById.set(TOPIC_ID, 't')
	return consumer
}

function encodeBatch(baseOffset: bigint, values: string[]): Buffer {
	return encodeRecordBatchSync(
		createRecordBatch(
			values.map(v => ({ value: v, timestamp: 0 })),
			baseOffset,
			0n
		)
	)
}

function response(
	recordsData: Buffer,
	acquiredRecords: Array<{ firstOffset: bigint; lastOffset: bigint; deliveryCount: number }>
) {
	return {
		errorCode: ErrorCode.None,
		topics: [
			{
				topicId: TOPIC_ID,
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						acknowledgeErrorCode: ErrorCode.None,
						acquiredRecords,
						recordsData,
					},
				],
			},
		],
	}
}

const subscriptionByTopic = new Map([['t', { topic: 't', decoder: (b: Buffer) => b, keyDecoder: undefined }]])

const gapOffsets = (ackManager: { enqueue: ReturnType<typeof vi.fn> }) =>
	ackManager.enqueue.mock.calls.filter(c => c[4] === ACK_GAP).map(c => c[3])

describe('ShareConsumer mid-stream decode failures must not gap-ack real acquired records', () => {
	it('surfaces an error for a corrupt non-trailing acquired batch instead of gap-acking its offsets', async () => {
		const consumer = makeConsumer()

		const batch1 = encodeBatch(0n, ['a', 'b'])
		const batch2 = encodeBatch(2n, ['c', 'd'])
		const data = Buffer.concat([batch1, batch2])
		// Corrupt a byte inside the second batch's record data. The buffer still holds the batch's
		// full declared length, so this is genuine corruption, NOT a maxBytes-truncated tail.
		data.writeUInt8(data.readUInt8(data.length - 3) ^ 0xff, data.length - 3)

		const ackManager = { enqueue: vi.fn().mockResolvedValue(undefined) }

		// Gap-acking offsets 2..3 would tell the broker those records do not exist and permanently
		// advance the share-partition start offset past them: data loss. Surface the corruption.
		await expect(
			consumer.collectShareFetchWorkItems(
				{ nodeId: 1 },
				response(data, [{ firstOffset: 0n, lastOffset: 3n, deliveryCount: 1 }]),
				subscriptionByTopic,
				ackManager
			)
		).rejects.toThrow(/corrupt/)

		expect(gapOffsets(ackManager)).toEqual([])
	})

	it('tolerates a maxBytes-truncated trailing batch but does not gap-ack its acquired offsets', async () => {
		const consumer = makeConsumer()

		const batch1 = encodeBatch(0n, ['a', 'b'])
		const batch2 = encodeBatch(2n, ['c', 'd'])
		// The trailing batch is cut short: fewer bytes remain than its declared batchLength.
		const data = Buffer.concat([batch1, batch2.subarray(0, batch2.length - 5)])

		const ackManager = { enqueue: vi.fn().mockResolvedValue(undefined) }

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response(data, [{ firstOffset: 0n, lastOffset: 3n, deliveryCount: 1 }]),
			subscriptionByTopic,
			ackManager
		)

		// The complete leading batch is delivered as usual.
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(items.map((i: any) => i.record.offset)).toEqual([0n, 1n])
		// Offsets 2..3 are real records the truncation withheld; they must be left to redeliver
		// (lock timeout), never gap-acked as nonexistent.
		expect(gapOffsets(ackManager)).toEqual([])
	})

	it('still gap-acks acquired holes inside fully decoded data', async () => {
		const consumer = makeConsumer()

		// Only offsets 0..1 have data; the broker also acquired 2..3 (e.g. compaction holes) with no
		// truncation or corruption in the buffer: those offsets must still be gap-acked so the
		// share-partition start offset can advance.
		const data = encodeBatch(0n, ['a', 'b'])
		const ackManager = { enqueue: vi.fn().mockResolvedValue(undefined) }

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response(data, [{ firstOffset: 0n, lastOffset: 3n, deliveryCount: 1 }]),
			subscriptionByTopic,
			ackManager
		)

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(items.map((i: any) => i.record.offset)).toEqual([0n, 1n])
		expect(gapOffsets(ackManager).sort((a: bigint, b: bigint) => (a < b ? -1 : 1))).toEqual([2n, 3n])
	})
})
