import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { ACK_GAP } from '@/share-consumer/ack-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

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

function decoded(offsets: number[]) {
	return offsets.map(o => ({
		offset: BigInt(o),
		timestamp: 0n,
		key: null,
		value: Buffer.from(`v${o}`),
		headers: [],
	}))
}

function response(acquiredRecords: Array<{ firstOffset: bigint; lastOffset: bigint; deliveryCount: number }>) {
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
						recordsData: Buffer.from('x'),
					},
				],
			},
		],
	}
}

const subscriptionByTopic = new Map([['t', { topic: 't', decoder: (b: Buffer) => b, keyDecoder: undefined }]])
const ackMgr = () => ({ enqueue: vi.fn().mockResolvedValue(undefined) })

describe('ShareConsumer acquiredRecords filtering', () => {
	it('delivers only records inside acquiredRecords ranges, skipping already-acked records in the batch', async () => {
		const consumer = makeConsumer()
		// The batch physically spans offsets 0..4, but the broker only acquired (locked) 2..3 for us
		// (e.g. 0..1 were already acked and held the share-partition start offset back).
		consumer.decodeRecords = vi.fn().mockResolvedValue(decoded([0, 1, 2, 3, 4]))

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response([{ firstOffset: 2n, lastOffset: 3n, deliveryCount: 4 }]),
			subscriptionByTopic,
			ackMgr()
		)

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(items.map((i: any) => i.record.offset)).toEqual([2n, 3n])
		// deliveryCount is sourced from the matched acquired range, never undefined for a delivered record.
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(items.every((i: any) => i.deliveryCount === 4)).toBe(true)
	})

	it('delivers nothing when acquiredRecords is empty even though the batch has records', async () => {
		const consumer = makeConsumer()
		consumer.decodeRecords = vi.fn().mockResolvedValue(decoded([0, 1]))

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response([]),
			subscriptionByTopic,
			ackMgr()
		)

		expect(items).toEqual([])
	})

	it('gap-acks acquired offsets that have no delivered record (compaction hole / control offset)', async () => {
		const consumer = makeConsumer()
		// Acquired offsets 0..3, but only 0 and 2 decoded as records (1 and 3 are holes/control).
		consumer.decodeRecords = vi.fn().mockResolvedValue(decoded([0, 2]))
		const ackManager = ackMgr()

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response([{ firstOffset: 0n, lastOffset: 3n, deliveryCount: 1 }]),
			subscriptionByTopic,
			ackManager
		)

		// Real records 0 and 2 are delivered.
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(items.map((i: any) => i.record.offset)).toEqual([0n, 2n])
		// Missing acquired offsets 1 and 3 are gap-acked so the SPSO can advance past them.
		const gapOffsets = ackManager.enqueue.mock.calls
			.filter(c => c[4] === ACK_GAP)
			.map(c => c[3])
			.sort((a, b) => (a < b ? -1 : 1))
		expect(gapOffsets).toEqual([1n, 3n])
	})

	it('swallows a failing gap-ack (no throw, no unhandled rejection)', async () => {
		const consumer = makeConsumer()
		consumer.decodeRecords = vi.fn().mockResolvedValue(decoded([0]))
		// Gap-ack for offset 1 fails (e.g. lock expired); it must not escape as an unhandled rejection.
		const ackManager = { enqueue: vi.fn().mockRejectedValue(new Error('gap ack flush failed')) }
		const unhandled = vi.fn()
		process.on('unhandledRejection', unhandled)
		try {
			const items = await consumer.collectShareFetchWorkItems(
				{ nodeId: 1 },
				response([{ firstOffset: 0n, lastOffset: 1n, deliveryCount: 1 }]),
				subscriptionByTopic,
				ackManager
			)
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			expect(items.map((i: any) => i.record.offset)).toEqual([0n])
			await new Promise(r => setTimeout(r, 10)) // let any rejection surface
			expect(unhandled).not.toHaveBeenCalled()
		} finally {
			process.off('unhandledRejection', unhandled)
		}
	})

	it('gap-acks a control-only response (no decoded records) so the SPSO still advances', async () => {
		const consumer = makeConsumer()
		consumer.decodeRecords = vi.fn().mockResolvedValue([])
		const ackManager = ackMgr()

		const items = await consumer.collectShareFetchWorkItems(
			{ nodeId: 1 },
			response([{ firstOffset: 10n, lastOffset: 11n, deliveryCount: 1 }]),
			subscriptionByTopic,
			ackManager
		)

		expect(items).toEqual([])
		const gapOffsets = ackManager.enqueue.mock.calls.filter(c => c[4] === ACK_GAP).map(c => c[3])
		expect(new Set(gapOffsets)).toEqual(new Set([10n, 11n]))
	})
})
