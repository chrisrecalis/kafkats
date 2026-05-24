import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ShareFetchRequest } from '@/protocol/messages/requests/share-fetch.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumer(): any {
	const cluster = { getLogger: () => null }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
}

function okResponse(topicId: string, partitions: number[]): unknown {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		errorMessage: null,
		acquisitionLockTimeoutMs: 0,
		topics: [
			{
				topicId,
				partitions: partitions.map(p => ({
					partitionIndex: p,
					errorCode: ErrorCode.None,
					acknowledgeErrorCode: ErrorCode.None,
					acquiredRecords: [],
					recordsData: null,
				})),
			},
		],
		nodeEndpoints: [],
	}
}

describe('ShareConsumer forgottenTopicsData', () => {
	it('forgets partitions that drop out of the assignment on the next incremental fetch', async () => {
		const consumer = makeConsumer()
		const requests: ShareFetchRequest[] = []
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn(async (req: ShareFetchRequest) => {
				requests.push(req)
				const ids = req.topics[0]!.partitions.map(p => p.partitionIndex)
				return okResponse('tid', ids)
			}),
		}

		// First fetch: full session (epoch 0), partitions 0 and 1.
		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0, 1] }])
		// Second fetch: incremental (epoch 1), partition 1 has been revoked.
		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0] }])

		// The full fetch forgets nothing.
		expect(requests[0]!.shareSessionEpoch).toBe(0)
		expect(requests[0]!.forgottenTopicsData).toEqual([])

		// The incremental fetch still requests partition 0 and explicitly forgets partition 1.
		expect(requests[1]!.shareSessionEpoch).toBe(1)
		expect(requests[1]!.topics[0]!.partitions.map(p => p.partitionIndex)).toEqual([0])
		expect(requests[1]!.forgottenTopicsData).toEqual([{ topicId: 'tid', partitions: [1] }])
	})

	it('a forget-only fetch (empty topics) forgets the entire tracked session set', async () => {
		const consumer = makeConsumer()
		const requests: ShareFetchRequest[] = []
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn(async (req: ShareFetchRequest) => {
				requests.push(req)
				const ids = req.topics[0]?.partitions.map(p => p.partitionIndex) ?? []
				return okResponse('tid', ids)
			}),
		}

		// Establish a session holding partitions 0 and 1 (full fetch, epoch 0).
		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0, 1] }])
		// All of this broker's partitions are now gone: a forget-only fetch drops the whole set.
		await consumer.shareFetch(broker, [])

		expect(requests[1]!.topics).toEqual([])
		expect(requests[1]!.forgottenTopicsData).toEqual([{ topicId: 'tid', partitions: [0, 1] }])
		// The tracked session set is now empty.
		expect(consumer.shareSessionPartitionsByBrokerId.get(1)?.size ?? 0).toBe(0)
	})

	it('forgets nothing while the assignment is stable', async () => {
		const consumer = makeConsumer()
		const requests: ShareFetchRequest[] = []
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn(async (req: ShareFetchRequest) => {
				requests.push(req)
				return okResponse('tid', [0, 1])
			}),
		}

		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0, 1] }])
		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0, 1] }])

		expect(requests[1]!.forgottenTopicsData).toEqual([])
	})
})
