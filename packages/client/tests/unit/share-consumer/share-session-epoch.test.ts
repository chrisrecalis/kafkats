import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { SHARE_SESSION_INITIAL_EPOCH } from '@/share-consumer/share-consumer-helpers.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumer(): any {
	const cluster = { getLogger: () => null }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
}

function shareFetchResponse(partition: { errorCode: ErrorCode; acknowledgeErrorCode: ErrorCode }): unknown {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		errorMessage: null,
		acquisitionLockTimeoutMs: 0,
		topics: [
			{
				topicId: 'tid',
				partitions: [
					{
						partitionIndex: 0,
						errorCode: partition.errorCode,
						acknowledgeErrorCode: partition.acknowledgeErrorCode,
						acquiredRecords: [],
						recordsData: null,
					},
				],
			},
		],
		nodeEndpoints: [],
	}
}

describe('ShareConsumer share session epoch', () => {
	it('resets (does not advance) the epoch on a per-partition session error', async () => {
		const consumer = makeConsumer()
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn().mockResolvedValue(
				shareFetchResponse({
					errorCode: ErrorCode.None,
					acknowledgeErrorCode: ErrorCode.InvalidShareSessionEpoch,
				})
			),
		}

		// Pre-advance the epoch so we can prove it is reset to the initial value, not just advanced.
		consumer.shareSessionEpochByBrokerId.set(1, 5)

		await expect(consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0] }])).rejects.toThrow()

		// The broker reported a session error, so the epoch must be reset, not advanced to 6.
		expect(consumer.shareSessionEpochByBrokerId.get(1)).toBe(SHARE_SESSION_INITIAL_EPOCH)
	})

	it('still advances the epoch on a non-session per-partition error (session intact)', async () => {
		const consumer = makeConsumer()
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn().mockResolvedValue(
				shareFetchResponse({
					errorCode: ErrorCode.NotLeaderOrFollower,
					acknowledgeErrorCode: ErrorCode.None,
				})
			),
		}

		consumer.shareSessionEpochByBrokerId.set(1, 5)

		// A non-session partition error does not break the session; shareFetch returns the
		// response (the per-partition error is handled by the response processor) and the
		// session epoch advances normally.
		await consumer.shareFetch(broker, [{ topicId: 'tid', partitions: [0] }])
		expect(consumer.shareSessionEpochByBrokerId.get(1)).toBe(6)
	})

	it('resets the epoch on a per-partition session error in ShareAcknowledge', async () => {
		const consumer = makeConsumer()
		const broker = {
			nodeId: 1,
			shareAcknowledge: vi.fn().mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				errorMessage: null,
				acquisitionLockTimeoutMs: 0,
				topics: [
					{
						topicId: 'tid',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.ShareSessionNotFound }],
					},
				],
				nodeEndpoints: [],
			}),
		}

		consumer.shareSessionEpochByBrokerId.set(1, 5)

		await expect(
			consumer.shareAcknowledge(broker, {
				groupId: 'g1',
				memberId: 'm1',
				topics: [],
			})
		).rejects.toThrow()

		expect(consumer.shareSessionEpochByBrokerId.get(1)).toBe(SHARE_SESSION_INITIAL_EPOCH)
	})
})
