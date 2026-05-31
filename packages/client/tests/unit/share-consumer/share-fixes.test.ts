import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumer(extraCluster: Record<string, unknown> = {}): any {
	const cluster = { getLogger: () => null, invalidateCoordinator: vi.fn(), ...extraCluster }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const c = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
	c.abortController = new AbortController()
	c.state = 'running'
	c.topicNameById.set(TOPIC_ID, 't')
	return c
}

function partitionErrorResponse(errorCode: ErrorCode) {
	return {
		errorCode: ErrorCode.None,
		topics: [
			{
				topicId: TOPIC_ID,
				partitions: [
					{
						partitionIndex: 0,
						errorCode,
						acknowledgeErrorCode: ErrorCode.None,
						acquiredRecords: [],
						recordsData: null,
					},
				],
			},
		],
	}
}

const subscriptionByTopic = new Map([['t', { topic: 't', decoder: (b: Buffer) => b, keyDecoder: undefined }]])
const ackMgr = () => ({ enqueue: vi.fn().mockResolvedValue(undefined) })

describe('ShareConsumer config validation', () => {
	const cluster = { getLogger: () => null }
	it('rejects non-positive maxRecords', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(() => new ShareConsumer(cluster as any, { groupId: 'g', maxRecords: 0 })).toThrow(/maxRecords/)
	})
	it('rejects non-positive batchSize', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(() => new ShareConsumer(cluster as any, { groupId: 'g', batchSize: -1 })).toThrow(/batchSize/)
	})
	it('rejects maxBytes < minBytes', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(() => new ShareConsumer(cluster as any, { groupId: 'g', minBytes: 100, maxBytes: 10 })).toThrow(
			/maxBytes/
		)
	})
	it('accepts a valid config', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		expect(() => new ShareConsumer(cluster as any, { groupId: 'g', maxRecords: 10, batchSize: 5 })).not.toThrow()
	})
})

describe('ShareConsumer fetch per-partition error classification', () => {
	it('skips (does not throw) and flags metadata stale on a recoverable error', async () => {
		const c = makeConsumer()
		const items = await c.collectShareFetchWorkItems(
			{ nodeId: 1 },
			partitionErrorResponse(ErrorCode.KafkaStorageError),
			subscriptionByTopic,
			ackMgr()
		)
		expect(items).toEqual([])
		expect(c.fetchMetadataStale).toBe(true)
	})

	it('skips (does not throw) without metadata refresh on a skip-only error', async () => {
		const c = makeConsumer()
		const items = await c.collectShareFetchWorkItems(
			{ nodeId: 1 },
			partitionErrorResponse(ErrorCode.UnknownServerError),
			subscriptionByTopic,
			ackMgr()
		)
		expect(items).toEqual([])
		expect(c.fetchMetadataStale).toBe(false)
	})

	it('throws (fatal) on an authorization error', async () => {
		const c = makeConsumer()
		await expect(
			c.collectShareFetchWorkItems(
				{ nodeId: 1 },
				partitionErrorResponse(ErrorCode.TopicAuthorizationFailed),
				subscriptionByTopic,
				ackMgr()
			)
		).rejects.toThrow()
	})
})

describe('ShareConsumer ShareSessionLimitReached (133)', () => {
	it('resets the share session epoch on a ShareFetch carrying 133', async () => {
		const c = makeConsumer()
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn().mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.ShareSessionLimitReached,
				errorMessage: null,
				acquisitionLockTimeoutMs: 0,
				topics: [],
				nodeEndpoints: [],
			}),
		}
		c.shareSessionEpochByBrokerId.set(1, 5)
		await expect(c.shareFetch(broker, [{ topicId: TOPIC_ID, partitions: [0] }])).rejects.toThrow()
		// 133 is a share-session error: epoch reset to the initial value, not advanced to 6.
		expect(c.shareSessionEpochByBrokerId.get(1)).toBe(0)
	})
})

describe('ShareConsumer member fencing', () => {
	for (const code of [ErrorCode.FencedMemberEpoch, ErrorCode.UnknownMemberId]) {
		it(`rejoins (does not throw) on ${ErrorCode[code]}`, async () => {
			const c = makeConsumer()
			const originalMemberId = c.memberId
			c.memberEpoch = 7
			c.assignment = [{ topic: 't', partition: 0 }]
			c.shareSessionEpochByBrokerId.set(1, 5)
			c.coordinator = {
				shareGroupHeartbeat: vi.fn().mockResolvedValue({
					errorCode: code,
					errorMessage: null,
					memberId: originalMemberId,
					memberEpoch: 7,
					heartbeatIntervalMs: 1000,
					assignment: null,
				}),
			}
			const revoked = vi.fn()
			c.on('partitionsRevoked', revoked)

			await expect(c.shareHeartbeat(['t'])).resolves.toBeUndefined()

			expect(c.memberEpoch).toBe(0) // reset to the rejoin epoch
			expect(c.memberId).toBe(originalMemberId) // memberId is kept across a fence
			expect(c.assignment).toEqual([]) // assignment abandoned
			expect(c.shareSessionEpochByBrokerId.size).toBe(0) // per-broker sessions wiped
			expect(revoked).toHaveBeenCalledWith([{ topic: 't', partition: 0 }])
		})
	}
})

describe('ShareConsumer stream() surfaces a fatal loop error', () => {
	it('throws the background loop error to the iterator instead of ending cleanly', async () => {
		const c = makeConsumer()
		c.state = 'idle' // stream() calls startRun(), which requires idle
		c.refreshTopicIdMaps = vi.fn().mockResolvedValue(undefined)
		c.ensureCoordinator = vi.fn().mockResolvedValue(undefined)
		c.joinGroup = vi.fn().mockResolvedValue(undefined)
		c.heartbeatLoop = vi.fn().mockResolvedValue(undefined)
		c.fetchLoop = vi.fn().mockRejectedValue(new Error('fatal loop failure'))
		c.finalizeRun = vi.fn().mockResolvedValue(undefined)

		await expect(async () => {
			for await (const item of c.stream('t')) {
				void item // no records; the loop fails fatally
			}
		}).rejects.toThrow('fatal loop failure')
	})
})
