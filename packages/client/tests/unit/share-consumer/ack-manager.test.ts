import { describe, it, expect, vi } from 'vitest'

import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ShareAcknowledgeResponse } from '@/protocol/messages/responses/share-acknowledge.js'
import { noopLogger } from '@/logger.js'

import { AckManager, ACK_ACCEPT, ACK_RELEASE, ACK_REJECT, ACK_RENEW } from '@/share-consumer/ack-manager.js'
import type { ShareAcknowledgeRequestWithoutEpoch } from '@/share-consumer/ack-manager.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'
const TOPIC_NAME = 'test-topic'
const PARTITION = 0

function makeSuccessResponse(topicId: string = TOPIC_ID, partitionIndex: number = PARTITION): ShareAcknowledgeResponse {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		errorMessage: null,
		acquisitionLockTimeoutMs: 30000,
		topics: [
			{
				topicId,
				partitions: [
					{
						partitionIndex,
						errorCode: ErrorCode.None,
						errorMessage: null,
						currentLeader: { leaderId: 1, leaderEpoch: 0 },
					},
				],
			},
		],
		nodeEndpoints: [],
	}
}

function makeResponse(partitions: Array<{ partitionIndex: number; errorCode: ErrorCode }>): ShareAcknowledgeResponse {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		errorMessage: null,
		acquisitionLockTimeoutMs: 30000,
		topics: [
			{
				topicId: TOPIC_ID,
				partitions: partitions.map(p => ({
					partitionIndex: p.partitionIndex,
					errorCode: p.errorCode,
					errorMessage: null,
					currentLeader: { leaderId: 1, leaderEpoch: 0 },
				})),
			},
		],
		nodeEndpoints: [],
	}
}

function makeAckManager(
	sendAcknowledge: (broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => Promise<ShareAcknowledgeResponse>
): AckManager {
	const fakeBroker = { nodeId: 1 } as unknown as Broker
	return new AckManager(
		'test-group',
		() => 'member-1',
		sendAcknowledge,
		async () => fakeBroker,
		async () => undefined,
		() => undefined,
		noopLogger,
		1000 // high batch size so we control flush timing via flushAll()
	)
}

describe('AckManager.flushAll - isRenewAck flag', () => {
	it('flushes with isRenewAck=false when only ACCEPT/RELEASE/REJECT entries are pending', async () => {
		const capturedRequests: ShareAcknowledgeRequestWithoutEpoch[] = []
		const sendAcknowledge = vi.fn(async (_broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => {
			capturedRequests.push(req)
			return makeSuccessResponse()
		})

		const ackManager = makeAckManager(sendAcknowledge)

		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 0n, ACK_ACCEPT)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 1n, ACK_RELEASE)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 2n, ACK_REJECT)

		await ackManager.flushAll()

		expect(capturedRequests).toHaveLength(1)
		expect(capturedRequests[0]?.isRenewAck).toBe(false)
	})

	it('flushes with isRenewAck=true when at least one RENEW entry is in the batch', async () => {
		const capturedRequests: ShareAcknowledgeRequestWithoutEpoch[] = []
		const sendAcknowledge = vi.fn(async (_broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => {
			capturedRequests.push(req)
			return makeSuccessResponse()
		})

		const ackManager = makeAckManager(sendAcknowledge)

		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 0n, ACK_ACCEPT)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 1n, ACK_RENEW)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 2n, ACK_RELEASE)

		await ackManager.flushAll()

		expect(capturedRequests).toHaveLength(1)
		expect(capturedRequests[0]?.isRenewAck).toBe(true)
	})

	it('drops RENEW when a finalizing ack exists for the same offset; both promises resolve', async () => {
		const capturedRequests: ShareAcknowledgeRequestWithoutEpoch[] = []
		const sendAcknowledge = vi.fn(async (_broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => {
			capturedRequests.push(req)
			return makeSuccessResponse()
		})

		const ackManager = makeAckManager(sendAcknowledge)

		// Same offset: RENEW first, then ACCEPT — finalizer wins; renew promise still resolves.
		const renewPromise = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 5n, ACK_RENEW)
		const acceptPromise = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 5n, ACK_ACCEPT)

		await ackManager.flushAll()
		await expect(renewPromise).resolves.toBeUndefined()
		await expect(acceptPromise).resolves.toBeUndefined()

		expect(capturedRequests).toHaveLength(1)
		const batches = capturedRequests[0]?.topics[0]?.partitions[0]?.acknowledgementBatches ?? []
		expect(batches).toHaveLength(1)
		expect(batches[0]?.firstOffset).toBe(5n)
		expect(batches[0]?.lastOffset).toBe(5n)
		expect(batches[0]?.acknowledgeTypes).toEqual([ACK_ACCEPT])
		// Finalizer present but no RENEW on the wire => isRenewAck must be false.
		expect(capturedRequests[0]?.isRenewAck).toBe(false)
	})

	it('collapses duplicate same-offset RENEW entries to a single wire entry', async () => {
		const capturedRequests: ShareAcknowledgeRequestWithoutEpoch[] = []
		const sendAcknowledge = vi.fn(async (_broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => {
			capturedRequests.push(req)
			return makeSuccessResponse()
		})

		const ackManager = makeAckManager(sendAcknowledge)

		const a = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 7n, ACK_RENEW)
		const b = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 7n, ACK_RENEW)

		await ackManager.flushAll()
		await expect(a).resolves.toBeUndefined()
		await expect(b).resolves.toBeUndefined()

		const batches = capturedRequests[0]?.topics[0]?.partitions[0]?.acknowledgementBatches ?? []
		expect(batches).toHaveLength(1)
		expect(batches[0]?.firstOffset).toBe(7n)
		expect(batches[0]?.lastOffset).toBe(7n)
		expect(batches[0]?.acknowledgeTypes).toEqual([ACK_RENEW])
		expect(capturedRequests[0]?.isRenewAck).toBe(true)
	})

	it('coalesces consecutive RENEW entries into a single batch with acknowledgeTypes=[4,4,...]', async () => {
		const capturedRequests: ShareAcknowledgeRequestWithoutEpoch[] = []
		const sendAcknowledge = vi.fn(async (_broker: Broker, req: ShareAcknowledgeRequestWithoutEpoch) => {
			capturedRequests.push(req)
			return makeSuccessResponse()
		})

		const ackManager = makeAckManager(sendAcknowledge)

		// Three consecutive RENEW offsets — should coalesce into one batch
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 10n, ACK_RENEW)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 11n, ACK_RENEW)
		void ackManager.enqueue(TOPIC_NAME, TOPIC_ID, PARTITION, 12n, ACK_RENEW)

		await ackManager.flushAll()

		expect(capturedRequests).toHaveLength(1)
		expect(capturedRequests[0]?.isRenewAck).toBe(true)

		const partitions = capturedRequests[0]?.topics[0]?.partitions ?? []
		expect(partitions).toHaveLength(1)

		const batches = partitions[0]?.acknowledgementBatches ?? []
		// All three consecutive RENEW offsets should be coalesced into a single range batch
		// with a single acknowledgeType entry (the range covers firstOffset..lastOffset)
		expect(batches).toHaveLength(1)
		expect(batches[0]?.firstOffset).toBe(10n)
		expect(batches[0]?.lastOffset).toBe(12n)
		expect(batches[0]?.acknowledgeTypes).toEqual([ACK_RENEW])
	})
})

describe('AckManager.flushAll - cross-partition error isolation', () => {
	it('retries a retriable partition even when another partition in the same request fails fatally', async () => {
		let calls = 0
		const sendAcknowledge = vi.fn(async (_broker: Broker, _req: ShareAcknowledgeRequestWithoutEpoch) => {
			calls++
			if (calls === 1) {
				// One request, two partitions: P0 fails fatally (non-retriable), P1 hits a
				// retriable leader error and should be queued for retry.
				return makeResponse([
					{ partitionIndex: 0, errorCode: ErrorCode.InvalidRecordState },
					{ partitionIndex: 1, errorCode: ErrorCode.NotLeaderOrFollower },
				])
			}
			// Retry attempt: the retriable partition now succeeds.
			return makeResponse([{ partitionIndex: 1, errorCode: ErrorCode.None }])
		})

		const ackManager = makeAckManager(sendAcknowledge)
		const p0 = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, 0, 0n, ACK_ACCEPT)
		const p1 = ackManager.enqueue(TOPIC_NAME, TOPIC_ID, 1, 0n, ACK_ACCEPT)
		const p0Settled = p0.then(
			() => 'resolved' as const,
			() => 'rejected' as const
		)
		const p1Settled = p1.then(
			() => 'resolved' as const,
			() => 'rejected' as const
		)

		await ackManager.flushAll().catch(() => undefined)

		// The retriable partition must NOT be rejected just because a different partition
		// in the same request failed fatally — it must be retried and resolve.
		expect(await p1Settled).toBe('resolved')
		// The fatally-failed partition is rejected on its own promise.
		expect(await p0Settled).toBe('rejected')
		// The retriable partition was actually retried (a second ShareAcknowledge).
		expect(sendAcknowledge).toHaveBeenCalledTimes(2)
	})
})
