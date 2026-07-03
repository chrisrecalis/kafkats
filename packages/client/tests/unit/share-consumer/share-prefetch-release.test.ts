import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { ACK_RELEASE } from '@/share-consumer/ack-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

function shareFetchResponse(
	acquiredRecords: Array<{ firstOffset: bigint; lastOffset: bigint; deliveryCount: number }>
) {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		errorMessage: null,
		acquisitionLockTimeoutMs: 30000,
		topics: [
			{
				topicId: TOPIC_ID,
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						acknowledgeErrorCode: ErrorCode.None,
						acquiredRecords,
						recordsData: null,
					},
				],
			},
		],
		nodeEndpoints: [],
	}
}

const shareAcknowledgeSuccess = {
	throttleTimeMs: 0,
	errorCode: ErrorCode.None,
	errorMessage: null,
	acquisitionLockTimeoutMs: 30000,
	topics: [
		{
			topicId: TOPIC_ID,
			partitions: [
				{
					partitionIndex: 0,
					errorCode: ErrorCode.None,
					errorMessage: null,
					currentLeader: { leaderId: 1, leaderEpoch: 0 },
				},
			],
		},
	],
	nodeEndpoints: [],
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumerWithBroker(broker: any): any {
	const cluster = {
		getLogger: () => null,
		getLeaderForPartition: vi.fn().mockResolvedValue(broker),
		getBroker: vi.fn().mockResolvedValue(broker),
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const consumer = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
	consumer.abortController = new AbortController()
	consumer.state = 'running'
	consumer.topicIdByName.set('t', TOPIC_ID)
	consumer.topicNameById.set(TOPIC_ID, 't')
	return consumer
}

describe('ShareConsumer warm-up prefetch', () => {
	it('releases the records the prefetch acquired instead of abandoning them until the lock timeout', async () => {
		const broker = {
			nodeId: 1,
			shareFetch: vi
				.fn()
				.mockResolvedValue(shareFetchResponse([{ firstOffset: 5n, lastOffset: 9n, deliveryCount: 1 }])),
			shareAcknowledge: vi.fn().mockResolvedValue(shareAcknowledgeSuccess),
		}
		const consumer = makeConsumerWithBroker(broker)

		await consumer.prefetchAssignedPartitions([{ topic: 't', partition: 0 }])

		// The prefetch acquired offsets 5..9; abandoning them would delay their first delivery by the
		// acquisition-lock timeout and burn a delivery attempt. They must be handed back with RELEASE.
		expect(broker.shareAcknowledge).toHaveBeenCalledTimes(1)
		const request = broker.shareAcknowledge.mock.calls[0]?.[0]
		expect(request.topics).toEqual([
			{
				topicId: TOPIC_ID,
				partitions: [
					{
						partitionIndex: 0,
						acknowledgementBatches: [{ firstOffset: 5n, lastOffset: 9n, acknowledgeTypes: [ACK_RELEASE] }],
					},
				],
			},
		])
		// The release rides the share session the prefetch fetch just advanced (epoch 1), never epoch 0.
		expect(request.shareSessionEpoch).toBe(1)
	})

	it('sends no ShareAcknowledge when the prefetch acquired nothing', async () => {
		const broker = {
			nodeId: 1,
			shareFetch: vi.fn().mockResolvedValue(shareFetchResponse([])),
			shareAcknowledge: vi.fn().mockResolvedValue(shareAcknowledgeSuccess),
		}
		const consumer = makeConsumerWithBroker(broker)

		await consumer.prefetchAssignedPartitions([{ topic: 't', partition: 0 }])

		expect(broker.shareAcknowledge).not.toHaveBeenCalled()
	})

	it('does not fail the prefetch when the release fails (best-effort)', async () => {
		const broker = {
			nodeId: 1,
			shareFetch: vi
				.fn()
				.mockResolvedValue(shareFetchResponse([{ firstOffset: 0n, lastOffset: 1n, deliveryCount: 1 }])),
			shareAcknowledge: vi.fn().mockRejectedValue(new Error('release failed')),
		}
		const consumer = makeConsumerWithBroker(broker)

		await expect(consumer.prefetchAssignedPartitions([{ topic: 't', partition: 0 }])).resolves.toBeUndefined()
	})
})
