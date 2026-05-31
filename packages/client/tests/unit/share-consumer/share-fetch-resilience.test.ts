import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { KafkaProtocolError } from '@/client/errors.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function makeConsumer(getBroker: any): any {
	const cluster = { getLogger: () => null, getBroker }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const consumer = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
	consumer.abortController = new AbortController()
	consumer.state = 'running'
	return consumer
}

describe('ShareConsumer fetch-loop resilience', () => {
	it('swallows a transport error from a leader (returns null) and flags metadata stale, not fatal', async () => {
		const consumer = makeConsumer(vi.fn().mockResolvedValue({ nodeId: 1 }))
		consumer.shareFetch = vi.fn().mockRejectedValue(new ConnectionClosedError('Connection closed unexpectedly'))

		const result = await consumer.buildShareFetchTask(1, [{ topicId: 'tid', partitions: [0] }])

		expect(result).toBeNull()
		expect(consumer.fetchMetadataStale).toBe(true)
	})

	it('swallows a share-session error (returns null) without flagging metadata stale', async () => {
		const consumer = makeConsumer(vi.fn().mockResolvedValue({ nodeId: 1 }))
		consumer.shareFetch = vi.fn().mockRejectedValue(new KafkaProtocolError(ErrorCode.InvalidShareSessionEpoch))

		const result = await consumer.buildShareFetchTask(1, [{ topicId: 'tid', partitions: [0] }])

		expect(result).toBeNull()
		expect(consumer.fetchMetadataStale).toBe(false)
	})

	it('rethrows a non-retriable protocol error (fatal)', async () => {
		const consumer = makeConsumer(vi.fn().mockResolvedValue({ nodeId: 1 }))
		consumer.shareFetch = vi.fn().mockRejectedValue(new KafkaProtocolError(ErrorCode.ClusterAuthorizationFailed))

		await expect(consumer.buildShareFetchTask(1, [{ topicId: 'tid', partitions: [0] }])).rejects.toThrow()
	})
})
