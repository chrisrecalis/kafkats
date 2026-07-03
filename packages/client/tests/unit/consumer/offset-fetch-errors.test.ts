import { describe, expect, it, vi } from 'vitest'

import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { Cluster } from '@/client/cluster.js'

// A partition-level OffsetFetch error must never be silently treated as "no committed
// offset": that falls through to autoOffsetReset and loses the consumer's position.
// Retriable errors are retried; non-retriable ones must throw.
function makeManager(responses: unknown[]) {
	const offsetFetch = vi.fn()
	for (const response of responses) {
		offsetFetch.mockResolvedValueOnce(response)
	}
	const coordinator = { offsetFetch }
	const cluster = {
		invalidateCoordinator: vi.fn(),
		getCoordinator: vi.fn().mockResolvedValue(coordinator),
	}
	// Short apiTimeoutMs bounds the retry loop in the failure cases.
	const manager = new OffsetManager(cluster as unknown as Cluster, 'g1', undefined, undefined, 1_500)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	;(manager as any).coordinator = coordinator
	return { manager, offsetFetch }
}

function response(partitions: Array<{ partitionIndex: number; committedOffset: bigint; errorCode: number }>) {
	return {
		errorCode: ErrorCode.None,
		topics: [{ name: 't', partitions }],
	}
}

describe('OffsetManager.fetchCommittedOffsets partition-level errors', () => {
	it('throws on a non-retriable partition-level error instead of resetting the offset', async () => {
		const { manager } = makeManager([
			response([{ partitionIndex: 0, committedOffset: -1n, errorCode: ErrorCode.GroupAuthorizationFailed }]),
		])

		await expect(manager.fetchCommittedOffsets([{ topic: 't', partition: 0 }])).rejects.toThrow(/OffsetFetch/)
	})

	it('retries a retriable partition-level error and returns the committed offset', async () => {
		const { manager, offsetFetch } = makeManager([
			response([{ partitionIndex: 0, committedOffset: -1n, errorCode: ErrorCode.UnstableOffsetCommit }]),
			response([{ partitionIndex: 0, committedOffset: 42n, errorCode: ErrorCode.None }]),
		])

		const result = await manager.fetchCommittedOffsets([{ topic: 't', partition: 0 }])

		expect(offsetFetch).toHaveBeenCalledTimes(2)
		expect(result.get('t:0')).toBe(42n)
	})

	it('still maps a clean -1 committedOffset to "no committed offset"', async () => {
		const { manager } = makeManager([
			response([{ partitionIndex: 0, committedOffset: -1n, errorCode: ErrorCode.None }]),
		])

		const result = await manager.fetchCommittedOffsets([{ topic: 't', partition: 0 }])
		expect(result.size).toBe(0)
	})
})
