import { EventEmitter } from 'node:events'
import { describe, expect, it, vi } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import { TimeoutError } from '@/client/errors.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { GroupPartitionProvider, type PartitionProviderCallbacks } from '@/consumer/partition-provider.js'
import { noopLogger } from '@/logger.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const assignment = [{ topic: 't', partition: 0 }]

/**
 * A rebalance whose group coordinator does not answer the first OffsetFetch within defaultApiTimeoutMs (50 ms here),
 * then answers offset 42: the real provider and offset manager.
 */
function rebalanceWithUnansweredFetch() {
	const coordinator = {
		offsetFetch: vi
			.fn()
			.mockReturnValueOnce(new Promise(() => {}))
			.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				topics: [
					{
						name: 't',
						partitions: [
							{
								partitionIndex: 0,
								committedOffset: 42n,
								committedLeaderEpoch: -1,
								metadata: '',
								errorCode: ErrorCode.None,
							},
						],
					},
				],
			}),
	}
	const cluster = { getCoordinator: vi.fn().mockResolvedValue(coordinator), invalidateCoordinator: vi.fn() }
	const consumerGroup = Object.assign(new EventEmitter(), {
		currentAssignment: [],
		currentRebalanceProtocol: 'eager' as const,
		currentMemberId: 'member-1',
		currentGenerationId: 2,
		rejoin: vi.fn().mockResolvedValue({
			protocol: 'eager',
			assignment,
			added: assignment,
			revoked: [],
			kept: [],
			needsRejoin: false,
		}),
		stop: vi.fn().mockResolvedValue(undefined),
	})
	const provider = new GroupPartitionProvider({
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		consumerGroup: consumerGroup as any,
		cluster: cluster as unknown as Cluster,
		groupId: 'g',
		autoOffsetReset: 'none',
		offsetManager: new OffsetManager(cluster as unknown as Cluster, 'g', undefined, undefined, 50),
		logger: noopLogger,
		isRunning: () => true,
	})
	const callbacks: PartitionProviderCallbacks = {
		onRebalancePending: vi.fn(),
		onRebalance: vi.fn().mockResolvedValue(undefined),
		onPartitionsAssigned: vi.fn().mockResolvedValue(undefined),
		onPartitionsRevoked: vi.fn().mockResolvedValue(undefined),
		onPartitionsLost: vi.fn().mockResolvedValue(undefined),
		onRebalanceComplete: vi.fn(),
		onOffsetReset: vi.fn(),
		onError: vi.fn(),
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	;(provider as any).callbacks = callbacks
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	;(provider as any).rebalanceRequested = true
	return { provider, callbacks, coordinator, consumerGroup }
}

describe('the committed offsets of a rebalance', () => {
	it('are fetched again after a fetch that ran out of time, like while the cluster is unreachable', async () => {
		const { provider, callbacks, coordinator, consumerGroup } = rebalanceWithUnansweredFetch()

		await provider.checkAndHandleRebalance()

		// Java's consumer tries again at its next poll: the rebalance is not failed, the group not left.
		expect(coordinator.offsetFetch).toHaveBeenCalledTimes(2)
		expect(callbacks.onError).toHaveBeenCalledWith(expect.any(TimeoutError))
		expect(callbacks.onPartitionsAssigned).toHaveBeenCalledWith([
			{ topic: 't', partition: 0, offset: 42n, committed: { offset: 42n, metadata: null } },
		])
		expect(callbacks.onRebalanceComplete).toHaveBeenCalledTimes(1)
		expect(consumerGroup.stop).not.toHaveBeenCalled()
	})
})
