import { describe, expect, it, vi } from 'vitest'
import { EventEmitter } from 'node:events'

import { Consumer } from '@/consumer/consumer.js'
import { GroupPartitionProvider } from '@/consumer/partition-provider.js'
import type { PartitionProviderCallbacks } from '@/consumer/partition-provider.js'
import { noopLogger } from '@/logger.js'

// When the rebalance handler fails, the provider stops the consumer group (LeaveGroup).
// The failure must then be FATAL for the run loop: checkAndHandleRebalance has to reject
// so the poll loop exits, instead of the consumer keeping on fetching partitions it no
// longer owns after having left the group.
function buildProvider() {
	const consumerGroup = Object.assign(new EventEmitter(), {
		currentAssignment: [{ topic: 't', partition: 0 }],
		currentRebalanceProtocol: 'cooperative' as const,
		rejoin: vi.fn().mockResolvedValue({
			protocol: 'cooperative',
			revoked: [],
			kept: [],
			added: [],
			assignment: [],
			needsRejoin: false,
		}),
		join: vi.fn(),
		stop: vi.fn().mockResolvedValue(undefined),
	})

	const provider = new GroupPartitionProvider({
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		consumerGroup: consumerGroup as any,
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster: { getLogger: () => null } as any,
		groupId: 'g',
		autoOffsetReset: 'latest',
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		offsetManager: {} as any,
		logger: noopLogger,
		isRunning: () => true,
	})

	return { provider, consumerGroup }
}

describe('failed rebalance stops the consumer', () => {
	it('checkAndHandleRebalance rejects after stopping the group when the rebalance handler throws', async () => {
		const { provider, consumerGroup } = buildProvider()

		const failure = new Error('rebalance handler failed')
		const callbacks: PartitionProviderCallbacks = {
			onRebalance: vi.fn().mockRejectedValue(failure),
			onPartitionsAssigned: vi.fn(),
			onPartitionsRevoked: vi.fn(),
			onPartitionsLost: vi.fn(),
			onError: vi.fn(),
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).callbacks = callbacks
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).rebalancePending = true

		// Pre-fix this resolved (only calling onError), so the fetch loop kept running on
		// revoked partitions after the group had been left.
		await expect(provider.checkAndHandleRebalance()).rejects.toBe(failure)
		expect(consumerGroup.stop).toHaveBeenCalledTimes(1)
		expect(consumerGroup.rejoin).not.toHaveBeenCalled()
	})

	it('the consumer run loop exits when checkAndHandleRebalance rejects', async () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = { getLogger: () => null } as any
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumer = new Consumer(cluster, { groupId: 'g' } as any)
		const errorListener = vi.fn()
		consumer.on('error', errorListener)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumerAny = consumer as any

		const failure = new Error('rebalance handler failed')
		consumerAny.state = 'running'
		consumerAny.abortController = new AbortController()
		consumerAny.commitOffsets = false
		consumerAny.fetchManager = { poll: vi.fn().mockResolvedValue([]) }
		consumerAny.offsetManager = { startAutoCommit: vi.fn() }
		consumerAny.partitionTracker = {}
		consumerAny.partitionProvider = {
			hasPendingRebalance: () => false,
			checkAndHandleRebalance: vi.fn().mockRejectedValue(failure),
		}

		// The poll loop must terminate with the rebalance failure instead of looping on.
		await expect(consumerAny.runPollLoop([], vi.fn(), 1, 5000, false)).rejects.toBe(failure)
		// The public run wrapper owns error emission. Emitting here as well reports one
		// rebalance failure twice.
		expect(errorListener).not.toHaveBeenCalled()
	})
})
