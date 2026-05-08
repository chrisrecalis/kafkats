import { describe, expect, it, vi } from 'vitest'
import { EventEmitter } from 'node:events'

import { GroupPartitionProvider } from '@/consumer/partition-provider.js'
import type { PartitionProviderCallbacks } from '@/consumer/partition-provider.js'
import { noopLogger } from '@/logger.js'

// Build the smallest possible GroupPartitionProvider that drives checkAndHandleRebalance
// against a stub ConsumerGroup. The point is to assert the *contract* (rejoin/revoke don't
// happen until onRebalance resolves) — not just the typed signature.
function buildProvider() {
	const consumerGroup = Object.assign(new EventEmitter(), {
		currentAssignment: [],
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
		ensureMetadata: vi.fn().mockResolvedValue(undefined),
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

describe('GroupPartitionProvider.checkAndHandleRebalance awaits onRebalance', () => {
	it('does not invoke consumerGroup.rejoin until onRebalance resolves', async () => {
		const { provider, consumerGroup } = buildProvider()

		let resolveHook: () => void = () => {}
		const hookDone = new Promise<void>(r => {
			resolveHook = r
		})

		const callbacks: PartitionProviderCallbacks = {
			onRebalance: vi.fn(async () => {
				await hookDone
			}),
			onPartitionsAssigned: vi.fn().mockResolvedValue(undefined),
			onPartitionsRevoked: vi.fn().mockResolvedValue(undefined),
			onPartitionsLost: vi.fn(),
			onError: vi.fn(),
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).callbacks = callbacks
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).rebalancePending = true

		const checkPromise = provider.checkAndHandleRebalance()

		// Yield enough microtasks for onRebalance to start but stay blocked on the deferred.
		for (let i = 0; i < 5; i++) await Promise.resolve()

		expect(callbacks.onRebalance).toHaveBeenCalledTimes(1)
		expect(consumerGroup.rejoin).not.toHaveBeenCalled()

		resolveHook()
		await checkPromise

		expect(consumerGroup.rejoin).toHaveBeenCalledTimes(1)
	})

	it('stops the consumer group and surfaces the error when onRebalance throws', async () => {
		const { provider, consumerGroup } = buildProvider()

		const failure = new Error('eos commit failed')
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

		await provider.checkAndHandleRebalance()

		expect(consumerGroup.rejoin).not.toHaveBeenCalled()
		expect(consumerGroup.stop).toHaveBeenCalledTimes(1)
		expect(callbacks.onError).toHaveBeenCalledWith(failure)
	})
})
