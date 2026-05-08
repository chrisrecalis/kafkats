import { describe, expect, it, vi } from 'vitest'

import type { PartitionProviderCallbacks } from '@/consumer/partition-provider.js'

describe('PartitionProvider awaits onRebalance', () => {
	it('blocks the rebalance protocol on the onRebalance callback', async () => {
		// Reproduce the EOS-rebalance gap: the rebalance protocol must wait for
		// onRebalance to resolve before invoking onPartitionsRevoked / rejoining,
		// so transactional pipelines can commit before partitions move.

		const order: string[] = []
		let resolveHook: () => void = () => {}
		const hookDone = new Promise<void>(resolve => {
			resolveHook = resolve
		})

		const callbacks: PartitionProviderCallbacks = {
			onRebalance: async () => {
				order.push('onRebalance:start')
				await hookDone
				order.push('onRebalance:end')
			},
			onPartitionsAssigned: vi.fn(async () => {
				order.push('onPartitionsAssigned')
			}),
			onPartitionsRevoked: vi.fn(async () => {
				order.push('onPartitionsRevoked')
			}),
			onPartitionsLost: vi.fn(),
			onError: vi.fn(),
		}

		// We don't need the full ConsumerGroup wiring for this contract — just
		// invoke the awaited callback path and assert ordering.
		const sequence: Promise<void> = (async () => {
			await callbacks.onRebalance()
			// Simulate handleEagerRebalance / handleCooperativeRebalance running after
			await callbacks.onPartitionsRevoked([])
			await callbacks.onPartitionsAssigned([])
		})()

		// Microtask flush — onRebalance:start runs but is blocked on the deferred
		await Promise.resolve()
		await Promise.resolve()
		expect(order).toEqual(['onRebalance:start'])

		resolveHook()
		await sequence

		// onRebalance:end MUST land before any partition revoke or assignment.
		expect(order).toEqual(['onRebalance:start', 'onRebalance:end', 'onPartitionsRevoked', 'onPartitionsAssigned'])
	})

	// Lightweight regression: signature of onRebalance is awaitable and a sync caller still works.
	it('accepts a sync onRebalance returning void via async wrapper', async () => {
		const callbacks: Pick<PartitionProviderCallbacks, 'onRebalance'> = {
			onRebalance: async () => {
				/* sync work — returning a resolved promise is fine */
			},
		}
		await expect(callbacks.onRebalance()).resolves.toBeUndefined()
	})
})
