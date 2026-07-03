import { describe, expect, it, vi } from 'vitest'
import { EventEmitter } from 'node:events'

import { GroupPartitionProvider } from '@/consumer/partition-provider.js'
import type { PartitionProviderCallbacks } from '@/consumer/partition-provider.js'
import { noopLogger } from '@/logger.js'

// The eager rebalance protocol requires onPartitionsRevoked (which commits the final
// offsets under the OLD generation) to run BEFORE the member rejoins the group.
// Committing after JoinGroup lets the new owner fetch a stale committed offset and
// reprocess records the previous owner already handled.
function buildProvider(protocol: 'eager' | 'cooperative', calls: string[]) {
	const previousAssignment = [
		{ topic: 't', partition: 0 },
		{ topic: 't', partition: 1 },
	]

	const consumerGroup = Object.assign(new EventEmitter(), {
		currentAssignment: previousAssignment,
		currentRebalanceProtocol: protocol,
		currentMemberId: 'member-1',
		currentGenerationId: 5,
		rejoin: vi.fn().mockImplementation(async () => {
			calls.push('rejoin')
			return {
				protocol,
				revoked: [],
				kept: [],
				added: [],
				assignment: [{ topic: 't', partition: 0 }],
				needsRejoin: false,
			}
		}),
		join: vi.fn(),
		stop: vi.fn().mockResolvedValue(undefined),
	})

	const offsetManager = {
		updateGroupState: vi.fn(() => calls.push('updateGroupState')),
		fetchCommittedOffsets: vi.fn().mockResolvedValue(new Map()),
		resolveStartingOffset: vi.fn().mockResolvedValue(0n),
	}

	const cluster = {
		getCoordinator: vi.fn().mockResolvedValue({}),
		getLogger: () => null,
	}

	const provider = new GroupPartitionProvider({
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		consumerGroup: consumerGroup as any,
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster: cluster as any,
		groupId: 'g',
		autoOffsetReset: 'earliest',
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		offsetManager: offsetManager as any,
		logger: noopLogger,
		isRunning: () => true,
	})

	return { provider, consumerGroup, offsetManager, previousAssignment }
}

function buildCallbacks(calls: string[]): PartitionProviderCallbacks {
	return {
		onRebalance: vi.fn().mockResolvedValue(undefined),
		onPartitionsAssigned: vi.fn().mockImplementation(async () => {
			calls.push('assigned')
		}),
		onPartitionsRevoked: vi.fn().mockImplementation(async () => {
			calls.push('revoked')
		}),
		onPartitionsLost: vi.fn(),
		onError: vi.fn(),
	}
}

describe('GroupPartitionProvider eager rebalance ordering', () => {
	it('revokes partitions (and commits) BEFORE rejoining under the eager protocol', async () => {
		const calls: string[] = []
		const { provider, consumerGroup, previousAssignment } = buildProvider('eager', calls)
		const callbacks = buildCallbacks(calls)

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).callbacks = callbacks
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).rebalancePending = true

		await provider.checkAndHandleRebalance()

		// The revoke (which triggers the offset commit under the old generation) must
		// come before rejoin, and before updateGroupState installs the new generation.
		expect(calls.indexOf('revoked')).toBeGreaterThanOrEqual(0)
		expect(calls.indexOf('revoked')).toBeLessThan(calls.indexOf('rejoin'))
		expect(calls.indexOf('revoked')).toBeLessThan(calls.indexOf('updateGroupState'))
		expect(calls.indexOf('assigned')).toBeGreaterThan(calls.indexOf('rejoin'))

		// All previously-owned partitions are revoked exactly once
		expect(callbacks.onPartitionsRevoked).toHaveBeenCalledTimes(1)
		expect(callbacks.onPartitionsRevoked).toHaveBeenCalledWith(previousAssignment)
		expect(consumerGroup.rejoin).toHaveBeenCalledTimes(1)
	})

	it('does not revoke before rejoin under the cooperative protocol (KIP-429)', async () => {
		const calls: string[] = []
		const { provider } = buildProvider('cooperative', calls)
		const callbacks = buildCallbacks(calls)

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).callbacks = callbacks
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(provider as any).rebalancePending = true

		await provider.checkAndHandleRebalance()

		// Cooperative: nothing revoked eagerly, nothing revoked at all here (rejoin
		// returned an empty revoked set), so onPartitionsRevoked is never invoked.
		expect(callbacks.onPartitionsRevoked).not.toHaveBeenCalled()
		expect(calls[0]).toBe('rejoin')
	})
})
