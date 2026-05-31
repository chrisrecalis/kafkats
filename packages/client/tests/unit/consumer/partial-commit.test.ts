import { describe, expect, it, vi } from 'vitest'

import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { Cluster } from '@/client/cluster.js'

describe('OffsetManager.commitPendingOffsets partial-failure handling', () => {
	it('clears successful partitions even if a sibling errored', async () => {
		const offsetCommit = vi.fn().mockResolvedValue({
			topics: [
				{
					name: 't',
					partitions: [
						{ partitionIndex: 0, errorCode: ErrorCode.None },
						{ partitionIndex: 1, errorCode: ErrorCode.NotCoordinator },
						{ partitionIndex: 2, errorCode: ErrorCode.None },
					],
				},
			],
		})

		const cluster = {
			getCoordinator: vi.fn().mockResolvedValue({ offsetCommit }),
			invalidateCoordinator: vi.fn(),
			getLogger: () => null,
		}
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const manager = new OffsetManager(cluster as unknown as Cluster, 'g1') as any
		manager.coordinator = { offsetCommit }
		manager.memberId = 'm1'
		manager.generationId = 5

		manager.assignedPartitions.add('t:0')
		manager.assignedPartitions.add('t:1')
		manager.assignedPartitions.add('t:2')
		manager.consumedOffsets.set('t:0', 10n)
		manager.consumedOffsets.set('t:1', 20n)
		manager.consumedOffsets.set('t:2', 30n)

		await expect(manager.commitPendingOffsets()).rejects.toThrow(/NotCoordinator|OffsetCommit failed/i)

		expect(manager.consumedOffsets.get('t:1')).toBe(20n)
		expect(manager.consumedOffsets.has('t:0')).toBe(false)
		expect(manager.consumedOffsets.has('t:2')).toBe(false)
	})
})
