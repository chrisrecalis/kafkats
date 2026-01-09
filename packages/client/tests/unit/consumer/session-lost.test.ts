import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'

describe('Consumer', () => {
	it('clears sessionLost after partitionsAssigned so commits resume after rejoin', async () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = { getLogger: () => null } as any
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumer = new Consumer(cluster, { groupId: 'g' } as any)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumerAny = consumer as any

		consumerAny.commitOffsets = true

		consumerAny.fetchManager = {
			addPartitions: vi.fn(),
			removePartitions: vi.fn(),
		}
		consumerAny.offsetManager = {
			addAssignedPartitions: vi.fn(),
			removeAssignedPartitions: vi.fn(),
			commitPendingOffsets: vi.fn(),
			clearPartitions: vi.fn(),
		}
		consumerAny.partitionTracker = {
			assign: vi.fn(),
			revoke: vi.fn().mockResolvedValue(undefined),
			endProcessing: vi.fn(),
		}

		const callbacks = consumerAny.createProviderCallbacks()
		const partitions = [{ topic: 't', partition: 0 }]
		const partitionsWithOffsets = [{ topic: 't', partition: 0, offset: 0n }]

		callbacks.onPartitionsLost(partitions)
		expect(consumerAny.sessionLost).toBe(true)

		await callbacks.onPartitionsAssigned(partitionsWithOffsets)
		expect(consumerAny.sessionLost).toBe(false)

		await callbacks.onPartitionsRevoked(partitions)
		expect(consumerAny.offsetManager.commitPendingOffsets).toHaveBeenCalledTimes(1)
	})
})
