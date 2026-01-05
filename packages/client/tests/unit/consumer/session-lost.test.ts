import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'

describe('Consumer', () => {
	it('clears sessionLost after partitionsAssigned so commits resume after rejoin', async () => {
		const cluster = { getLogger: () => null } as any
		const consumer = new Consumer(cluster, { groupId: 'g' } as any)
		const consumerAny = consumer as any

		consumerAny.commitOffsets = true

		consumerAny.fetchManager = {
			setPartitions: vi.fn(),
			removePartitions: vi.fn(),
		}
		consumerAny.offsetManager = {
			commitPendingOffsets: vi.fn(),
			clearPartitions: vi.fn(),
		}
		consumerAny.resolvePartitionOffsets = vi.fn().mockResolvedValue([{ topic: 't', partition: 0, offset: 0n }])

		const callbacks = consumerAny.createProviderCallbacks()
		const partitions = [{ topic: 't', partition: 0 }]

		callbacks.onPartitionsLost(partitions)
		expect(consumerAny.sessionLost).toBe(true)

		await callbacks.onPartitionsAssigned(partitions)
		expect(consumerAny.sessionLost).toBe(false)

		await callbacks.onPartitionsRevoked(partitions)
		expect(consumerAny.offsetManager.commitPendingOffsets).toHaveBeenCalledTimes(1)
	})
})

