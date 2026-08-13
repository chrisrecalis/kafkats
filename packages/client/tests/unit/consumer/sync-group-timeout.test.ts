import { describe, expect, it, vi } from 'vitest'

import { ConsumerGroup } from '@/consumer/consumer-group.js'

describe('ConsumerGroup SyncGroup timeout', () => {
	it('passes the resolved rebalance timeout to the coordinator broker', async () => {
		const failure = new Error('stop after capturing request')
		const coordinator = {
			syncGroup: vi.fn().mockRejectedValue(failure),
		}
		const group = new ConsumerGroup({} as never, {
			groupId: 'test-group',
			rebalanceTimeoutMs: 75000,
		})
		const groupInternal = group as unknown as {
			coordinator: typeof coordinator
			memberId: string
			generationId: number
			syncGroup(topics: string[]): Promise<void>
		}
		groupInternal.coordinator = coordinator
		groupInternal.memberId = 'test-member'
		groupInternal.generationId = 3

		await expect(groupInternal.syncGroup([])).rejects.toBe(failure)

		expect(coordinator.syncGroup).toHaveBeenCalledWith(
			expect.objectContaining({
				groupId: 'test-group',
				generationId: 3,
				memberId: 'test-member',
			}),
			75000
		)
	})
})
