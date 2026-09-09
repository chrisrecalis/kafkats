import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'

function batch(partition: number, offset: bigint) {
	return {
		topic: 'test-topic',
		partition,
		records: [{ offset }],
		assignmentEpoch: 1,
	}
}

describe('consumer partition scheduler', () => {
	it('continues polling an idle partition while another partition handler is blocked', async () => {
		const consumer = new Consumer({ getLogger: () => null } as never, { groupId: 'test-group' })
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumerInternal = consumer as any
		let releaseSlowHandler!: () => void
		const slowHandler = new Promise<void>(resolve => {
			releaseSlowHandler = resolve
		})
		let releasedBySafetyTimer = false
		let fastPartitionCalls = 0
		let fastPartitionAdvancedBeforeSlowRelease = false
		const pollExclusions: string[][] = []

		const safetyTimer = setTimeout(() => {
			releasedBySafetyTimer = true
			releaseSlowHandler()
		}, 200)

		consumerInternal.state = 'running'
		consumerInternal.abortController = new AbortController()
		const polls = [[batch(0, 0n)], [batch(1, 0n)], [batch(1, 1n)]]
		consumerInternal.fetchManager = {
			poll: vi.fn().mockImplementation((excluded: ReadonlyMap<string, unknown>) => {
				pollExclusions.push([...excluded.keys()])
				return Promise.resolve(polls.shift() ?? [])
			}),
			isBatchAssigned: vi.fn().mockReturnValue(true),
			wakePoll: vi.fn(),
		}
		consumerInternal.offsetManager = { startAutoCommit: vi.fn() }
		consumerInternal.partitionTracker = {
			startProcessing: vi.fn().mockReturnValue(true),
			endProcessing: vi.fn(),
		}
		consumerInternal.partitionProvider = {
			hasPendingRebalance: () => false,
			checkAndHandleRebalance: vi.fn(),
		}

		await consumerInternal.runPollLoop(
			[],
			async (_topic: string, partition: number) => {
				if (partition === 0) {
					await slowHandler
					return
				}

				fastPartitionCalls++
				if (fastPartitionCalls === 2) {
					fastPartitionAdvancedBeforeSlowRelease = !releasedBySafetyTimer
					releaseSlowHandler()
					consumerInternal.state = 'stopping'
					consumerInternal.abortController.abort()
				}
			},
			2,
			5000,
			false
		)
		clearTimeout(safetyTimer)

		expect(fastPartitionCalls).toBe(2)
		expect(fastPartitionAdvancedBeforeSlowRelease).toBe(true)
		expect(pollExclusions[1]).toContain('test-topic:0')
		expect(pollExclusions[2]).toContain('test-topic:0')
	})
})
