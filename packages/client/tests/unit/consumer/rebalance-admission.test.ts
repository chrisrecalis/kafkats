import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'

describe('consumer rebalance admission', () => {
	it('stops starting partition handlers when a rebalance becomes pending', async () => {
		const consumer = new Consumer({ getLogger: () => null } as never, { groupId: 'test-group' })
		const consumerInternal = consumer as unknown as {
			state: string
			abortController: AbortController
			fetchManager: {
				poll: ReturnType<typeof vi.fn>
				isBatchAssigned: ReturnType<typeof vi.fn>
			}
			offsetManager: { startAutoCommit: ReturnType<typeof vi.fn> }
			partitionTracker: {
				startProcessing: ReturnType<typeof vi.fn>
				endProcessing: ReturnType<typeof vi.fn>
			}
			partitionProvider: {
				hasPendingRebalance(): boolean
				checkAndHandleRebalance: ReturnType<typeof vi.fn>
			}
			runPollLoop(
				subscriptions: [],
				batchHandler: (topic: string, partition: number) => Promise<void>,
				concurrency: number,
				autoCommitIntervalMs: number,
				autoCommit: boolean
			): Promise<void>
		}

		const batches = Array.from({ length: 80 }, (_, partition) => ({
			topic: 'test-topic',
			partition,
			records: [],
			assignmentEpoch: 1,
		}))
		let rebalancePending = false
		let handlerCount = 0
		let handlersStartedBeforeRejoin = 0

		consumerInternal.state = 'running'
		consumerInternal.abortController = new AbortController()
		consumerInternal.fetchManager = {
			poll: vi.fn().mockResolvedValueOnce(batches),
			isBatchAssigned: vi.fn().mockReturnValue(true),
		}
		consumerInternal.offsetManager = { startAutoCommit: vi.fn() }
		consumerInternal.partitionTracker = {
			startProcessing: vi.fn().mockReturnValue(true),
			endProcessing: vi.fn(),
		}
		consumerInternal.partitionProvider = {
			hasPendingRebalance: () => rebalancePending,
			checkAndHandleRebalance: vi.fn().mockImplementation(async () => {
				if (!rebalancePending) return
				handlersStartedBeforeRejoin = handlerCount
				rebalancePending = false
				consumerInternal.state = 'stopping'
				consumerInternal.abortController.abort()
			}),
		}

		await consumerInternal.runPollLoop(
			[],
			async () => {
				handlerCount++
				if (handlerCount === 1) rebalancePending = true
			},
			1,
			5000,
			false
		)

		expect(handlersStartedBeforeRejoin).toBe(1)
		expect(handlerCount).toBe(1)
	})
})
