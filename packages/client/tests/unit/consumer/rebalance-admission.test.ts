import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'

describe('consumer rebalance admission', () => {
	it('stops starting partition handlers when a rebalance becomes pending', async () => {
		const consumer = new Consumer({ getLogger: () => null } as never, { groupId: 'test-group' })
		const consumerInternal = consumer as unknown as {
			state: string
			abortController: AbortController
			undelivered: unknown[]
			fetchManager: {
				poll: ReturnType<typeof vi.fn>
				isBatchAssigned: ReturnType<typeof vi.fn>
				wakePoll: ReturnType<typeof vi.fn>
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
			// Like the FetchManager, it queues what it drains on the consumer's undelivered batches. A second poll fails
			// the run: answering at once forever would spin the loop instead of failing.
			poll: vi
				.fn()
				.mockImplementationOnce(async () => {
					consumerInternal.undelivered.push(...batches)
				})
				.mockRejectedValue(new Error('poll() called after the scripted answer was used up')),
			isBatchAssigned: vi.fn().mockReturnValue(true),
			wakePoll: vi.fn(),
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
