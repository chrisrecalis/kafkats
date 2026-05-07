import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { ConsumerGroup } from '@/consumer/consumer-group.js'
import { ConsumerGroupState } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('ConsumerGroup heartbeat scheduling', () => {
	beforeEach(() => {
		vi.useFakeTimers()
	})

	afterEach(() => {
		vi.useRealTimers()
		vi.restoreAllMocks()
	})

	function createGroupWithCoordinator(heartbeatResults: Array<{ errorCode: ErrorCode }>) {
		const heartbeatMock = vi.fn()
		for (const r of heartbeatResults) {
			heartbeatMock.mockResolvedValueOnce(r)
		}
		// Default after the queued results — keep returning None to verify continued ticking.
		heartbeatMock.mockResolvedValue({ errorCode: ErrorCode.None })

		const coordinator = {
			heartbeat: heartbeatMock,
			joinGroup: vi.fn(),
			syncGroup: vi.fn(),
			leaveGroup: vi.fn(),
			offsetCommit: vi.fn(),
		}

		const cluster = {
			getCoordinator: vi.fn().mockResolvedValue(coordinator),
			invalidateCoordinator: vi.fn(),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const group = new ConsumerGroup(cluster as any, {
			groupId: 'g1',
			sessionTimeoutMs: 30_000,
			rebalanceTimeoutMs: 60_000,
			heartbeatIntervalMs: 100,
			groupInstanceId: undefined,
		})

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const groupAny = group as any
		groupAny.coordinator = coordinator
		groupAny.memberId = 'member-1'
		groupAny.generationId = 7
		groupAny.state = ConsumerGroupState.STABLE
		groupAny.abortController = new AbortController()
		groupAny.ownedPartitions = []

		return { group, coordinator, heartbeatMock, groupAny }
	}

	it('keeps rescheduling heartbeats after RebalanceInProgress', async () => {
		const { heartbeatMock, groupAny } = createGroupWithCoordinator([
			{ errorCode: ErrorCode.RebalanceInProgress },
			{ errorCode: ErrorCode.None },
			{ errorCode: ErrorCode.None },
		])

		groupAny.startHeartbeat()

		// First fire (after 100ms): RebalanceInProgress → emit rebalance, reschedule
		await vi.advanceTimersByTimeAsync(100)
		expect(heartbeatMock).toHaveBeenCalledTimes(1)

		// Second fire (after another 100ms): None → reschedule
		await vi.advanceTimersByTimeAsync(100)
		expect(heartbeatMock).toHaveBeenCalledTimes(2)

		// Third fire: None → reschedule
		await vi.advanceTimersByTimeAsync(100)
		expect(heartbeatMock).toHaveBeenCalledTimes(3)
	})

	it('keeps rescheduling heartbeats after IllegalGeneration handleHeartbeatError', async () => {
		const { heartbeatMock, groupAny } = createGroupWithCoordinator([
			{ errorCode: ErrorCode.IllegalGeneration },
			{ errorCode: ErrorCode.None },
		])

		groupAny.startHeartbeat()

		await vi.advanceTimersByTimeAsync(100)
		expect(heartbeatMock).toHaveBeenCalledTimes(1)

		await vi.advanceTimersByTimeAsync(100)
		expect(heartbeatMock).toHaveBeenCalledTimes(2)
	})
})
