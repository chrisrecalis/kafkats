/**
 * Unit tests for ConsumerGroup.leave() KIP-345 static membership behaviour.
 *
 * Static members (groupInstanceId set) must NOT send LeaveGroup on graceful shutdown.
 * Sending LeaveGroup as a static member triggers an immediate rebalance, defeating the
 * purpose of static membership.  Dynamic members (no groupInstanceId) MUST send
 * LeaveGroup so the broker can rebalance immediately rather than waiting for the session
 * timeout.
 *
 * See: packages/client/src/consumer/consumer-group.ts leave()
 */

import { describe, expect, it, vi } from 'vitest'

import { ConsumerGroup } from '@/consumer/consumer-group.js'
import { ConsumerGroupState } from '@/consumer/types.js'

function buildGroup(groupInstanceId: string | undefined) {
	const leaveGroupMock = vi.fn().mockResolvedValue({ errorCode: 0, members: [] })

	const coordinator = {
		heartbeat: vi.fn().mockResolvedValue({ errorCode: 0 }),
		joinGroup: vi.fn(),
		syncGroup: vi.fn(),
		leaveGroup: leaveGroupMock,
		offsetCommit: vi.fn(),
	}

	const cluster = {
		getCoordinator: vi.fn().mockResolvedValue(coordinator),
		invalidateCoordinator: vi.fn(),
		getLogger: () => null,
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const group = new ConsumerGroup(cluster as any, {
		groupId: 'test-group',
		groupInstanceId,
		sessionTimeoutMs: 30_000,
		rebalanceTimeoutMs: 60_000,
		heartbeatIntervalMs: 3_000,
	})

	// Inject coordinator and member state directly so we skip the full join flow
	// (same technique as heartbeat-reschedule.test.ts)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const groupAny = group as any
	groupAny.coordinator = coordinator
	groupAny.memberId = 'test-member-1'
	groupAny.generationId = 5
	groupAny.state = ConsumerGroupState.STABLE
	groupAny.abortController = new AbortController()
	groupAny.ownedPartitions = [{ topic: 'test-topic', partition: 0 }]

	return { group, groupAny, coordinator, leaveGroupMock }
}

describe('ConsumerGroup.leave() — KIP-345 static membership', () => {
	it('static member: does NOT call leaveGroup (skips LeaveGroup to avoid triggering rebalance)', async () => {
		const { group, leaveGroupMock } = buildGroup('static-instance-0')

		await group.leave()

		expect(leaveGroupMock).not.toHaveBeenCalled()
	})

	it('static member: still stops heartbeat and transitions to UNJOINED after leave()', async () => {
		const { group, groupAny } = buildGroup('static-instance-0')

		// Plant a heartbeat timer so we can confirm stopHeartbeat() is called
		const fakeTimer = setTimeout(() => {}, 100_000)
		groupAny.heartbeatTimer = fakeTimer

		await group.leave()

		expect(groupAny.heartbeatTimer).toBeNull()
		expect(group.groupState).toBe(ConsumerGroupState.UNJOINED)
		clearTimeout(fakeTimer)
	})

	it('static member: clears memberId / generationId / assignment after leave()', async () => {
		const { group, groupAny } = buildGroup('static-instance-0')

		await group.leave()

		expect(groupAny.memberId).toBe('')
		expect(groupAny.generationId).toBe(-1)
		expect(groupAny.assignment).toEqual([])
		expect(groupAny.ownedPartitions).toEqual([])
	})

	it('dynamic member: calls leaveGroup exactly once with groupInstanceId: null', async () => {
		const { group, leaveGroupMock } = buildGroup(undefined)

		await group.leave()

		expect(leaveGroupMock).toHaveBeenCalledTimes(1)
		expect(leaveGroupMock).toHaveBeenCalledWith(
			expect.objectContaining({
				groupId: 'test-group',
				members: expect.arrayContaining([
					expect.objectContaining({
						memberId: 'test-member-1',
						groupInstanceId: null,
					}),
				]),
			})
		)
	})

	it('dynamic member: still transitions to UNJOINED and clears state even if leaveGroup throws', async () => {
		const { group, groupAny, leaveGroupMock } = buildGroup(undefined)
		leaveGroupMock.mockRejectedValueOnce(new Error('connection lost'))

		// Should not throw — leave() swallows errors
		await expect(group.leave()).resolves.toBeUndefined()

		expect(group.groupState).toBe(ConsumerGroupState.UNJOINED)
		expect(groupAny.memberId).toBe('')
	})
})
