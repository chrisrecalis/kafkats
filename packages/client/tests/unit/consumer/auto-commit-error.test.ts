import { describe, expect, it, vi } from 'vitest'

import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError } from '@/client/errors.js'
import type { Cluster } from '@/client/cluster.js'

describe('OffsetManager auto-commit error handling', () => {
	function makeManagerWithCommitError(error: Error) {
		const fakeCoordinator = {
			offsetCommit: vi.fn().mockRejectedValue(error),
		}
		const cluster = {
			getCoordinator: vi.fn().mockResolvedValue(fakeCoordinator),
			getLogger: () => null,
		}
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const m = new OffsetManager(cluster as unknown as Cluster, 'g1') as any
		// Simulate an active group state.
		m.coordinator = fakeCoordinator
		m.memberId = 'mem-1'
		m.generationId = 7
		// Provide an assigned partition with a consumed offset so commit attempts
		// fire (it skips if there are none).
		m.assignedPartitions.add('t:0')
		m.consumedOffsets.set('t:0', 42n)
		return m
	}

	it('invokes onCommitError when commitPendingOffsets throws', async () => {
		const protocolError = new KafkaProtocolError(ErrorCode.IllegalGeneration, 'illegal generation')
		const manager = makeManagerWithCommitError(protocolError)

		const handler = vi.fn()
		manager.setCommitErrorHandler(handler)

		// Drive a single auto-commit cycle. Use a short interval and let it tick.
		vi.useFakeTimers()
		try {
			manager.startAutoCommit(10)
			await vi.advanceTimersByTimeAsync(15)
			// Allow the .catch microtask to flush.
			await Promise.resolve()
			await Promise.resolve()
			expect(handler).toHaveBeenCalled()
			expect(handler.mock.calls[0]![0]).toBe(protocolError)
		} finally {
			manager.stopAutoCommit()
			vi.useRealTimers()
		}
	})
})
