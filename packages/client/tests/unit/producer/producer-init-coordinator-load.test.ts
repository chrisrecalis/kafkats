import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, type MockBroker } from './_helpers.js'

/**
 * Regression: InitProducerId previously used only the produce retry budget (retries + 1,
 * default 4) and threw fatally on CoordinatorLoadInProgress. A transaction coordinator that
 * is still loading its state log (fresh cluster, broker restart, coordinator move) returns
 * that retriable error for longer than a few attempts, so producer init would fail. It must
 * now retry past the small budget (mirrors the Java client blocking up to max.block.ms).
 */
describe('Producer InitProducerId retries CoordinatorLoadInProgress', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let txnCoordinator: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		txnCoordinator = createMockBroker(1)
		mockCluster.getCoordinator.mockResolvedValue(txnCoordinator)
		mockCluster.getAnyBroker.mockResolvedValue(txnCoordinator)
		txnCoordinator.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('retries past more CoordinatorLoadInProgress responses than the produce retry budget', async () => {
		const load = {
			throttleTimeMs: 0,
			errorCode: ErrorCode.CoordinatorLoadInProgress,
			producerId: -1n,
			producerEpoch: -1,
		}
		const ok = { throttleTimeMs: 0, errorCode: ErrorCode.None, producerId: 7n, producerEpoch: 3 }
		// Six coordinator-load responses exceeds the old budget (retries + 1 = 4); pre-fix
		// this threw "InitProducerId failed: CoordinatorLoadInProgress".
		txnCoordinator.initProducerId
			.mockResolvedValueOnce(load)
			.mockResolvedValueOnce(load)
			.mockResolvedValueOnce(load)
			.mockResolvedValueOnce(load)
			.mockResolvedValueOnce(load)
			.mockResolvedValueOnce(load)
			.mockResolvedValue(ok)

		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 3,
			retryBackoffMs: 1,
			maxRetryBackoffMs: 1,
			transactionalId: 'tx-1',
		})

		await expect(producer.transaction(async () => {})).resolves.toBeUndefined()
		// 6 CoordinatorLoadInProgress + 1 success = 7 calls — proves init retried well past the
		// old produce budget (retries + 1 = 4) before succeeding.
		expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(7)
		// And the retried success identity (producerId 7n / epoch 3) propagated into txn state.
		expect(txnCoordinator.endTxn).toHaveBeenCalledWith(
			expect.objectContaining({ producerId: 7n, producerEpoch: 3 })
		)
	})

	it('fails fast on a non-retriable InitProducerId error instead of blocking the deadline', async () => {
		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.ClusterAuthorizationFailed,
			producerId: -1n,
			producerEpoch: -1,
		})

		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 3,
			retryBackoffMs: 1,
			maxRetryBackoffMs: 1,
			transactionalId: 'tx-1',
		})

		await expect(producer.transaction(async () => {})).rejects.toThrow(/InitProducerId failed/)
		// Non-retriable code must escape the retry loop on the first attempt (no deadline blocking).
		expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
	})
})
