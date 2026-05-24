import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer.addPartitionsToTransaction top-level error handling', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let txnCoordinator: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		txnCoordinator = createMockBroker(1)
		mockCluster.getCoordinator.mockResolvedValue(txnCoordinator)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer() {
		return new Producer(mockCluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
		})
	}

	it('throws on a top-level error and does not mark the partitions as added', async () => {
		txnCoordinator.addPartitionsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.InvalidTxnState,
			// Broker rejected the whole request at the top level; no per-partition results.
			results: [],
		})

		const producer = makeProducer()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const p = producer as any

		await expect(p.addPartitionsToTransaction(TOPIC, [0])).rejects.toThrow()
		expect(p.partitionsInTransaction.has(`${TOPIC}:0`)).toBe(false)
	})

	it('invalidates the transaction coordinator on a top-level coordinator error', async () => {
		txnCoordinator.addPartitionsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.NotCoordinator,
			results: [],
		})

		const producer = makeProducer()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const p = producer as any

		await expect(p.addPartitionsToTransaction(TOPIC, [0])).rejects.toThrow()
		expect(mockCluster.invalidateCoordinator).toHaveBeenCalledWith('TRANSACTION', 'tx-1')
	})

	it('marks partitions added when the request fully succeeds', async () => {
		txnCoordinator.addPartitionsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			results: [{ name: TOPIC, resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
		})

		const producer = makeProducer()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const p = producer as any

		await p.addPartitionsToTransaction(TOPIC, [0])
		expect(p.partitionsInTransaction.has(`${TOPIC}:0`)).toBe(true)
	})
})
