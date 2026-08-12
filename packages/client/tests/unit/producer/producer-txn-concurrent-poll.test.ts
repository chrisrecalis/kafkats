import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, buildProduceResponse, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer CONCURRENT_TRANSACTIONS fast polling', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let txnCoordinator: MockBroker
	let mockBroker: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		txnCoordinator = createMockBroker(1)
		mockBroker = createMockBroker(2)

		mockCluster.getCoordinator.mockResolvedValue(txnCoordinator)
		mockCluster.getAnyBroker.mockResolvedValue(txnCoordinator)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)

		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		txnCoordinator.addPartitionsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			results: [{ name: TOPIC, resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
		})
		txnCoordinator.endTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
		mockBroker.produce.mockResolvedValue(
			buildProduceResponse([
				{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }] },
			])
		)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function concurrentTxnResponse() {
		return {
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			results: [
				{
					name: TOPIC,
					resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.ConcurrentTransactions }],
				},
			],
		}
	}

	it('polls quickly instead of waiting the full retry backoff', async () => {
		// Two CONCURRENT_TRANSACTIONS responses, then success. With the default
		// 100ms retryBackoffMs this would take >= 200ms; the fast poll stays well under.
		txnCoordinator.addPartitionsToTxn
			.mockResolvedValueOnce(concurrentTxnResponse())
			.mockResolvedValueOnce(concurrentTxnResponse())

		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			transactionalId: 'tx-poll',
			retryBackoffMs: 100,
			maxRetryBackoffMs: 1000,
		})

		const start = performance.now()
		await producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('msg') })
		})
		const elapsed = performance.now() - start

		expect(txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(3)
		expect(elapsed).toBeLessThan(150)
	})

	it('does not consume the retry budget while polling', async () => {
		// More CONCURRENT_TRANSACTIONS rounds than retries+1 allows: with normal
		// budgeted retries (retries: 2 -> 3 attempts) the transaction would abort.
		for (let i = 0; i < 6; i += 1) {
			txnCoordinator.addPartitionsToTxn.mockResolvedValueOnce(concurrentTxnResponse())
		}

		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			transactionalId: 'tx-poll',
			retries: 2,
			retryBackoffMs: 1,
		})

		await expect(
			producer.transaction(async tx => {
				await tx.send(TOPIC, { value: Buffer.from('msg') })
			})
		).resolves.toBeUndefined()

		expect(txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(7)
	})

	it('gives up once maxBlockMs elapses instead of polling forever', async () => {
		txnCoordinator.addPartitionsToTxn.mockResolvedValue(concurrentTxnResponse())

		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			transactionalId: 'tx-poll',
			retries: 1,
			retryBackoffMs: 1,
			maxRetryBackoffMs: 5,
			maxBlockMs: 100,
		})

		await expect(
			producer.transaction(async tx => {
				await tx.send(TOPIC, { value: Buffer.from('msg') })
			})
		).rejects.toThrow()
	})
})
