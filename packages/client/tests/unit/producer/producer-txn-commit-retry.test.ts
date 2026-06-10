import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'
const GROUP = 'consumer-group-1'

describe('Producer.commitTransactionOffsets retry', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let txnCoordinator: MockBroker
	let groupCoordinator: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		txnCoordinator = createMockBroker(1)
		groupCoordinator = createMockBroker(2)

		mockCluster.getCoordinator.mockImplementation(async (type: string) =>
			type === 'TRANSACTION' ? txnCoordinator : groupCoordinator
		)
		mockCluster.getAnyBroker.mockResolvedValue(txnCoordinator)

		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		txnCoordinator.addOffsetsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
		txnCoordinator.endTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer() {
		return new Producer(mockCluster, {
			lingerMs: 0,
			retries: 3,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
		})
	}

	function commitOffsetsResponse(errorCode: ErrorCode) {
		return {
			throttleTimeMs: 0,
			topics: [
				{
					name: TOPIC,
					partitions: [{ partitionIndex: 0, errorCode }],
				},
			],
		}
	}

	function endTxnResponse(errorCode: ErrorCode) {
		return {
			throttleTimeMs: 0,
			errorCode,
		}
	}

	async function runTransaction(producer: Producer) {
		await producer.transaction(async tx => {
			await tx.sendOffsets({
				groupId: GROUP,
				offsets: [{ topic: TOPIC, partition: 0, offset: 42n }],
			})
		})
	}

	async function runEmptyTransaction(producer: Producer) {
		await producer.transaction(async () => {})
	}

	it('retries TxnOffsetCommit on RebalanceInProgress', async () => {
		groupCoordinator.txnOffsetCommit
			.mockResolvedValueOnce(commitOffsetsResponse(ErrorCode.RebalanceInProgress))
			.mockResolvedValueOnce(commitOffsetsResponse(ErrorCode.None))

		await runTransaction(makeProducer())

		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledTimes(2)
	})

	it('retries TxnOffsetCommit on NotCoordinator and invalidates the coordinator cache', async () => {
		groupCoordinator.txnOffsetCommit
			.mockResolvedValueOnce(commitOffsetsResponse(ErrorCode.NotCoordinator))
			.mockResolvedValueOnce(commitOffsetsResponse(ErrorCode.None))

		await runTransaction(makeProducer())

		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledTimes(2)
		expect(mockCluster.invalidateCoordinator).toHaveBeenCalledWith('GROUP', GROUP)
	})

	it('throws after the retry budget is exhausted', async () => {
		groupCoordinator.txnOffsetCommit.mockResolvedValue(commitOffsetsResponse(ErrorCode.RebalanceInProgress))

		await expect(runTransaction(makeProducer())).rejects.toThrow(/TxnOffsetCommit failed/)
	})

	it('aborts the transaction when EndTxn(commit) fails', async () => {
		txnCoordinator.endTxn
			.mockImplementationOnce(async () => endTxnResponse(ErrorCode.InvalidTxnState))
			.mockImplementationOnce(async () => endTxnResponse(ErrorCode.None))

		await expect(runEmptyTransaction(makeProducer())).rejects.toThrow(/Transaction aborted/)

		expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(
			1,
			expect.objectContaining({
				committed: true,
			})
		)
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(
			2,
			expect.objectContaining({
				committed: false,
			})
		)
	})
})
