import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { InvalidTxnStateError, TransactionAbortedError } from '@/client/errors.js'
import { createMockBroker, createMockCluster, buildProduceResponse, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer transaction safety', () => {
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

	function makeProducer() {
		return new Producer(mockCluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
		})
	}

	describe('commitTransaction with failed sends (A1)', () => {
		it('aborts instead of committing when a fire-and-forget transactional send failed', async () => {
			// The broker definitively rejects the batch (non-retriable).
			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.MessageTooLarge }] },
				])
			)

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					// Fire-and-forget: the failure must still poison the transaction.
					void tx.send(TOPIC, { value: Buffer.from('too large') }).catch(() => {})
				})
			).rejects.toThrow(TransactionAbortedError)

			expect(txnCoordinator.endTxn).not.toHaveBeenCalledWith(expect.objectContaining({ committed: true }))
			expect(txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
		})
	})

	describe('abortTransaction EndTxn failure handling (A2)', () => {
		it('retries EndTxn(abort) on retriable errors', async () => {
			txnCoordinator.endTxn
				.mockResolvedValueOnce({ throttleTimeMs: 0, errorCode: ErrorCode.ConcurrentTransactions })
				.mockResolvedValueOnce({ throttleTimeMs: 0, errorCode: ErrorCode.None })

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					await tx.send(TOPIC, { value: Buffer.from('msg') })
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)

			expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
			expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(1, expect.objectContaining({ committed: false }))
			expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(2, expect.objectContaining({ committed: false }))

			// The abort marker was written — the producer must be usable again.
			await producer.transaction(async tx => {
				await tx.send(TOPIC, { value: Buffer.from('next') })
			})
			expect(txnCoordinator.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ committed: true }))
		})

		it('invalidates the coordinator when EndTxn(abort) returns NotCoordinator', async () => {
			txnCoordinator.endTxn
				.mockResolvedValueOnce({ throttleTimeMs: 0, errorCode: ErrorCode.NotCoordinator })
				.mockResolvedValueOnce({ throttleTimeMs: 0, errorCode: ErrorCode.None })

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					await tx.send(TOPIC, { value: Buffer.from('msg') })
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)

			expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
			expect(mockCluster.invalidateCoordinator).toHaveBeenCalledWith('TRANSACTION', 'tx-1')
		})

		it('enters a fatal state when EndTxn(abort) definitively fails', async () => {
			txnCoordinator.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState })

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					await tx.send(TOPIC, { value: Buffer.from('msg') })
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)

			// The abort marker was never written — a new transaction would silently
			// merge with the open one, so the producer must refuse to start it.
			await expect(producer.transaction(async () => {})).rejects.toThrow(InvalidTxnStateError)

			// But a fatally-errored producer must still be closeable.
			await expect(producer.disconnect()).resolves.toBeUndefined()
		})

		it('keeps TransactionAbortedError and enters a fatal state when EndTxn(abort) throws', async () => {
			txnCoordinator.endTxn.mockRejectedValue(new Error('connection lost'))

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					await tx.send(TOPIC, { value: Buffer.from('msg') })
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)

			await expect(producer.transaction(async () => {})).rejects.toThrow(InvalidTxnStateError)
		})
	})

	describe('abortTransaction drains in-flight sends (A3)', () => {
		it('does not issue EndTxn(abort) until in-flight produce requests settle', async () => {
			const events: string[] = []

			mockBroker.produce.mockImplementation(async () => {
				await new Promise(resolve => setTimeout(resolve, 25))
				events.push('produce:settled')
				return buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }] },
				])
			})
			txnCoordinator.endTxn.mockImplementation(async () => {
				events.push('endTxn')
				return { throttleTimeMs: 0, errorCode: ErrorCode.None }
			})

			const producer = makeProducer()

			await expect(
				producer.transaction(async tx => {
					// Fire-and-forget send that is still in flight when the abort starts.
					void tx.send(TOPIC, { value: Buffer.from('slow') }).catch(() => {})
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)

			expect(events).toEqual(['produce:settled', 'endTxn'])
		})
	})

	describe('empty transactions skip EndTxn (A6)', () => {
		it('commits an empty transaction without sending EndTxn', async () => {
			const producer = makeProducer()

			await producer.transaction(async () => {})

			expect(txnCoordinator.endTxn).not.toHaveBeenCalled()
		})

		it('aborts an empty transaction without sending EndTxn and stays usable', async () => {
			const producer = makeProducer()

			await expect(
				producer.transaction(async () => {
					throw new Error('boom')
				})
			).rejects.toThrow(TransactionAbortedError)

			expect(txnCoordinator.endTxn).not.toHaveBeenCalled()

			await producer.transaction(async tx => {
				await tx.send(TOPIC, { value: Buffer.from('next') })
			})
			expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: true }))
		})
	})
})
