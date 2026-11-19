import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { InvalidTxnStateError, TransactionAbortedError } from '@/client/errors.js'
import { createTransactionalMocks } from './_helpers.js'

const TOPIC = 'test-topic'
const GROUP = 'group-1'
const value = Buffer.from('v')

describe('Producer transactional requests: connection failures and the commit outcome', () => {
	let mocks: ReturnType<typeof createTransactionalMocks>

	beforeEach(() => {
		mocks = createTransactionalMocks(TOPIC)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer() {
		return new Producer(mocks.cluster, { lingerMs: 0, retries: 3, retryBackoffMs: 1, transactionalId: 'tx-1' })
	}

	async function sendAndCommitOffsets(producer: Producer): Promise<void> {
		await producer.transaction(async tx => {
			await tx.send(TOPIC, { value })
			await tx.sendOffsets({ groupId: GROUP, offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
		})
	}

	describe('retries a lost connection and looks the coordinator up again', () => {
		it('for AddPartitionsToTxn', async () => {
			mocks.txnCoordinator.addPartitionsToTxn.mockRejectedValueOnce(new ConnectionClosedError())

			await sendAndCommitOffsets(makeProducer())

			expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2)
			expect(mocks.cluster.invalidateCoordinator).toHaveBeenCalledWith('TRANSACTION', 'tx-1')
		})

		it('for AddOffsetsToTxn', async () => {
			mocks.txnCoordinator.addOffsetsToTxn.mockRejectedValueOnce(new ConnectionClosedError())

			await sendAndCommitOffsets(makeProducer())

			expect(mocks.txnCoordinator.addOffsetsToTxn).toHaveBeenCalledTimes(2)
			expect(mocks.cluster.invalidateCoordinator).toHaveBeenCalledWith('TRANSACTION', 'tx-1')
		})

		it('for TxnOffsetCommit', async () => {
			mocks.groupCoordinator.txnOffsetCommit.mockRejectedValueOnce(new ConnectionClosedError())

			await sendAndCommitOffsets(makeProducer())

			expect(mocks.groupCoordinator.txnOffsetCommit).toHaveBeenCalledTimes(2)
			expect(mocks.cluster.invalidateCoordinator).toHaveBeenCalledWith('GROUP', GROUP)
		})

		it('for EndTxn(commit), resent with the same producer id and epoch', async () => {
			mocks.txnCoordinator.endTxn.mockRejectedValueOnce(new ConnectionClosedError())
			const producer = makeProducer()

			await sendAndCommitOffsets(producer)

			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
			const [first, second] = mocks.txnCoordinator.endTxn.mock.calls.map(([request]) => request)
			expect(second).toEqual(first)
			expect(first).toMatchObject({ producerId: 1n, producerEpoch: 0, committed: true })
			expect(producer.transactionState).toBe('idle')
		})
	})

	describe('outcome', () => {
		it('is unknown when EndTxn(commit) is lost on every attempt', async () => {
			mocks.txnCoordinator.endTxn.mockRejectedValue(new ConnectionClosedError())
			const producer = makeProducer()
			const errors: Error[] = []
			producer.on('error', error => errors.push(error))

			const error = await sendAndCommitOffsets(producer).catch((e: unknown) => e)

			expect(error).toBeInstanceOf(TransactionAbortedError)
			expect(error).toMatchObject({ outcome: 'unknown', cause: expect.any(ConnectionClosedError) })
			expect((error as Error).message).toMatch(/commit outcome unknown/)
			// retries + 1 attempts, every one a commit: no abort is sent after a commit that may have been applied.
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(4)
			expect(mocks.txnCoordinator.endTxn).not.toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
			expect(producer.transactionState).toBe('error')
			expect(errors).toHaveLength(1)

			await expect(producer.transaction(async () => {})).rejects.toThrow(InvalidTxnStateError)
			await expect(producer.disconnect()).resolves.toBeUndefined()
		})

		it('stays unknown when a lost EndTxn(commit) is followed by a rejection', async () => {
			mocks.txnCoordinator.endTxn
				.mockRejectedValueOnce(new ConnectionClosedError())
				.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState })

			const error = await sendAndCommitOffsets(makeProducer()).catch((e: unknown) => e)

			expect(error).toMatchObject({ outcome: 'unknown' })
		})

		it('is aborted when the broker rejects EndTxn(commit) without a lost request', async () => {
			mocks.txnCoordinator.endTxn
				.mockResolvedValueOnce({ throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState })
				.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
			const producer = makeProducer()

			const error = await sendAndCommitOffsets(producer).catch((e: unknown) => e)

			expect(error).toMatchObject({ outcome: 'aborted' })
			expect(mocks.txnCoordinator.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ committed: false }))
			expect(producer.transactionState).toBe('idle')
		})

		it('is aborted when the transaction fails before EndTxn(commit)', async () => {
			mocks.groupCoordinator.txnOffsetCommit.mockResolvedValue({
				throttleTimeMs: 0,
				topics: [{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.IllegalGeneration }] }],
			})

			const error = await sendAndCommitOffsets(makeProducer()).catch((e: unknown) => e)

			expect(error).toMatchObject({ outcome: 'aborted' })
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
		})
	})
})
