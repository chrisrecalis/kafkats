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

describe('Producer: concurrent transactional sends register a partition once', () => {
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

	/** A transactional producer whose actual produce step is stubbed out. */
	function makeProducer() {
		const producer = new Producer(mockCluster, { retries: 1, retryBackoffMs: 1, transactionalId: 'tx-1' })
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const p = producer as any
		vi.spyOn(p, 'getPartitionCount').mockResolvedValue(1)
		vi.spyOn(p, 'append').mockResolvedValue([
			Promise.resolve({ topic: TOPIC, partition: 0, offset: 0n, timestamp: 0n }),
		])
		return p
	}

	const send = (p: { transactionalSend: (topic: unknown, message: unknown) => Promise<unknown> }) =>
		p.transactionalSend({ topic: TOPIC }, { value: Buffer.from('v'), partition: 0 })

	it('issues a single AddPartitionsToTxn for sends started before it returns', async () => {
		txnCoordinator.addPartitionsToTxn.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 10))
			return {
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				results: [{ name: TOPIC, resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
			}
		})
		const p = makeProducer()

		await Promise.all(Array.from({ length: 50 }, () => send(p)))

		expect(txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(1)
		// None of the sends produced before the partition was part of the transaction.
		expect(p.partitionsInTransaction.has(`${TOPIC}:0`)).toBe(true)
		expect(p.append).toHaveBeenCalledTimes(50)
	})

	it('fails a send whose registration fails, without producing anything', async () => {
		txnCoordinator.addPartitionsToTxn.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 10))
			return { throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState, results: [] }
		})
		const p = makeProducer()

		const outcomes = await Promise.allSettled(Array.from({ length: 20 }, () => send(p)))

		expect(outcomes.every(o => o.status === 'rejected')).toBe(true)
		// Nothing was produced to a partition the transaction does not contain.
		expect(p.append).not.toHaveBeenCalled()

		// Each send that still needs the partition asks again, one at a time.
		expect(txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(20)
	})
})
