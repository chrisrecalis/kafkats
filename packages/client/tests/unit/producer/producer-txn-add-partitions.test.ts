import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createTransactionalMocks } from './_helpers.js'

const TOPIC = 'test-topic'
const value = Buffer.from('v')

describe('Producer: AddPartitionsToTxn of a transactional send', () => {
	let mocks: ReturnType<typeof createTransactionalMocks>

	beforeEach(() => {
		mocks = createTransactionalMocks(TOPIC)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer() {
		return new Producer(mocks.cluster, { lingerMs: 0, retries: 2, retryBackoffMs: 1, transactionalId: 'tx-1' })
	}

	it('rejects the send on a top-level error, does not add the partition, and produces nothing', async () => {
		mocks.txnCoordinator.addPartitionsToTxn.mockResolvedValueOnce({
			throttleTimeMs: 0,
			errorCode: ErrorCode.InvalidTxnState,
			// Broker rejected the whole request at the top level; no per-partition results.
			results: [],
		})
		const tx = await makeProducer().beginTransaction()

		await expect(tx.send(TOPIC, { value, partition: 0 })).rejects.toThrow(/AddPartitionsToTxn failed/)
		expect(mocks.leader.produce).not.toHaveBeenCalled()
		// Not part of the transaction: the next send of the partition registers it.
		await tx.send(TOPIC, { value, partition: 0 })
		expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2)
		await tx.abort()
	})

	it('invalidates the transaction coordinator on a top-level coordinator error', async () => {
		mocks.txnCoordinator.addPartitionsToTxn.mockResolvedValueOnce({
			throttleTimeMs: 0,
			errorCode: ErrorCode.NotCoordinator,
			results: [],
		})
		const tx = await makeProducer().beginTransaction()

		await tx.send(TOPIC, { value, partition: 0 })
		expect(mocks.cluster.invalidateCoordinator).toHaveBeenCalledWith('TRANSACTION', 'tx-1')
		expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2)
		await tx.commit()
	})

	it('adds the partition once the request fully succeeded: later sends of it register nothing', async () => {
		const tx = await makeProducer().beginTransaction()

		await tx.send(TOPIC, { value, partition: 0 })
		await tx.send(TOPIC, { value, partition: 0 })
		expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(1)
		await tx.commit()
	})
})

describe('Producer: concurrent transactional sends register a partition once', () => {
	let mocks: ReturnType<typeof createTransactionalMocks>

	beforeEach(() => {
		mocks = createTransactionalMocks(TOPIC)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	const makeProducer = () =>
		new Producer(mocks.cluster, { lingerMs: 0, retries: 1, retryBackoffMs: 1, transactionalId: 'tx-1' })

	it('issues a single AddPartitionsToTxn for sends started before it returns', async () => {
		const events: string[] = []
		const register = mocks.txnCoordinator.addPartitionsToTxn.getMockImplementation()!
		mocks.txnCoordinator.addPartitionsToTxn.mockImplementation(async request => {
			await new Promise(resolve => setTimeout(resolve, 10))
			events.push('AddPartitionsToTxn answered')
			return register(request)
		})
		const produce = mocks.leader.produce.getMockImplementation()!
		mocks.leader.produce.mockImplementation(async request => {
			events.push('Produce')
			return produce(request)
		})
		const tx = await makeProducer().beginTransaction()

		await Promise.all(Array.from({ length: 50 }, () => tx.send(TOPIC, { value, partition: 0 })))

		expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(1)
		// None of the sends produced before the partition was part of the transaction.
		expect(events[0]).toBe('AddPartitionsToTxn answered')
		expect(events.length).toBeGreaterThan(1)
		await tx.commit()
	})

	it('fails a send whose registration fails, without producing anything', async () => {
		mocks.txnCoordinator.addPartitionsToTxn.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 10))
			return { throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState, results: [] }
		})
		const tx = await makeProducer().beginTransaction()

		const outcomes = await Promise.allSettled(
			Array.from({ length: 20 }, () => tx.send(TOPIC, { value, partition: 0 }))
		)

		expect(outcomes.every(o => o.status === 'rejected')).toBe(true)
		// Nothing was produced to a partition the transaction does not contain.
		expect(mocks.leader.produce).not.toHaveBeenCalled()

		// Each send that still needs the partition asks again, one at a time.
		expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(20)
		await tx.abort()
	})
})
