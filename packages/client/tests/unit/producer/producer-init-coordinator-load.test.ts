import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { KafkaProtocolError, TimeoutError } from '@/client/errors.js'
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
		txnCoordinator.addOffsetsToTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
		txnCoordinator.txnOffsetCommit.mockResolvedValue({
			throttleTimeMs: 0,
			topics: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
		})
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

		// Non-empty transaction (offsets prepared) so EndTxn is actually sent.
		await expect(
			producer.transaction(async tx => {
				await tx.sendOffsets({
					groupId: 'g1',
					offsets: [{ topic: 'test-topic', partition: 0, offset: 1n }],
				})
			})
		).resolves.toBeUndefined()
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

	it('a non-retriable InitProducerId error is a KafkaProtocolError with its code, rethrown by every later begin', async () => {
		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.TransactionalIdAuthorizationFailed,
			producerId: -1n,
			producerEpoch: -1,
		})
		const producer = new Producer(mockCluster, { lingerMs: 0, retryBackoffMs: 1, transactionalId: 'tx-1' })

		const first: unknown = await producer.beginTransaction().catch((error: unknown) => error)
		expect(first).toBeInstanceOf(KafkaProtocolError)
		expect((first as KafkaProtocolError).errorCode).toBe(ErrorCode.TransactionalIdAuthorizationFailed)
		await expect(producer.beginTransaction()).rejects.toBe(first)
		expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
	})

	it('a non-retriable FindCoordinator answer (no Describe on the transactional id) fails fast too', async () => {
		const denied = new KafkaProtocolError(ErrorCode.TransactionalIdAuthorizationFailed, 'FindCoordinator failed')
		mockCluster.getCoordinator.mockRejectedValue(denied)
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retryBackoffMs: 1,
			maxBlockMs: 60_000,
			transactionalId: 'tx-1',
		})

		await expect(producer.beginTransaction()).rejects.toBe(denied)
		expect(mockCluster.getCoordinator).toHaveBeenCalledTimes(1)
		expect(txnCoordinator.initProducerId).not.toHaveBeenCalled()
	})

	it('past maxBlockMs a TimeoutError naming the transactional id; the next begin initializes again (Java)', async () => {
		const busy = {
			throttleTimeMs: 0,
			errorCode: ErrorCode.ConcurrentTransactions,
			producerId: -1n,
			producerEpoch: -1,
		}
		let answered = 0
		let ready = false
		txnCoordinator.initProducerId.mockImplementation(async () => {
			answered++
			return ready ? { throttleTimeMs: 0, errorCode: ErrorCode.None, producerId: 9n, producerEpoch: 1 } : busy
		})
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retryBackoffMs: 5,
			maxRetryBackoffMs: 5,
			maxBlockMs: 50,
			transactionalId: 'tx-1',
		})

		const timedOut: unknown = await producer.beginTransaction().catch((error: unknown) => error)
		expect(timedOut).toBeInstanceOf(TimeoutError)
		expect((timedOut as TimeoutError).message).toMatch(
			/^InitProducerId of transactional id 'tx-1' did not complete within maxBlockMs: .*ConcurrentTransactions/
		)
		expect((timedOut as TimeoutError).cause).toBeInstanceOf(KafkaProtocolError)
		expect(answered).toBeGreaterThan(1)

		// The next begin initializes again: one more InitProducerId, whose identity the transaction then uses.
		const before = answered
		ready = true
		const transaction = await producer.beginTransaction()
		expect(answered).toBe(before + 1)
		await transaction.sendOffsets({ groupId: 'g1', offsets: [{ topic: 'test-topic', partition: 0, offset: 1n }] })
		await transaction.commit()
		expect(txnCoordinator.addOffsetsToTxn).toHaveBeenCalledWith(
			expect.objectContaining({ transactionalId: 'tx-1', producerId: 9n, producerEpoch: 1 })
		)
		expect(txnCoordinator.endTxn).toHaveBeenCalledWith(
			expect.objectContaining({ producerId: 9n, producerEpoch: 1, committed: true })
		)
		await producer.disconnect({ timeoutMs: 100 })
	})

	it('a disconnect stops the retries: a producer given up never bumps the epoch after its successor', async () => {
		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.ConcurrentTransactions,
			producerId: -1n,
			producerEpoch: -1,
		})
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retryBackoffMs: 5,
			maxRetryBackoffMs: 5,
			maxBlockMs: 60_000,
			transactionalId: 'tx-1',
		})
		const begin = producer.beginTransaction()
		begin.catch(() => {})
		await vi.waitFor(() => expect(txnCoordinator.initProducerId.mock.calls.length).toBeGreaterThanOrEqual(2))

		await producer.disconnect({ timeoutMs: 100 })
		await expect(begin).rejects.toThrow('Producer disconnected')
		const calls = txnCoordinator.initProducerId.mock.calls.length
		await new Promise(resolve => setTimeout(resolve, 50))
		expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(calls)
	})

	// Without the bound, each of these waits for an answer that never comes: fail them fast.
	describe('maxBlockMs bounds the whole initialization, like Java initTransactions()', { timeout: 5000 }, () => {
		const ok = { throttleTimeMs: 0, errorCode: ErrorCode.None, producerId: 9n, producerEpoch: 1 }
		const transactional = (options: { maxBlockMs: number; retryBackoffMs?: number }) =>
			new Producer(mockCluster, {
				lingerMs: 0,
				retryBackoffMs: options.retryBackoffMs ?? 5,
				maxRetryBackoffMs: options.retryBackoffMs ?? 5,
				maxBlockMs: options.maxBlockMs,
				transactionalId: 'tx-1',
			})
		/** How long `promise` takes to settle, and its rejection. */
		const timed = async (promise: Promise<unknown>) => {
			const started = Date.now()
			const error = await promise.then(
				() => undefined,
				(error: unknown) => error
			)
			return { error, elapsedMs: Date.now() - started }
		}

		it('an InitProducerId in flight at the deadline: a TimeoutError then, and the next initialization awaits it instead of sending another', async () => {
			let answer!: (response: typeof ok) => void
			txnCoordinator.initProducerId.mockImplementationOnce(
				() => new Promise<typeof ok>(resolve => (answer = resolve))
			)
			const producer = transactional({ maxBlockMs: 100 })

			const first = await timed(producer.beginTransaction())
			expect(first.error).toBeInstanceOf(TimeoutError)
			expect(first.elapsedMs).toBeLessThan(1000)

			// Another InitProducerId would bump the epoch again once the broker processes the one in flight, which
			// fences a request that carries the epoch of before. The next begin waits for the answer of the first.
			const second = producer.beginTransaction()
			await new Promise(resolve => setTimeout(resolve, 20))
			expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
			answer(ok)
			const transaction = await second
			await transaction.sendOffsets({
				groupId: 'g1',
				offsets: [{ topic: 'test-topic', partition: 0, offset: 1n }],
			})
			await transaction.commit()
			expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
			expect(txnCoordinator.endTxn).toHaveBeenCalledWith(
				expect.objectContaining({ producerId: 9n, producerEpoch: 1 })
			)
			await producer.disconnect({ timeoutMs: 100 })
		})

		it('an attempt in flight that fails after the deadline is retried by the next initialization', async () => {
			let fail!: (error: Error) => void
			txnCoordinator.initProducerId
				.mockImplementationOnce(() => new Promise<typeof ok>((_, reject) => (fail = reject)))
				.mockResolvedValue(ok)
			const producer = transactional({ maxBlockMs: 100 })

			expect((await timed(producer.beginTransaction())).error).toBeInstanceOf(TimeoutError)
			const second = producer.beginTransaction()
			fail(new Error('Request timeout'))
			await second
			expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(2)
			await producer.disconnect({ timeoutMs: 100 })
		})

		it('a coordinator lookup that does not answer', async () => {
			mockCluster.getCoordinator.mockImplementationOnce(() => new Promise(() => {}))
			const producer = transactional({ maxBlockMs: 100 })

			const { error, elapsedMs } = await timed(producer.beginTransaction())
			expect(error).toBeInstanceOf(TimeoutError)
			expect(elapsedMs).toBeLessThan(1000)
			expect(txnCoordinator.initProducerId).not.toHaveBeenCalled()
			await producer.disconnect({ timeoutMs: 100 })
		})

		/** The next coordinator lookup waits until `release()`, then finds the coordinator. */
		function holdLookup() {
			let release!: () => void
			mockCluster.getCoordinator.mockImplementationOnce(async () => {
				await new Promise<void>(resolve => (release = resolve))
				return txnCoordinator
			})
			return { release: () => release() }
		}

		it('a lookup answering after transaction() timed out sends no InitProducerId; the next begin looks up again', async () => {
			txnCoordinator.initProducerId.mockResolvedValue(ok)
			const lookup = holdLookup()
			const producer = transactional({ maxBlockMs: 100 })

			const { error } = await timed(producer.transaction(async () => {}))
			expect(error).toBeInstanceOf(TimeoutError)
			// An InitProducerId now would bump the epoch of the transactional id behind whatever replaced this producer.
			lookup.release()
			await new Promise(resolve => setTimeout(resolve, 20))
			expect(txnCoordinator.initProducerId).not.toHaveBeenCalled()

			const transaction = await producer.beginTransaction()
			expect(mockCluster.getCoordinator).toHaveBeenCalledTimes(2)
			expect(txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
			await transaction.abort()
			await producer.disconnect({ timeoutMs: 100 })
		})

		it('a lookup answering after disconnect() returned sends no InitProducerId', async () => {
			txnCoordinator.initProducerId.mockResolvedValue(ok)
			const lookup = holdLookup()
			const producer = transactional({ maxBlockMs: 60_000 })
			const begin = producer.beginTransaction()
			begin.catch(() => {})
			await vi.waitFor(() => expect(mockCluster.getCoordinator).toHaveBeenCalled())

			await producer.disconnect({ timeoutMs: 100 })
			await expect(begin).rejects.toThrow('Producer disconnected')
			lookup.release()
			await new Promise(resolve => setTimeout(resolve, 20))
			expect(txnCoordinator.initProducerId).not.toHaveBeenCalled()
		})

		it('a lookup answering once maxBlockMs passed sends nothing, even before the deadline timer ran', async () => {
			vi.useFakeTimers({ toFake: ['Date'] })
			try {
				txnCoordinator.initProducerId.mockResolvedValue(ok)
				const lookup = holdLookup()
				const producer = transactional({ maxBlockMs: 60_000 })
				const begin = producer.beginTransaction().catch((error: unknown) => error)
				await vi.waitFor(() => expect(mockCluster.getCoordinator).toHaveBeenCalled())

				// The clock passes the deadline while the lookup's answer is on its way, ahead of the timer.
				vi.setSystemTime(Date.now() + 60_001)
				lookup.release()

				expect(await begin).toBeInstanceOf(TimeoutError)
				expect(txnCoordinator.initProducerId).not.toHaveBeenCalled()
			} finally {
				vi.useRealTimers()
			}
		})

		it('a retry backoff longer than the time left', async () => {
			txnCoordinator.initProducerId.mockResolvedValue({ ...ok, errorCode: ErrorCode.ConcurrentTransactions })
			const producer = transactional({ maxBlockMs: 100, retryBackoffMs: 5000 })

			const { error, elapsedMs } = await timed(producer.beginTransaction())
			expect(error).toBeInstanceOf(TimeoutError)
			expect(elapsedMs).toBeLessThan(1000)
			await producer.disconnect({ timeoutMs: 100 })
		})
	})
})
