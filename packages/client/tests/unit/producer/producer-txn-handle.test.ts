import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import type { ProducerConfig, TransactionHandle } from '@/producer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	CommitFailedError,
	InvalidTxnStateError,
	ProducerFencedError,
	TimeoutError,
	TransactionAbortedError,
} from '@/client/errors.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { buildProduceResponse, createTransactionalMocks, modelTransactions } from './_helpers.js'

const TOPIC = 'test-topic'
const value = Buffer.from('v')

function tick(ms = 10): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/** A promise that never settles: a broker that does not answer. */
function never<T>(): Promise<T> {
	return new Promise<T>(() => {})
}

describe('Producer.beginTransaction()', () => {
	let mocks: ReturnType<typeof createTransactionalMocks>

	beforeEach(() => {
		mocks = createTransactionalMocks(TOPIC)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer(config: ProducerConfig = {}) {
		return new Producer(mocks.cluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
			...config,
		})
	}

	describe('state machine', () => {
		it('initializes on the first begin, commits, and is idle again', async () => {
			const producer = makeProducer()
			expect(producer.transactionState).toBe('idle')

			const tx = await producer.beginTransaction()
			expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(1)
			expect(producer.transactionState).toBe('in_transaction')

			const sent = await tx.send(TOPIC, { value })
			expect(sent).toMatchObject({ topic: TOPIC, partition: 0 })

			const committed = tx.commit()
			expect(producer.transactionState).toBe('committing')
			await committed

			expect(producer.transactionState).toBe('idle')
			expect(tx.signal.aborted).toBe(false)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: true }))
		})

		it('makes a second begin wait until the open transaction ended', async () => {
			const producer = makeProducer()
			const first = await producer.beginTransaction()

			let secondBegun = false
			const second = producer.beginTransaction().then(tx => {
				secondBegun = true
				return tx
			})
			await tick()
			expect(secondBegun).toBe(false)

			await first.send(TOPIC, { value })
			await first.commit()
			const tx = await second
			expect(secondBegun).toBe(true)
			await tx.abort()
		})

		it('rejects commit and send after an abort, and accepts a second abort', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })

			await tx.abort()
			expect(tx.signal.aborted).toBe(true)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))

			await expect(tx.commit()).rejects.toThrow(InvalidTxnStateError)
			await expect(tx.send(TOPIC, { value })).rejects.toThrow(InvalidTxnStateError)
			await expect(tx.sendOffsets({ groupId: 'g', offsets: [] })).rejects.toThrow(InvalidTxnStateError)
			await expect(tx.abort()).resolves.toBeUndefined()
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(producer.transactionState).toBe('idle')
		})

		it('rejects abort and a second commit once committed', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })
			await tx.commit()

			await expect(tx.abort()).rejects.toThrow(InvalidTxnStateError)
			await expect(tx.commit()).rejects.toThrow(InvalidTxnStateError)
		})

		it('does not let a stale handle send into the next transaction', async () => {
			const producer = makeProducer()
			const first = await producer.beginTransaction()
			await first.commit()
			const second = await producer.beginTransaction()

			await expect(first.send(TOPIC, { value })).rejects.toThrow(InvalidTxnStateError)
			expect(mocks.leader.produce).not.toHaveBeenCalled()
			await second.abort()
		})

		it('aborts after a failed commit and reports TransactionAbortedError', async () => {
			mocks.leader.produce.mockResolvedValue(
				buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.MessageTooLarge }] },
				])
			)
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			void tx.send(TOPIC, { value }).catch(() => {})

			const error = await tx.commit().catch((e: unknown) => e)
			expect(error).toBeInstanceOf(TransactionAbortedError)
			expect(error).toMatchObject({ outcome: 'aborted' })
			expect(tx.signal.aborted).toBe(true)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
			await expect(tx.abort()).resolves.toBeUndefined()
			expect(producer.transactionState).toBe('idle')
		})
	})

	describe('abort', () => {
		it('waits for in-flight sends before EndTxn(abort)', async () => {
			const events: string[] = []
			mocks.leader.produce.mockImplementation(async () => {
				await tick(25)
				events.push('produce:settled')
				return buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] },
				])
			})
			mocks.txnCoordinator.endTxn.mockImplementation(async () => {
				events.push('endTxn')
				return { throttleTimeMs: 0, errorCode: ErrorCode.None }
			})

			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const sent = tx.send(TOPIC, { value })
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalled())

			await tx.abort()
			expect(events).toEqual(['produce:settled', 'endTxn'])
			// A produce already sent settles with the broker's answer (Java); the abort marker discards its records.
			await expect(sent).resolves.toMatchObject({ topic: TOPIC, partition: 0 })
		})

		it('lets a produce already sent retry before EndTxn(abort), like the Java client (its sequence is taken)', async () => {
			const events: string[] = []
			let firstSent!: () => void
			const sentOnce = new Promise<void>(resolve => (firstSent = resolve))
			const answer = (errorCode: ErrorCode) =>
				buildProduceResponse([{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode }] }])
			mocks.leader.produce
				.mockImplementationOnce(async () => {
					events.push('produce')
					firstSent()
					return answer(ErrorCode.NotLeaderOrFollower)
				})
				.mockImplementationOnce(async () => {
					events.push('produce (retry)')
					return answer(ErrorCode.None)
				})
			mocks.txnCoordinator.endTxn.mockImplementation(async () => {
				events.push('endTxn')
				return { throttleTimeMs: 0, errorCode: ErrorCode.None }
			})
			const producer = makeProducer({ retryBackoffMs: 100 })
			const tx = await producer.beginTransaction()
			const sent = tx.send(TOPIC, { value })
			await sentOnce

			await tx.abort()
			expect(events).toEqual(['produce', 'produce (retry)', 'endTxn'])
			await expect(sent).resolves.toMatchObject({ topic: TOPIC, partition: 0 })
		})

		it('is triggered by the begin signal, and passes the turn on', async () => {
			const producer = makeProducer()
			const controller = new AbortController()
			const tx = await producer.beginTransaction({ signal: controller.signal })
			await tx.send(TOPIC, { value })

			controller.abort()
			expect(tx.signal.aborted).toBe(true)
			await vi.waitFor(() =>
				expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
			)
			await expect(tx.commit()).rejects.toThrow(InvalidTxnStateError)

			const next = await producer.beginTransaction()
			await next.send(TOPIC, { value })
			await next.commit()
			expect(mocks.txnCoordinator.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ committed: true }))
		})

		it('is not triggered by the begin signal once commit() was called: the commit bounds itself', async () => {
			let answer!: () => void
			mocks.txnCoordinator.endTxn.mockImplementationOnce(
				() =>
					new Promise(resolve => {
						answer = () => resolve({ throttleTimeMs: 0, errorCode: ErrorCode.None })
					})
			)
			const producer = makeProducer()
			const controller = new AbortController()
			const tx = await producer.beginTransaction({ signal: controller.signal })
			await tx.send(TOPIC, { value })

			const committed = tx.commit()
			await vi.waitFor(() => expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1))
			controller.abort()
			await tick()
			answer()

			await expect(committed).resolves.toBeUndefined()
			expect(tx.signal.aborted).toBe(false)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledExactlyOnceWith(
				expect.objectContaining({ committed: true })
			)
			expect(producer.transactionState).toBe('idle')
		})

		it('enters the error state when it does not finish within timeoutMs', async () => {
			mocks.txnCoordinator.endTxn.mockImplementation(() => never())
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })

			const aborting = tx.abort({ timeoutMs: 30 })
			// Called while the first one runs: it rejects too once that failed (Java rethrows in an error state).
			const again = tx.abort()
			await expect(aborting).rejects.toThrow(TimeoutError)
			await expect(again).rejects.toMatchObject({ name: 'InvalidTxnStateError', currentState: 'error' })
			expect(producer.transactionState).toBe('error')
			await expect(tx.abort()).rejects.toMatchObject({ name: 'InvalidTxnStateError', currentState: 'error' })
			await expect(producer.beginTransaction()).rejects.toThrow(InvalidTxnStateError)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			await expect(producer.disconnect()).resolves.toBeUndefined()
		})
	})

	describe('begin', () => {
		it('rejects a begin whose signal aborts while it waits, and keeps the queue moving', async () => {
			const producer = makeProducer()
			const first = await producer.beginTransaction()
			const controller = new AbortController()
			const gaveUp = producer.beginTransaction({ signal: controller.signal })
			const third = producer.beginTransaction()

			controller.abort(new Error('gave up'))
			await expect(gaveUp).rejects.toThrow('gave up')

			await first.commit()
			const tx = await third
			await tx.commit()
		})

		it('requires a transactional id', async () => {
			const producer = new Producer(mocks.cluster, { lingerMs: 0 })
			await expect(producer.beginTransaction()).rejects.toThrow(/requires transactionalId/)
		})

		it('throws when called from inside a transaction() callback', async () => {
			const producer = makeProducer()
			await expect(
				producer.transaction(async () => {
					await producer.beginTransaction()
				})
			).rejects.toThrow(/inside an active transaction callback/)
		})

		it('runs handles in parallel on the lanes of a pooled producer', async () => {
			const producer = makeProducer({ transactionConcurrency: 2 })
			const a = await producer.beginTransaction()
			const b = await producer.beginTransaction()

			let thirdBegun = false
			const third = producer.beginTransaction().then(tx => {
				thirdBegun = true
				return tx
			})
			await tick()
			expect(thirdBegun).toBe(false)
			expect(producer.transactionState).toBe('in_transaction')

			await a.send(TOPIC, { value })
			await a.commit()
			const c = await third
			await Promise.all([b.abort(), c.abort()])
			expect(producer.transactionState).toBe('idle')
			const transactionalIds = new Set(
				mocks.txnCoordinator.initProducerId.mock.calls.map(([r]) => r.transactionalId)
			)
			expect(transactionalIds).toEqual(new Set(['tx-1', 'tx-1-1']))
		})

		it('a pooled producer with a fenced lane begins nothing any more; a transaction open on another lane still commits', async () => {
			const producer = makeProducer({ transactionConcurrency: 2 })
			const healthy = await producer.beginTransaction()
			await healthy.send(TOPIC, { value })
			const doomed = await producer.beginTransaction()
			mocks.leader.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.ProducerFenced }] },
				])
			)
			const fenced = await doomed.send(TOPIC, { value }).catch((e: unknown) => e)
			expect(fenced).toBeInstanceOf(ProducerFencedError)
			expect(producer.transactionState).toBe('error')

			await expect(doomed.abort()).rejects.toBe(fenced)
			await healthy.commit()
			expect(producer.transactionState).toBe('error')
			// Both lanes are free, one of them healthy: like a single producer in the 'error' state, the pool refuses.
			const concurrent = await Promise.all(
				[0, 1].map(() => producer.transaction(async tx => void (await tx.send(TOPIC, { value }))).catch(e => e))
			)
			expect(concurrent).toEqual([fenced, fenced])
			await expect(producer.beginTransaction()).rejects.toBe(fenced)
			expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(2)
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledExactlyOnceWith(
				expect.objectContaining({ committed: true })
			)
		})
	})

	describe('commit outcome under timeoutMs and signal', () => {
		it('reports an unknown outcome when EndTxn(commit) is not answered in time', async () => {
			mocks.txnCoordinator.endTxn.mockImplementation(() => never())
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })

			const error = await tx.commit({ timeoutMs: 30 }).catch((e: unknown) => e)
			expect(error).toBeInstanceOf(TransactionAbortedError)
			expect(error).toMatchObject({ outcome: 'unknown' })
			expect((error as TransactionAbortedError).cause).toBeInstanceOf(TimeoutError)
			// The commit may have been applied: no abort is attempted.
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
			expect(producer.transactionState).toBe('error')
			// Not aborted: the broker may have committed it (Java refuses an abort after a commit that timed out).
			await expect(tx.abort()).rejects.toMatchObject({ name: 'InvalidTxnStateError', currentState: 'error' })
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
		})

		it('abort() rejects after a commit whose EndTxn lost its connection, and sends nothing', async () => {
			mocks.txnCoordinator.endTxn.mockRejectedValue(new ConnectionClosedError('connection lost'))
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })

			const error = await tx.commit().catch((e: unknown) => e)
			expect(error).toMatchObject({ name: 'TransactionAbortedError', outcome: 'unknown' })
			const sent = mocks.txnCoordinator.endTxn.mock.calls.length
			expect(mocks.txnCoordinator.endTxn.mock.calls.every(([r]) => r.committed)).toBe(true)

			await expect(tx.abort()).rejects.toMatchObject({ name: 'InvalidTxnStateError', currentState: 'error' })
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(sent)
			expect(producer.transactionState).toBe('error')
		})

		it('a timeoutMs of Infinity puts no deadline on the commit', async () => {
			mocks.txnCoordinator.endTxn.mockImplementation(async () => {
				// Later than a timer armed with Infinity, which Node runs after 1 ms.
				await tick(5)
				return { throttleTimeMs: 0, errorCode: ErrorCode.None }
			})
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })

			await expect(tx.commit({ timeoutMs: Infinity })).resolves.toBeUndefined()
			expect(producer.transactionState).toBe('idle')
		})

		it('sends no EndTxn once the signal aborts the commit before it', async () => {
			mocks.leader.produce.mockImplementation(() => never())
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			void tx.send(TOPIC, { value }).catch(() => {})
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalled())

			const controller = new AbortController()
			const committing = tx.commit({ signal: controller.signal }).catch((e: unknown) => e)
			controller.abort(new Error('closing'))
			const error = await committing

			expect(error).toBeInstanceOf(TransactionAbortedError)
			expect(error).toMatchObject({ outcome: 'aborted', cause: expect.objectContaining({ message: 'closing' }) })
			expect(mocks.txnCoordinator.endTxn).not.toHaveBeenCalled()
			// The abort could not be attempted within the bound: the broker's timeout ends the transaction.
			expect(producer.transactionState).toBe('error')
		})
	})

	/**
	 * The coordinator and the leader of Kafka 3.9 modeled (`modelTransactions`): what the broker does with the
	 * requests that follow a failed epoch, not just answers queued for them.
	 */
	describe('an epoch that fails during the transaction (Java parity)', () => {
		let coordinator: ReturnType<typeof modelTransactions>

		beforeEach(() => {
			mocks = createTransactionalMocks(TOPIC, { partitionCount: 2 })
			coordinator = modelTransactions(mocks)
		})

		/** Producer id, epoch and transactional flag of every batch the leader received. */
		async function produced(): Promise<string[]> {
			const batches = await Promise.all(
				mocks.leader.produce.mock.calls.flatMap(([request]) =>
					request.topics.flatMap(t => t.partitions.map(p => decodeRecordBatch(p.records)))
				)
			)
			return batches.map(
				b => `pid ${b.producerId} epoch ${b.producerEpoch}${b.attributes & 0x10 ? ' transactional' : ''}`
			)
		}
		const answer = (errorCode: ErrorCode, partition = 0) =>
			mocks.leader.produce.mockResolvedValueOnce(
				buildProduceResponse([{ name: TOPIC, partitions: [{ partitionIndex: partition, errorCode }] }])
			)
		const offsets = [{ topic: TOPIC, partition: 0, offset: 1n }]
		const caught = (promise: Promise<unknown>): Promise<unknown> =>
			promise.then(
				() => undefined,
				(error: unknown) => error
			)

		describe.each([
			{
				failure: 'an out-of-order sequence',
				fail: () => answer(ErrorCode.OutOfOrderSequenceNumber),
				attempts: 1,
				error: { errorCode: ErrorCode.OutOfOrderSequenceNumber },
			},
			{
				failure: 'a send whose retries left its outcome unknown',
				fail: () => {
					for (let attempt = 0; attempt < 3; attempt++) {
						mocks.leader.produce.mockRejectedValueOnce(new ConnectionClosedError('connection lost'))
					}
				},
				attempts: 3,
				error: { retriable: true, message: expect.stringContaining('Ambiguous produce outcome') },
			},
		])('after $failure', ({ fail, attempts, error }) => {
			/** Open a transaction whose first send fails its epoch. */
			async function failedTransaction() {
				const producer = makeProducer()
				const tx = await producer.beginTransaction()
				fail()
				const failed = await caught(tx.send(TOPIC, { value, partition: 0 }))
				expect(failed).toMatchObject(error)
				return { producer, tx, failed }
			}
			const before = ['InitProducerId()', 'AddPartitionsToTxn(0)']
			const bumped = ['EndTxn(abort, 0)', 'InitProducerId(1, 0): ConcurrentTransactions', 'InitProducerId(1, 0)']

			it('the transaction can only abort, and nothing is sent until it does', async () => {
				const { producer, tx, failed } = await failedTransaction()

				const rejected = await caught(tx.send(TOPIC, { value, partition: 0 }))
				expect(rejected).toBeInstanceOf(TransactionAbortedError)
				expect((rejected as TransactionAbortedError).cause).toBe(failed)
				await expect(tx.send(TOPIC, { value, partition: 1 })).rejects.toThrow(TransactionAbortedError)
				await expect(tx.sendOffsets({ groupId: 'g1', offsets })).rejects.toThrow(TransactionAbortedError)
				await tick()
				expect(coordinator.requests).toEqual(before)
				expect(producer.transactionState).toBe('in_transaction')
				expect(await produced()).toEqual(Array(attempts).fill('pid 1 epoch 0 transactional'))
			})

			it('abort() sends EndTxn, then InitProducerId bumps the epoch, and the next transaction runs under it', async () => {
				const { producer, tx } = await failedTransaction()

				await tx.abort()
				expect(coordinator.requests).toEqual([...before, ...bumped])
				expect(producer.transactionState).toBe('idle')

				const next = await producer.beginTransaction()
				await next.send(TOPIC, { value, partition: 0 })
				await next.commit()
				expect(coordinator.requests).toEqual([
					...before,
					...bumped,
					'AddPartitionsToTxn(1)',
					'EndTxn(commit, 1)',
				])
				expect(await produced()).toEqual([
					...Array(attempts).fill('pid 1 epoch 0 transactional'),
					'pid 1 epoch 1 transactional',
				])
				expect(coordinator.fenceAborts).toBe(0)
			})

			it('a commit turns into that abort', async () => {
				const { producer, tx, failed } = await failedTransaction()

				const rejected = await caught(tx.commit())
				expect(rejected).toBeInstanceOf(TransactionAbortedError)
				expect(rejected).toMatchObject({ outcome: 'aborted', cause: failed })
				expect(coordinator.requests).toEqual([...before, ...bumped])

				await producer.transaction(async next => {
					await next.send(TOPIC, { value, partition: 0 })
				})
				expect(coordinator.requests.slice(-2)).toEqual(['AddPartitionsToTxn(1)', 'EndTxn(commit, 1)'])
			})

			it('disconnect() aborts with EndTxn, without the bump', async () => {
				const { producer, tx } = await failedTransaction()
				const errors: unknown[] = []
				producer.on('error', error => errors.push(error))

				await producer.disconnect()
				expect(coordinator.requests).toEqual([...before, 'EndTxn(abort, 0)'])
				// The transaction aborted: skipping the bump is no failure of the abort.
				expect(producer.transactionState).toBe('idle')
				expect(errors).toEqual([])
				await expect(tx.commit()).rejects.toMatchObject({
					name: 'InvalidTxnStateError',
					currentState: 'aborted',
				})
			})

			it('a disconnect() while the abort bumps the epoch skips the bump: the abort resolves', async () => {
				const { producer, tx } = await failedTransaction()
				const errors: unknown[] = []
				producer.on('error', error => errors.push(error))
				// EndTxn finds the coordinator; the bump's lookup waits.
				const lookup = mocks.cluster.getCoordinator.getMockImplementation()!
				let release!: () => void
				mocks.cluster.getCoordinator.mockImplementationOnce(lookup).mockImplementationOnce(async (...args) => {
					await new Promise<void>(resolve => (release = resolve))
					return lookup(...args)
				})

				const aborting = caught(tx.abort())
				await vi.waitFor(() => expect(release).toBeDefined())
				await producer.disconnect()
				release()
				await tick()

				expect(await aborting).toBeUndefined()
				expect(coordinator.requests).toEqual([...before, 'EndTxn(abort, 0)'])
				expect(producer.transactionState).toBe('idle')
				expect(errors).toEqual([])
			})
		})

		it('InvalidProducerEpoch on produce is abortable: the EndTxn that follows tells a fence, fatal without a bump', async () => {
			const zombie = makeProducer()
			const tx = await zombie.beginTransaction()
			await tx.send(TOPIC, { value, partition: 0 })
			// A successor with the same transactional id initializes (aborting the zombie's transaction under a newer
			// epoch, whose markers the partition saw) and begins a transaction of its own.
			coordinator.successor()
			coordinator.successorBegins()

			const failed = await caught(tx.send(TOPIC, { value, partition: 0 }))
			expect(failed).toMatchObject({ errorCode: ErrorCode.InvalidProducerEpoch })
			await tick()
			expect(zombie.transactionState).toBe('in_transaction')

			const fenced = await caught(tx.abort())
			expect(fenced).toBeInstanceOf(ProducerFencedError)
			expect(zombie.transactionState).toBe('error')
			await expect(zombie.beginTransaction()).rejects.toBe(fenced)
			expect(coordinator.requests).toEqual([
				'InitProducerId()',
				'AddPartitionsToTxn(0)',
				'EndTxn(abort, 0): ProducerFenced',
			])
			// The only transaction aborted by a fence is the zombie's own, when the successor initialized.
			expect(coordinator.fenceAborts).toBe(1)
			expect(coordinator.successorEndTxn(true)).toBe(ErrorCode.None)
		})

		it.each(['commit', 'abort'] as const)(
			'PRODUCER_FENCED on produce is fatal: no InitProducerId ever again, %s() and every later call reject with it',
			async end => {
				const producer = makeProducer()
				const errors: unknown[] = []
				producer.on('error', error => errors.push(error))
				const tx = await producer.beginTransaction()
				answer(ErrorCode.ProducerFenced)
				const fenced = await caught(tx.send(TOPIC, { value, partition: 0 }))
				expect(fenced).toBeInstanceOf(ProducerFencedError)
				expect(producer.transactionState).toBe('error')

				await expect(tx.send(TOPIC, { value, partition: 0 })).rejects.toBe(fenced)
				await expect(tx.send(TOPIC, { value, partition: 1 })).rejects.toBe(fenced)
				await expect(tx.sendOffsets({ groupId: 'g1', offsets })).rejects.toBe(fenced)
				await expect(tx[end]()).rejects.toBe(fenced)
				await expect(producer.beginTransaction()).rejects.toBe(fenced)
				await expect(producer.transaction(async () => {})).rejects.toBe(fenced)
				await producer.disconnect()

				expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)'])
				expect(errors).toEqual([fenced])
				expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
			}
		)

		it.each([1, 2])(
			'transaction() rejects with the fence its callback met (transactionConcurrency %i)',
			async transactionConcurrency => {
				// With 2, the transaction runs on a lane: the lane is fenced, not the pool.
				const producer = makeProducer({ transactionConcurrency })
				answer(ErrorCode.ProducerFenced)

				const error = await caught(
					producer.transaction(async tx => {
						await tx.send(TOPIC, { value, partition: 0 })
					})
				)
				expect(error).toBeInstanceOf(ProducerFencedError)
				expect(producer.transactionState).toBe('error')
				expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)'])
			}
		)

		/** What a coordinator answers a stale epoch: INVALID_PRODUCER_EPOCH before KIP-588, both a fence (Java). */
		const fences = [
			{ code: 'PRODUCER_FENCED', errorCode: ErrorCode.ProducerFenced },
			{ code: 'INVALID_PRODUCER_EPOCH', errorCode: ErrorCode.InvalidProducerEpoch },
		]

		describe.each(fences)('$code from a coordinator', ({ errorCode }) => {
			beforeEach(() => {
				coordinator.staleEpochError = errorCode
			})

			it.each([
				{
					request: 'AddPartitionsToTxn',
					last: (tx: TransactionHandle) => tx.send(TOPIC, { value, partition: 1 }),
				},
				{
					request: 'AddOffsetsToTxn',
					last: (tx: TransactionHandle) => tx.sendOffsets({ groupId: 'g1', offsets }),
				},
				{
					request: 'TxnOffsetCommit',
					first: (tx: TransactionHandle) => tx.sendOffsets({ groupId: 'g1', offsets }),
					last: (tx: TransactionHandle) => {
						mocks.groupCoordinator.txnOffsetCommit.mockResolvedValueOnce({
							throttleTimeMs: 0,
							topics: [{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode }] }],
						})
						return tx.commit()
					},
				},
				{ request: 'EndTxn', last: (tx: TransactionHandle) => tx.commit() },
			])('to $request is fatal at once: every later call rejects with the fence', async ({ first, last }) => {
				const producer = makeProducer()
				const tx = await producer.beginTransaction()
				await tx.send(TOPIC, { value, partition: 0 })
				await first?.(tx)
				coordinator.successor()
				coordinator.successorBegins()
				const sent = coordinator.requests.length

				const fenced = await caught(last(tx))
				expect(fenced).toBeInstanceOf(ProducerFencedError)
				expect(fenced).toMatchObject({
					errorCode: ErrorCode.ProducerFenced,
					producerId: 1n,
					producerEpoch: 0,
					cause: expect.objectContaining({ errorCode }),
				})
				expect(producer.transactionState).toBe('error')
				// Also once the handle ended (a fenced commit) and after an abort (Java: every call in FATAL_ERROR).
				await expect(tx.abort()).rejects.toBe(fenced)
				await expect(tx.abort()).rejects.toBe(fenced)
				await expect(tx.send(TOPIC, { value, partition: 0 })).rejects.toBe(fenced)
				await expect(tx.sendOffsets({ groupId: 'g1', offsets })).rejects.toBe(fenced)
				await expect(tx.commit()).rejects.toBe(fenced)
				await expect(producer.beginTransaction()).rejects.toBe(fenced)

				// The fenced request was the last one: no EndTxn to abort, no InitProducerId.
				expect(coordinator.requests.length - sent).toBeLessThanOrEqual(1)
				expect(coordinator.requests.filter(request => request.startsWith('InitProducerId'))).toEqual([
					'InitProducerId()',
				])
				expect(coordinator.fenceAborts).toBe(1)
				expect(coordinator.successorEndTxn(true)).toBe(ErrorCode.None)
			})

			it('to the bump is fatal: the abort and every later begin reject with the fence', async () => {
				const producer = makeProducer()
				const tx = await producer.beginTransaction()
				answer(ErrorCode.OutOfOrderSequenceNumber)
				await caught(tx.send(TOPIC, { value, partition: 0 }))
				// A successor initializes between the abort's EndTxn and its bump.
				const initProducerId = mocks.txnCoordinator.initProducerId.getMockImplementation()!
				mocks.txnCoordinator.initProducerId.mockImplementationOnce(async request => {
					coordinator.successor()
					return initProducerId(request)
				})

				const fenced = await caught(tx.abort())
				expect(fenced).toBeInstanceOf(ProducerFencedError)
				expect(fenced).toMatchObject({ cause: expect.objectContaining({ errorCode }) })
				expect(producer.transactionState).toBe('error')
				await expect(tx.abort()).rejects.toBe(fenced)
				await expect(producer.beginTransaction()).rejects.toBe(fenced)
				expect(coordinator.requests).toEqual([
					'InitProducerId()',
					'AddPartitionsToTxn(0)',
					'EndTxn(abort, 0)',
					`InitProducerId(1, 0): ${ErrorCode[errorCode]}`,
				])
			})
		})

		/**
		 * INVALID_PRODUCER_ID_MAPPING: the coordinator maps the transactional id to no producer id any more (it expired)
		 * or to another one. Java's abortableErrorIfPossible: the abort skips EndTxn, which the coordinator rejects too,
		 * and InitProducerId bumps the epoch (with no mapping: a new producer id, epoch 0).
		 */
		describe('INVALID_PRODUCER_ID_MAPPING (the transactional id expired)', () => {
			/** A transaction committed, then the transactional id expired. */
			async function expired() {
				const producer = makeProducer()
				const errors: unknown[] = []
				producer.on('error', error => errors.push(error))
				const first = await producer.beginTransaction()
				await first.send(TOPIC, { value, partition: 0 })
				await first.commit()
				coordinator.expire()
				return { producer, errors }
			}
			const committed = ['InitProducerId()', 'AddPartitionsToTxn(0)', 'EndTxn(commit, 0)']

			/** The next transaction runs under producer id 2. */
			async function runsUnderNewProducerId(producer: Producer) {
				const next = await producer.beginTransaction()
				await next.send(TOPIC, { value, partition: 0 })
				await next.commit()
				expect(coordinator.requests.slice(-2)).toEqual(['AddPartitionsToTxn(0)', 'EndTxn(commit, 0)'])
				expect((await produced()).at(-1)).toBe('pid 2 epoch 0 transactional')
				expect(mocks.txnCoordinator.endTxn).toHaveBeenLastCalledWith(
					expect.objectContaining({ producerId: 2n, producerEpoch: 0, committed: true })
				)
			}

			it.each([
				{
					request: 'AddPartitionsToTxn',
					register: (tx: TransactionHandle) => tx.send(TOPIC, { value, partition: 0 }),
				},
				{
					request: 'AddOffsetsToTxn',
					register: (tx: TransactionHandle) => tx.sendOffsets({ groupId: 'g1', offsets }),
				},
			])(
				'to $request fails the epoch: the abort skips EndTxn and gets a new producer id',
				async ({ request, register }) => {
					const { producer, errors } = await expired()
					const tx = await producer.beginTransaction()

					const failed = await caught(register(tx))
					expect(failed).toBeInstanceOf(TransactionAbortedError)
					expect(failed).toMatchObject({ cause: { errorCode: ErrorCode.InvalidProducerIdMapping } })
					await expect(tx.send(TOPIC, { value, partition: 1 })).rejects.toThrow(TransactionAbortedError)
					expect(producer.transactionState).toBe('in_transaction')

					await tx.abort()
					expect(producer.transactionState).toBe('idle')
					expect(coordinator.requests).toEqual([
						...committed,
						`${request}(0): InvalidProducerIdMapping`,
						'InitProducerId(1, 0)',
					])
					await runsUnderNewProducerId(producer)
					expect(errors).toEqual([])
				}
			)

			it.each(['commit', 'abort'] as const)(
				'to EndTxn(%s) of a transaction that timed out: no EndTxn more, and a new producer id',
				async end => {
					const producer = makeProducer()
					const tx = await producer.beginTransaction()
					await tx.send(TOPIC, { value, partition: 0 })
					// The broker aborts the transaction after transactionTimeoutMs, then the id expires.
					coordinator.expire()

					const ended = await caught(tx[end]())
					if (end === 'commit') {
						expect(ended).toBeInstanceOf(TransactionAbortedError)
						expect(ended).toMatchObject({
							outcome: 'aborted',
							cause: { errorCode: ErrorCode.InvalidProducerIdMapping },
						})
					} else {
						expect(ended).toBeUndefined()
					}
					expect(coordinator.requests).toEqual([
						'InitProducerId()',
						'AddPartitionsToTxn(0)',
						`EndTxn(${end}, 0): InvalidProducerIdMapping`,
						'InitProducerId(1, 0)',
					])
					expect(producer.transactionState).toBe('idle')
					await runsUnderNewProducerId(producer)
				}
			)

			it("a successor holding another producer id fences the bump, which leaves the successor's transaction alone", async () => {
				const { producer, errors } = await expired()
				coordinator.successor()
				coordinator.successorBegins()
				const tx = await producer.beginTransaction()
				await caught(tx.send(TOPIC, { value, partition: 0 }))

				const fenced = await caught(tx.abort())
				expect(fenced).toBeInstanceOf(ProducerFencedError)
				expect(producer.transactionState).toBe('error')
				expect(errors).toEqual([fenced])
				await expect(producer.beginTransaction()).rejects.toBe(fenced)
				expect(coordinator.requests).toEqual([
					...committed,
					'AddPartitionsToTxn(0): InvalidProducerIdMapping',
					'InitProducerId(1, 0): ProducerFenced',
				])
				expect(coordinator.fenceAborts).toBe(0)
				expect(coordinator.successorEndTxn(true)).toBe(ErrorCode.None)
			})
		})

		it('a bump that does not finish within maxBlockMs leaves the producer in the error state', async () => {
			const producer = makeProducer({ maxBlockMs: 50, maxRetryBackoffMs: 5 })
			const tx = await producer.beginTransaction()
			answer(ErrorCode.OutOfOrderSequenceNumber)
			await caught(tx.send(TOPIC, { value, partition: 0 }))
			mocks.txnCoordinator.initProducerId.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.CoordinatorLoadInProgress,
				producerId: -1n,
				producerEpoch: -1,
			})

			await expect(tx.abort()).rejects.toThrow(TimeoutError)
			expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)', 'EndTxn(abort, 0)'])
			expect(producer.transactionState).toBe('error')
			await expect(producer.beginTransaction()).rejects.toThrow(InvalidTxnStateError)
		})

		it('the bump of an abort given up at its timeoutMs sends no InitProducerId once its lookup answers', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			answer(ErrorCode.OutOfOrderSequenceNumber)
			await caught(tx.send(TOPIC, { value, partition: 0 }))
			// EndTxn finds the coordinator; the bump's lookup waits.
			const lookup = mocks.cluster.getCoordinator.getMockImplementation()!
			let release!: () => void
			mocks.cluster.getCoordinator.mockImplementationOnce(lookup).mockImplementationOnce(async (...args) => {
				await new Promise<void>(resolve => (release = resolve))
				return lookup(...args)
			})

			await expect(tx.abort({ timeoutMs: 50 })).rejects.toThrow(TimeoutError)
			release()
			await tick()
			expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)', 'EndTxn(abort, 0)'])
			expect(producer.transactionState).toBe('error')
		})

		it('a send preparing when the epoch failed is not appended, and the abort waits for it', async () => {
			let outOfOrder!: () => void
			mocks.leader.produce.mockImplementationOnce(
				() =>
					new Promise(resolve => {
						outOfOrder = () =>
							resolve(
								buildProduceResponse([
									{
										name: TOPIC,
										partitions: [
											{ partitionIndex: 0, errorCode: ErrorCode.OutOfOrderSequenceNumber },
										],
									},
								])
							)
					})
			)
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const first = caught(tx.send(TOPIC, { value, partition: 0 }))
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalledTimes(1))
			// The second send registers partition 1, the coordinator answering only after the epoch failed.
			const register = mocks.txnCoordinator.addPartitionsToTxn.getMockImplementation()!
			let registered!: () => void
			mocks.txnCoordinator.addPartitionsToTxn.mockImplementationOnce(async request => {
				await new Promise<void>(resolve => (registered = resolve))
				return register(request)
			})
			const second = caught(tx.send(TOPIC, { value, partition: 1 }))
			await vi.waitFor(() => expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2))

			outOfOrder()
			expect(await first).toMatchObject({ errorCode: ErrorCode.OutOfOrderSequenceNumber })
			let aborted = false
			const aborting = tx.abort().then(() => (aborted = true))
			await tick()
			expect(aborted).toBe(false)
			registered()

			expect(await second).toBeInstanceOf(TransactionAbortedError)
			await aborting
			expect(coordinator.requests).toEqual([
				'InitProducerId()',
				'AddPartitionsToTxn(0)',
				'AddPartitionsToTxn(0)',
				'EndTxn(abort, 0)',
				'InitProducerId(1, 0): ConcurrentTransactions',
				'InitProducerId(1, 0)',
			])
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		/**
		 * A coordinator request of the transaction answered retriably (loading), the coordinator lookup of its retry
		 * held until the epoch failed (an out-of-order sequence of the first send).
		 */
		async function failedDuringRetry(
			request: 'addPartitionsToTxn' | 'addOffsetsToTxn',
			send: (tx: TransactionHandle) => Promise<unknown>
		) {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			let outOfOrder!: () => void
			mocks.leader.produce.mockImplementationOnce(
				() =>
					new Promise(resolve => {
						outOfOrder = () =>
							resolve(
								buildProduceResponse([
									{
										name: TOPIC,
										partitions: [
											{ partitionIndex: 0, errorCode: ErrorCode.OutOfOrderSequenceNumber },
										],
									},
								])
							)
					})
			)
			const first = caught(tx.send(TOPIC, { value, partition: 0 }))
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalledTimes(1))

			let release!: () => void
			let held = false
			const lookup = mocks.cluster.getCoordinator.getMockImplementation()!
			mocks.txnCoordinator[request].mockImplementationOnce(async () => {
				mocks.cluster.getCoordinator.mockImplementationOnce(async (...args) => {
					held = true
					await new Promise<void>(resolve => (release = resolve))
					return lookup(...args)
				})
				return {
					throttleTimeMs: 0,
					errorCode: ErrorCode.CoordinatorLoadInProgress,
					results: [],
				}
			})
			const retried = caught(send(tx))
			await vi.waitFor(() => expect(held).toBe(true))
			outOfOrder()
			expect(await first).toMatchObject({ errorCode: ErrorCode.OutOfOrderSequenceNumber })
			release()
			expect(await retried).toBeInstanceOf(TransactionAbortedError)
			await tx.abort()
		}

		it('a partition registration retried after the epoch failed is not sent again', async () => {
			await failedDuringRetry('addPartitionsToTxn', tx => tx.send(TOPIC, { value, partition: 1 }))
			expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2)
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		it('offsets registered by a request retried after the epoch failed are not sent again', async () => {
			await failedDuringRetry('addOffsetsToTxn', tx => tx.sendOffsets({ groupId: 'g1', offsets }))
			expect(mocks.txnCoordinator.addOffsetsToTxn).toHaveBeenCalledTimes(1)
		})
	})

	/** Registrations answered while their transaction ends, against the model of the 3.9 coordinator. */
	describe('nothing of one transaction reaches another', () => {
		let coordinator: ReturnType<typeof modelTransactions>

		beforeEach(() => {
			mocks = createTransactionalMocks(TOPIC, { partitionCount: 2 })
			coordinator = modelTransactions(mocks)
		})

		const offsets = (offset: bigint) => ({ groupId: 'g1', offsets: [{ topic: TOPIC, partition: 0, offset }] })
		const caught = (promise: Promise<unknown>): Promise<unknown> =>
			promise.then(
				() => undefined,
				(error: unknown) => error
			)
		/** The offsets of every TxnOffsetCommit the group coordinator received. */
		const committedOffsets = () =>
			mocks.groupCoordinator.txnOffsetCommit.mock.calls.map(([request]) =>
				request.topics.flatMap(t => t.partitions.map(p => p.committedOffset))
			)

		/**
		 * The next request of this kind is answered by the model only once `release()` was called; or its answer is lost
		 * (the connection closes) after the model applied it, or before it reached the model.
		 */
		function hold(
			request: 'addOffsetsToTxn' | 'addPartitionsToTxn',
			outcome: 'answered' | 'applied' | 'lost' = 'answered'
		) {
			const state = { held: false, release: () => {} }
			const answer = mocks.txnCoordinator[request].getMockImplementation() as (r: unknown) => Promise<never>
			mocks.txnCoordinator[request].mockImplementationOnce(async (r: unknown) => {
				state.held = true
				await new Promise<void>(resolve => (state.release = resolve))
				if (outcome === 'answered') return answer(r)
				if (outcome === 'applied') await answer(r)
				throw new ConnectionClosedError('connection lost')
			})
			return state
		}
		/** The values of the records the leader received, in the order of its produce requests. */
		async function producedValues(): Promise<string[]> {
			const batches = await Promise.all(
				mocks.leader.produce.mock.calls.flatMap(([request]) =>
					request.topics.flatMap(t => t.partitions.map(p => decodeRecordBatch(p.records)))
				)
			)
			return batches.flatMap(batch => batch.records.map(record => String(record.value)))
		}

		it("an aborted transaction's batch queued behind a busy partition is rejected, not produced into the next one", async () => {
			let releaseA1!: () => void
			mocks.leader.produce
				.mockImplementationOnce(
					() =>
						new Promise(resolve => {
							releaseA1 = () =>
								resolve(
									buildProduceResponse([
										{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] },
									])
								)
						})
				)
				.mockResolvedValueOnce(
					buildProduceResponse([
						{ name: TOPIC, partitions: [{ partitionIndex: 1, errorCode: ErrorCode.MessageTooLarge }] },
					])
				)
			const producer = makeProducer()
			const a = await producer.beginTransaction()
			const a1 = a.send(TOPIC, { value: Buffer.from('A1'), partition: 0 })
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalledTimes(1))
			// A2-p0 waits behind A1 for partition 0; A2-p1 is rejected, which rejects the whole send.
			const a2 = await caught(
				a.send(TOPIC, [
					{ value: Buffer.from('A2-p0'), partition: 0 },
					{ value: Buffer.from('A2-p1'), partition: 1 },
				])
			)
			expect(a2).toMatchObject({ errorCode: ErrorCode.MessageTooLarge })

			const aborting = a.abort()
			releaseA1()
			await a1
			await aborting
			const b = await producer.beginTransaction()
			await b.send(TOPIC, { value: Buffer.from('B1'), partition: 0 })
			await b.commit()

			expect(await producedValues()).toEqual(['A1', 'A2-p1', 'B1'])
			expect(coordinator.requests).toEqual([
				'InitProducerId()',
				'AddPartitionsToTxn(0)',
				'AddPartitionsToTxn(0)',
				'EndTxn(abort, 0)',
				'AddPartitionsToTxn(0): ConcurrentTransactions',
				'AddPartitionsToTxn(0)',
				'EndTxn(commit, 0)',
			])
		})

		it("an aborted transaction's offsets answered late are not committed by the next one", async () => {
			const producer = makeProducer()
			const first = await producer.beginTransaction()
			const registration = hold('addOffsetsToTxn')
			const registering = caught(first.sendOffsets(offsets(999n)))
			await vi.waitFor(() => expect(registration.held).toBe(true))

			const aborting = first.abort()
			const beginning = producer.beginTransaction()
			await tick()
			// The abort waits for the answer: EndTxn must follow it, as the coordinator may begin the transaction.
			expect(coordinator.requests).toEqual(['InitProducerId()'])
			registration.release()

			await aborting
			const second = await beginning
			await second.sendOffsets(offsets(5n))
			await second.commit()

			// Offset 999 is the aborted transaction's.
			expect(committedOffsets()).toEqual([[5n]])
			expect(await registering).toBeInstanceOf(TransactionAbortedError)
			expect(coordinator.requests).toEqual([
				'InitProducerId()',
				'AddOffsetsToTxn(0)',
				'EndTxn(abort, 0)',
				'AddOffsetsToTxn(0): ConcurrentTransactions',
				'AddOffsetsToTxn(0)',
				'EndTxn(commit, 0)',
			])
		})

		it('a transaction() whose callback timed out awaiting its offsets leaks none into the next transaction', async () => {
			const producer = makeProducer()
			const registration = hold('addOffsetsToTxn')
			const failed = caught(producer.transaction(tx => tx.sendOffsets(offsets(999n)), { timeoutMs: 30 }))
			await vi.waitFor(() => expect(registration.held).toBe(true))
			await tick(60)

			const beginning = producer.beginTransaction()
			registration.release()
			expect(await failed).toBeInstanceOf(TransactionAbortedError)
			const next = await beginning
			await next.sendOffsets(offsets(5n))
			await next.commit()

			expect(committedOffsets()).toEqual([[5n]])
			expect(coordinator.requests.slice(0, 3)).toEqual([
				'InitProducerId()',
				'AddOffsetsToTxn(0)',
				'EndTxn(abort, 0)',
			])
		})

		it('a send whose partition is answered once the transaction aborts is rejected and produces nothing', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const registration = hold('addPartitionsToTxn')
			const sending = caught(tx.send(TOPIC, { value }))
			await vi.waitFor(() => expect(registration.held).toBe(true))

			const aborting = tx.abort()
			registration.release()

			expect(await sending).toBeInstanceOf(TransactionAbortedError)
			await aborting
			expect(mocks.leader.produce).not.toHaveBeenCalled()
			expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)', 'EndTxn(abort, 0)'])
		})

		it('a commit waits for offsets still being registered, and commits them', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const registration = hold('addOffsetsToTxn')
			const registering = tx.sendOffsets(offsets(7n))
			await vi.waitFor(() => expect(registration.held).toBe(true))

			const committing = tx.commit()
			await tick()
			expect(coordinator.requests).toEqual(['InitProducerId()'])
			registration.release()
			await registering
			await committing

			expect(committedOffsets()).toEqual([[7n]])
			expect(coordinator.requests).toEqual(['InitProducerId()', 'AddOffsetsToTxn(0)', 'EndTxn(commit, 0)'])
		})

		it('a commit aborts when offsets it waited for failed to register', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			mocks.txnCoordinator.addOffsetsToTxn.mockResolvedValueOnce({
				throttleTimeMs: 0,
				errorCode: ErrorCode.GroupAuthorizationFailed,
			})
			const registering = caught(tx.sendOffsets(offsets(7n)))

			const error = await caught(tx.commit())
			expect(error).toMatchObject({ outcome: 'aborted', cause: await registering })
			expect(error).toBeInstanceOf(TransactionAbortedError)
			expect(await registering).toMatchObject({ errorCode: ErrorCode.GroupAuthorizationFailed })
			expect(mocks.groupCoordinator.txnOffsetCommit).not.toHaveBeenCalled()
			// The coordinator never began the transaction: nothing to end.
			expect(coordinator.requests).toEqual(['InitProducerId()'])
			expect(producer.transactionState).toBe('idle')
		})

		it.each([
			{
				invalid: 'an unjoined generation',
				params: {
					consumerGroupMetadata: { groupId: 'g1', generationId: -1, memberId: '', groupInstanceId: null },
					offsets: offsets(7n).offsets,
				},
				error: CommitFailedError,
			},
			{ invalid: 'no group', params: { offsets: offsets(7n).offsets }, error: Error },
		])('a sendOffsets() with $invalid, not awaited, makes the commit abort', async ({ params, error }) => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value, partition: 0 })
			const registering = caught(tx.sendOffsets(params))

			const aborted = await caught(tx.commit())
			expect(await registering).toBeInstanceOf(error)
			expect(aborted).toBeInstanceOf(TransactionAbortedError)
			expect(aborted).toMatchObject({ outcome: 'aborted', cause: await registering })
			expect(mocks.groupCoordinator.txnOffsetCommit).not.toHaveBeenCalled()
			expect(coordinator.requests).toEqual(['InitProducerId()', 'AddPartitionsToTxn(0)', 'EndTxn(abort, 0)'])
		})

		/**
		 * A registration whose answer is lost while its transaction aborts: the coordinator may have begun the
		 * transaction (Java retries it to an answer before EndTxn).
		 */
		describe.each([
			{
				request: 'addOffsetsToTxn' as const,
				registered: 'AddOffsetsToTxn(0)',
				register: (tx: TransactionHandle) => tx.sendOffsets(offsets(1n)),
			},
			{
				request: 'addPartitionsToTxn' as const,
				registered: 'AddPartitionsToTxn(0)',
				register: (tx: TransactionHandle) => tx.send(TOPIC, { value, partition: 0 }),
			},
		])(
			'$request answered with a lost connection while its transaction aborts',
			({ request, registered, register }) => {
				it.each([
					{
						outcome: 'applied' as const,
						// The coordinator began the transaction: EndTxn aborts it.
						requests: ['InitProducerId()', registered, 'EndTxn(abort, 0)'],
					},
					{
						outcome: 'lost' as const,
						// It never did: the coordinator has no transaction to abort.
						requests: ['InitProducerId()', 'EndTxn(abort, 0): InvalidTxnState'],
					},
				])('the abort sends EndTxn; $outcome by the coordinator', async ({ outcome, requests }) => {
					const producer = makeProducer()
					const tx = await producer.beginTransaction()
					const registration = hold(request, outcome)
					const registering = caught(register(tx))
					await vi.waitFor(() => expect(registration.held).toBe(true))

					const aborting = tx.abort()
					registration.release()
					await aborting
					expect(await registering).toBeInstanceOf(TransactionAbortedError)
					expect(coordinator.requests).toEqual(requests)
					expect(coordinator.status).not.toBe('Ongoing')
					expect(producer.transactionState).toBe('idle')

					const next = await producer.beginTransaction()
					await next.send(TOPIC, { value, partition: 0 })
					await next.commit()
					expect(coordinator.requests.at(-1)).toBe('EndTxn(commit, 0)')
				})
			}
		)

		it.each([
			{ sent: 'nothing', send: async () => {} },
			{ sent: 'a record', send: (tx: TransactionHandle) => tx.send(TOPIC, { value, partition: 0 }) },
		])(
			'a fence met while the abort waits for a registration is fatal: no EndTxn ($sent sent before)',
			async ({ send }) => {
				const producer = makeProducer()
				const errors: unknown[] = []
				producer.on('error', error => errors.push(error))
				const tx = await producer.beginTransaction()
				await send(tx)
				const registration = hold('addOffsetsToTxn')
				const registering = caught(tx.sendOffsets(offsets(1n)))
				await vi.waitFor(() => expect(registration.held).toBe(true))
				// The model answers the held registration PRODUCER_FENCED.
				coordinator.successor()

				const aborting = caught(tx.abort())
				registration.release()
				const fenced = await registering
				expect(fenced).toBeInstanceOf(ProducerFencedError)
				expect(await aborting).toBe(fenced)
				expect(producer.transactionState).toBe('error')
				expect(errors).toEqual([fenced])
				expect(coordinator.requests.filter(request => request.startsWith('EndTxn'))).toEqual([])
				await expect(producer.beginTransaction()).rejects.toBe(fenced)
			}
		)
	})

	describe('disconnect', () => {
		it('aborts a transaction left open and rejects begins still waiting', async () => {
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })
			const waiting = producer.beginTransaction()

			await producer.disconnect()

			await expect(waiting).rejects.toThrow('Producer disconnected')
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
			await expect(tx.commit()).rejects.toThrow(InvalidTxnStateError)
			await expect(producer.beginTransaction()).rejects.toThrow('Producer disconnected')
		})

		it('returns within timeoutMs against an unresponsive broker, rejecting what is in flight', async () => {
			mocks.leader.produce.mockImplementation(() => never())
			mocks.txnCoordinator.endTxn.mockImplementation(() => never())
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const sent = tx.send(TOPIC, { value })
			sent.catch(() => {})
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalled())

			const startedAt = Date.now()
			await producer.disconnect({ timeoutMs: 100 })

			expect(Date.now() - startedAt).toBeLessThan(1000)
			await expect(sent).rejects.toThrow('Producer disconnected')
			// The abort did not finish: the transaction is left to the broker's timeout.
			expect(producer.transactionState).toBe('error')
		})

		it('waits for a commit in progress', async () => {
			let answer!: () => void
			mocks.txnCoordinator.endTxn.mockImplementation(
				() => new Promise(resolve => (answer = () => resolve({ throttleTimeMs: 0, errorCode: ErrorCode.None })))
			)
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await tx.send(TOPIC, { value })
			const committed = tx.commit()
			await vi.waitFor(() => expect(mocks.txnCoordinator.endTxn).toHaveBeenCalled())

			const disconnected = producer.disconnect()
			answer()
			await expect(committed).resolves.toBeUndefined()
			await disconnected
			expect(mocks.txnCoordinator.endTxn).toHaveBeenCalledTimes(1)
		})

		it('still refuses to run under a transaction() call', async () => {
			const producer = makeProducer()
			let release!: () => void
			const running = producer.transaction(() => new Promise<void>(resolve => (release = resolve)))
			await tick()

			await expect(producer.disconnect()).rejects.toThrow(/active or queued/)
			release()
			await running
			await expect(producer.disconnect()).resolves.toBeUndefined()
		})
	})
})
