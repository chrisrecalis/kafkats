import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import type { ProducerConfig } from '@/producer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { InvalidTxnStateError, ProducerFencedError, TimeoutError, TransactionAbortedError } from '@/client/errors.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'
import { buildProduceResponse, createTransactionalMocks } from './_helpers.js'

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
			void tx.send(TOPIC, { value }).catch(() => {})
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalled())

			await tx.abort()
			expect(events).toEqual(['produce:settled', 'endTxn'])
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

			await expect(tx.abort({ timeoutMs: 30 })).rejects.toThrow(TimeoutError)
			expect(producer.transactionState).toBe('error')
			await expect(producer.beginTransaction()).rejects.toThrow(InvalidTxnStateError)
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
			await expect(tx.abort()).resolves.toBeUndefined()
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

	describe('fenced while the transaction is open (the send path never re-initializes it)', () => {
		const fencedProduce = (partition: number) =>
			buildProduceResponse([
				{ name: TOPIC, partitions: [{ partitionIndex: partition, errorCode: ErrorCode.ProducerFenced }] },
			])
		const identity = (producerId: bigint, producerEpoch: number) => ({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId,
			producerEpoch,
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

		/** Epoch and outcome of every EndTxn the coordinator received. */
		const endTxns = () => mocks.txnCoordinator.endTxn.mock.calls.map(([r]) => [r.producerEpoch, r.committed])
		/** The coordinator answers EndTxn only at `epoch()`, its current epoch of the producer id (KIP-360). */
		const endTxnAtEpoch = (epoch: () => number) =>
			mocks.txnCoordinator.endTxn.mockImplementation(async request => ({
				throttleTimeMs: 0,
				errorCode: request.producerEpoch === epoch() ? ErrorCode.None : ErrorCode.ProducerFenced,
			}))

		it('fenced by a successor: the transaction sends nothing more, and its abort at the fenced epoch is fatal (Java)', async () => {
			// The successor bumped the epoch: the coordinator rejects the re-initialization and the abort at epoch 0.
			mocks.txnCoordinator.initProducerId
				.mockResolvedValueOnce(identity(1n, 0))
				.mockResolvedValue({ ...identity(-1n, -1), errorCode: ErrorCode.ProducerFenced })
			endTxnAtEpoch(() => 2)
			mocks.leader.produce.mockResolvedValueOnce(fencedProduce(0))
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await expect(tx.send(TOPIC, { value, partition: 0 })).rejects.toThrow(ProducerFencedError)
			await vi.waitFor(() => expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(2))

			const rejected: unknown = await tx.send(TOPIC, { value, partition: 0 }).catch((error: unknown) => error)
			expect(rejected).toBeInstanceOf(TransactionAbortedError)
			expect((rejected as TransactionAbortedError).cause).toBeInstanceOf(ProducerFencedError)
			await expect(
				tx.sendOffsets({ groupId: 'g1', offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
			).rejects.toThrow(TransactionAbortedError)
			expect(mocks.txnCoordinator.addOffsetsToTxn).not.toHaveBeenCalled()
			await expect(tx.commit()).rejects.toThrow(TransactionAbortedError)

			expect(endTxns()).toEqual([[0, false]])
			expect(producer.transactionState).toBe('error')
			await expect(producer.beginTransaction()).rejects.toThrow(InvalidTxnStateError)
			expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(2)
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		it('a send after a re-initialization that timed out is rejected; the abort and the next transaction go on', async () => {
			// An out-of-order sequence does not fence, and the re-initialization never reaches the loading coordinator:
			// epoch 0 stays current, so the coordinator accepts the abort at it.
			let coordinatorEpoch = 0
			let loading = true
			mocks.txnCoordinator.initProducerId.mockResolvedValueOnce(identity(1n, 0)).mockImplementation(async () => {
				if (loading) return { ...identity(-1n, -1), errorCode: ErrorCode.CoordinatorLoadInProgress }
				coordinatorEpoch += 1
				return identity(1n, coordinatorEpoch)
			})
			endTxnAtEpoch(() => coordinatorEpoch)
			mocks.leader.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.OutOfOrderSequenceNumber }] },
				])
			)
			const producer = makeProducer({ maxBlockMs: 50, retryBackoffMs: 5, maxRetryBackoffMs: 5 })
			const tx = await producer.beginTransaction()
			const outOfOrder: unknown = await tx.send(TOPIC, { value, partition: 0 }).catch((error: unknown) => error)
			expect(outOfOrder).toMatchObject({ errorCode: ErrorCode.OutOfOrderSequenceNumber })
			// The re-initialization the fence started gives up after maxBlockMs.
			await tick(150)
			const inits = mocks.txnCoordinator.initProducerId.mock.calls.length
			expect(producer.transactionState).toBe('in_transaction')

			loading = false
			const rejected: unknown = await tx.send(TOPIC, { value, partition: 0 }).catch((error: unknown) => error)
			expect(rejected).toBeInstanceOf(TransactionAbortedError)
			expect((rejected as TransactionAbortedError).cause).toBe(outOfOrder)
			await expect(
				tx.sendOffsets({ groupId: 'g1', offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
			).rejects.toThrow(TransactionAbortedError)
			expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(inits)
			expect(mocks.txnCoordinator.addOffsetsToTxn).not.toHaveBeenCalled()
			await expect(tx.commit()).rejects.toThrow(TransactionAbortedError)
			expect(endTxns()).toEqual([[0, false]])

			// Only a begin initializes it again; its transaction is the new epoch's.
			const next = await producer.beginTransaction()
			await next.send(TOPIC, { value, partition: 0 })
			await next.commit()
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional', 'pid 1 epoch 1 transactional'])
			expect(mocks.txnCoordinator.addPartitionsToTxn.mock.calls.map(([r]) => r.producerEpoch)).toEqual([0, 1])
			expect(endTxns()).toEqual([
				[0, false],
				[1, true],
			])
		})

		it('a send while the re-initialization runs is rejected at once, not sent under the new epoch', async () => {
			let initialized!: () => void
			mocks.txnCoordinator.initProducerId
				.mockResolvedValueOnce(identity(1n, 0))
				.mockImplementationOnce(() => new Promise(resolve => (initialized = () => resolve(identity(1n, 2)))))
			mocks.leader.produce.mockResolvedValueOnce(fencedProduce(0))
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			await expect(tx.send(TOPIC, { value, partition: 0 })).rejects.toThrow(ProducerFencedError)
			await vi.waitFor(() => expect(mocks.txnCoordinator.initProducerId).toHaveBeenCalledTimes(2))

			let settled = false
			const sent = tx.send(TOPIC, { value, partition: 0 }).finally(() => (settled = true))
			sent.catch(() => {})
			await tick()
			expect(settled).toBe(true)
			await expect(sent).rejects.toThrow(TransactionAbortedError)

			initialized()
			await vi.waitFor(() => expect(producer.transactionState).toBe('idle'))
			await tx.abort()
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		it('a send preparing when the fence came is not appended under the new epoch', async () => {
			mocks = createTransactionalMocks(TOPIC, { partitionCount: 2 })
			mocks.txnCoordinator.initProducerId
				.mockResolvedValueOnce(identity(1n, 0))
				.mockResolvedValue(identity(1n, 2))
			let fence!: () => void
			mocks.leader.produce.mockImplementationOnce(
				() => new Promise(resolve => (fence = () => resolve(fencedProduce(0))))
			)
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const first = tx.send(TOPIC, { value, partition: 0 })
			first.catch(() => {})
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalledTimes(1))
			// The second send registers partition 1, the coordinator answering only after the fence.
			let registered!: () => void
			mocks.txnCoordinator.addPartitionsToTxn.mockImplementationOnce(
				() =>
					new Promise(
						resolve =>
							(registered = () =>
								resolve({
									throttleTimeMs: 0,
									errorCode: ErrorCode.None,
									results: [
										{
											name: TOPIC,
											resultsByPartition: [{ partitionIndex: 1, errorCode: ErrorCode.None }],
										},
									],
								}))
					)
			)
			const second = tx.send(TOPIC, { value, partition: 1 })
			second.catch(() => {})
			await vi.waitFor(() => expect(mocks.txnCoordinator.addPartitionsToTxn).toHaveBeenCalledTimes(2))

			fence()
			await expect(first).rejects.toThrow(ProducerFencedError)
			await vi.waitFor(() => expect(producer.transactionState).toBe('idle'))
			registered()

			await expect(second).rejects.toThrow(TransactionAbortedError)
			await tick()
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		/**
		 * A transaction whose first send's produce is held until the fence: answered PRODUCER_FENCED, the
		 * re-initialization then getting epoch 2. `holdNextLookup()`, called by the retriable (`loading`) answer of a
		 * coordinator request, holds the coordinator lookup of its retry until `fenceAndReinitialize()` is done.
		 */
		async function fencedDuringRetry(partitionCount: number) {
			mocks = createTransactionalMocks(TOPIC, { partitionCount })
			mocks.txnCoordinator.initProducerId
				.mockResolvedValueOnce(identity(1n, 0))
				.mockResolvedValue(identity(1n, 2))
			let fence!: () => void
			mocks.leader.produce.mockImplementationOnce(
				() => new Promise(resolve => (fence = () => resolve(fencedProduce(0))))
			)
			const producer = makeProducer()
			const tx = await producer.beginTransaction()
			const first = tx.send(TOPIC, { value, partition: 0 })
			first.catch(() => {})
			await vi.waitFor(() => expect(mocks.leader.produce).toHaveBeenCalledTimes(1))

			let release!: () => void
			let held = false
			const lookup = mocks.cluster.getCoordinator.getMockImplementation()!
			const holdNextLookup = () =>
				mocks.cluster.getCoordinator.mockImplementationOnce(async (...args) => {
					held = true
					await new Promise<void>(resolve => (release = resolve))
					return lookup(...args)
				})
			const loading = { throttleTimeMs: 0, errorCode: ErrorCode.CoordinatorLoadInProgress }
			const fenceAndReinitialize = async () => {
				await vi.waitFor(() => expect(held).toBe(true))
				fence()
				await expect(first).rejects.toThrow(ProducerFencedError)
				await vi.waitFor(() => expect(producer.transactionState).toBe('idle'))
				release()
			}
			return { producer, tx, holdNextLookup, loading, fenceAndReinitialize }
		}

		it('a partition registration retried across the fence never goes out under the new epoch', async () => {
			const { tx, holdNextLookup, loading, fenceAndReinitialize } = await fencedDuringRetry(2)
			mocks.txnCoordinator.addPartitionsToTxn.mockImplementationOnce(async () => {
				holdNextLookup()
				return { ...loading, results: [] }
			})
			const second = tx.send(TOPIC, { value, partition: 1 })
			second.catch(() => {})

			await fenceAndReinitialize()
			await expect(second).rejects.toThrow(TransactionAbortedError)
			expect(mocks.txnCoordinator.addPartitionsToTxn.mock.calls.map(([r]) => r.producerEpoch)).toEqual([0, 0])
			expect(await produced()).toEqual(['pid 1 epoch 0 transactional'])
		})

		it('offsets registered by a request retried across the fence never go out under the new epoch', async () => {
			const { tx, holdNextLookup, loading, fenceAndReinitialize } = await fencedDuringRetry(1)
			mocks.txnCoordinator.addOffsetsToTxn.mockImplementationOnce(async () => {
				holdNextLookup()
				return loading
			})
			const offsets = tx.sendOffsets({ groupId: 'g1', offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
			offsets.catch(() => {})

			await fenceAndReinitialize()
			await expect(offsets).rejects.toThrow(TransactionAbortedError)
			expect(mocks.txnCoordinator.addOffsetsToTxn.mock.calls.map(([r]) => r.producerEpoch)).toEqual([0])
		})
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
