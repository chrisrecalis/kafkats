import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { InvalidTxnStateError, TransactionAbortedError } from '@/client/errors.js'
import { createMockBroker, createMockCluster, buildProduceResponse, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer transaction queueing', () => {
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

	it('serializes concurrent transactions instead of throwing', async () => {
		const producer = makeProducer()
		const events: string[] = []

		let releaseFirst!: () => void
		const firstGate = new Promise<void>(resolve => {
			releaseFirst = resolve
		})

		const first = producer.transaction(async tx => {
			events.push('tx1:start')
			await tx.send(TOPIC, { value: Buffer.from('one') })
			await firstGate
			events.push('tx1:end')
		})

		const second = producer.transaction(async tx => {
			events.push('tx2:start')
			await tx.send(TOPIC, { value: Buffer.from('two') })
			events.push('tx2:end')
		})

		// Give the second call a chance to (incorrectly) start early.
		await new Promise(resolve => setTimeout(resolve, 10))
		expect(events).toEqual(['tx1:start'])

		releaseFirst()
		await Promise.all([first, second])

		expect(events).toEqual(['tx1:start', 'tx1:end', 'tx2:start', 'tx2:end'])
		expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(1, expect.objectContaining({ committed: true }))
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(2, expect.objectContaining({ committed: true }))
	})

	it('emits transaction:queued when a transaction waits for capacity', async () => {
		const producer = makeProducer()
		const queuedEvents: Array<{ queued: number }> = []
		producer.on('transaction:queued', info => queuedEvents.push(info))

		let release!: () => void
		const gate = new Promise<void>(resolve => {
			release = resolve
		})

		const first = producer.transaction(async () => {
			await gate
		})
		const second = producer.transaction(async () => {})
		const third = producer.transaction(async () => {})

		release()
		await Promise.all([first, second, third])

		expect(queuedEvents).toEqual([{ queued: 1 }, { queued: 2 }])
	})

	it('does not wedge the queue when a transaction:queued listener throws', async () => {
		const producer = makeProducer()
		producer.on('transaction:queued', () => {
			throw new Error('bad listener')
		})

		let release!: () => void
		const gate = new Promise<void>(resolve => {
			release = resolve
		})

		const first = producer.transaction(async () => {
			await gate
		})
		// This call fires transaction:queued; the throwing listener must not
		// leave the queue permanently blocked or fail the transaction.
		const second = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('two') })
		})

		release()
		await expect(first).resolves.toBeUndefined()
		await expect(second).resolves.toBeUndefined()

		// The queue must still be usable afterwards.
		await expect(producer.transaction(async () => {})).resolves.toBeUndefined()
	})

	it('runs a queued transaction after the previous one aborts', async () => {
		const producer = makeProducer()

		const first = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('doomed') })
			throw new Error('user abort')
		})
		const second = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('survivor') })
		})

		await expect(first).rejects.toThrow(TransactionAbortedError)
		await expect(second).resolves.toBeUndefined()

		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(1, expect.objectContaining({ committed: false }))
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(2, expect.objectContaining({ committed: true }))
	})

	it('rejects queued transactions when the producer enters a fatal state', async () => {
		// EndTxn(abort) definitively fails: the abort marker is never written, so
		// the producer must refuse all further transactions - including queued ones.
		txnCoordinator.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.InvalidTxnState })

		const producer = makeProducer()

		const first = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('msg') })
			throw new Error('user abort')
		})
		const second = producer.transaction(async () => {})

		await expect(first).rejects.toThrow(TransactionAbortedError)
		await expect(second).rejects.toThrow(InvalidTxnStateError)
	})

	it('throws immediately on a nested transaction instead of deadlocking', async () => {
		const producer = makeProducer()

		await expect(
			producer.transaction(async () => {
				await producer.transaction(async () => {})
			})
		).rejects.toThrow(/inside an active transaction callback/)
	})

	it('allows a transaction on a different producer from inside a callback', async () => {
		const producerA = makeProducer()
		const producerB = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-2',
		})

		await expect(
			producerA.transaction(async () => {
				await producerB.transaction(async tx => {
					await tx.send(TOPIC, { value: Buffer.from('cross-producer') })
				})
			})
		).resolves.toBeUndefined()
	})

	it('detects re-entry through another producer’s callback (A → B → A)', async () => {
		const producerA = makeProducer()
		const producerB = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-2',
		})

		await expect(
			producerA.transaction(async () => {
				await producerB.transaction(async () => {
					await producerA.transaction(async () => {})
				})
			})
		).rejects.toThrow(/inside an active transaction callback/)
	})

	it('refuses to disconnect while transactions are queued', async () => {
		const producer = makeProducer()

		let release!: () => void
		const gate = new Promise<void>(resolve => {
			release = resolve
		})

		const first = producer.transaction(async () => {
			await gate
		})
		const second = producer.transaction(async () => {})

		await expect(producer.disconnect()).rejects.toThrow(/active or queued/)

		release()
		await Promise.all([first, second])
		await expect(producer.disconnect()).resolves.toBeUndefined()
	})
})
