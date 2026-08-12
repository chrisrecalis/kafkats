import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { TransactionAbortedError } from '@/client/errors.js'
import { createMockBroker, createMockCluster, buildProduceResponse, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'

function gate() {
	let release!: () => void
	const opened = new Promise<void>(resolve => {
		release = resolve
	})
	return { opened, release }
}

async function tick(ms = 10) {
	await new Promise(resolve => setTimeout(resolve, ms))
}

describe('Producer transaction concurrency (lane pool)', () => {
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

	function makeProducer(transactionConcurrency = 2) {
		return new Producer(mockCluster, {
			lingerMs: 0,
			retries: 2,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
			transactionConcurrency,
		})
	}

	it('rejects invalid transactionConcurrency values', () => {
		expect(() => makeProducer(0)).toThrow(/integer >= 1/)
		expect(() => makeProducer(1.5)).toThrow(/integer >= 1/)
		expect(() => new Producer(mockCluster, { transactionConcurrency: 2 })).toThrow(/requires transactionalId/)
	})

	it('runs transactions genuinely in parallel up to the concurrency level', async () => {
		const producer = makeProducer(2)
		const events: string[] = []
		const g1 = gate()

		const first = producer.transaction(async tx => {
			events.push('tx1:start')
			await tx.send(TOPIC, { value: Buffer.from('one') })
			await g1.opened
			events.push('tx1:end')
		})

		const second = producer.transaction(async tx => {
			events.push('tx2:start')
			await tx.send(TOPIC, { value: Buffer.from('two') })
			events.push('tx2:end')
		})

		// tx2 must start AND commit while tx1 is still parked mid-transaction.
		await second
		expect(events).toEqual(['tx1:start', 'tx2:start', 'tx2:end'])
		expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(1)

		g1.release()
		await first
		expect(events).toEqual(['tx1:start', 'tx2:start', 'tx2:end', 'tx1:end'])
		expect(txnCoordinator.endTxn).toHaveBeenCalledTimes(2)
	})

	it('uses a distinct transactional ID per lane, keeping the base ID on lane 0', async () => {
		const producer = makeProducer(2)
		const g1 = gate()

		const first = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('one') })
			await g1.opened
		})
		const second = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('two') })
		})

		await second
		g1.release()
		await first

		const initIds = txnCoordinator.initProducerId.mock.calls.map(
			call => (call[0] as { transactionalId: string }).transactionalId
		)
		expect(new Set(initIds)).toEqual(new Set(['tx-1', 'tx-1-1']))
	})

	it('admits waiting transactions first-in first-out once a lane frees up', async () => {
		const producer = makeProducer(2)
		const events: string[] = []
		const g1 = gate()
		const g2 = gate()
		const g3 = gate()

		const first = producer.transaction(async () => {
			events.push('tx1:start')
			await g1.opened
		})
		const second = producer.transaction(async () => {
			events.push('tx2:start')
			await g2.opened
		})
		const third = producer.transaction(async () => {
			events.push('tx3:start')
			await g3.opened
		})
		const fourth = producer.transaction(async () => {
			events.push('tx4:start')
		})

		await tick()
		expect(events).toEqual(['tx1:start', 'tx2:start'])

		// Freeing one lane admits tx3 (queued first), not tx4.
		g1.release()
		await tick()
		expect(events).toEqual(['tx1:start', 'tx2:start', 'tx3:start'])

		g2.release()
		g3.release()
		await Promise.all([first, second, third, fourth])
		expect(events).toEqual(['tx1:start', 'tx2:start', 'tx3:start', 'tx4:start'])
	})

	it('emits transaction:queued only when all lanes are busy', async () => {
		const producer = makeProducer(2)
		const queuedEvents: Array<{ queued: number }> = []
		producer.on('transaction:queued', info => queuedEvents.push(info))

		const g1 = gate()
		const g2 = gate()

		const first = producer.transaction(async () => {
			await g1.opened
		})
		const second = producer.transaction(async () => {
			await g2.opened
		})
		const third = producer.transaction(async () => {})

		await tick()
		expect(queuedEvents).toEqual([{ queued: 2 }])

		g1.release()
		g2.release()
		await Promise.all([first, second, third])
	})

	it('keeps other lanes usable when one transaction aborts', async () => {
		const producer = makeProducer(2)
		const g1 = gate()

		const doomed = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('doomed') })
			await g1.opened
			throw new Error('user abort')
		})
		const survivor = producer.transaction(async tx => {
			await tx.send(TOPIC, { value: Buffer.from('survivor') })
		})

		await survivor
		g1.release()
		await expect(doomed).rejects.toThrow(TransactionAbortedError)

		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(1, expect.objectContaining({ committed: true }))
		expect(txnCoordinator.endTxn).toHaveBeenNthCalledWith(2, expect.objectContaining({ committed: false }))

		// The aborted lane returns to the pool and works again.
		await expect(
			producer.transaction(async tx => {
				await tx.send(TOPIC, { value: Buffer.from('after-abort') })
			})
		).resolves.toBeUndefined()
	})

	it('throws immediately on a nested transaction instead of taking another lane', async () => {
		const producer = makeProducer(2)

		await expect(
			producer.transaction(async () => {
				await producer.transaction(async () => {})
			})
		).rejects.toThrow(/inside an active transaction callback/)
	})

	it('refuses to disconnect while pooled transactions are active or queued', async () => {
		const producer = makeProducer(2)
		const g1 = gate()

		const first = producer.transaction(async () => {
			await g1.opened
		})

		await expect(producer.disconnect()).rejects.toThrow(/active or queued/)

		g1.release()
		await first
		await expect(producer.disconnect()).resolves.toBeUndefined()
	})
})
