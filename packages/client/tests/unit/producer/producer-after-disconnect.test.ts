import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

describe('Producer: nothing is produced once disconnect() has returned', () => {
	beforeEach(() => {
		vi.useFakeTimers()
	})

	afterEach(() => {
		vi.useRealTimers()
		vi.restoreAllMocks()
	})

	it('rejects a send that is still looking up its topic and never produces it', async () => {
		const cluster = createMockCluster()
		const broker = createMockBroker(1)
		cluster.getLeaderForPartition.mockResolvedValue(broker)
		broker.produce.mockResolvedValue(
			buildProduceResponse([
				{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] },
			])
		)
		// The metadata request does not come back before the shutdown deadline.
		const metadata = await cluster.refreshMetadata(['test-topic'])
		let answerMetadata!: () => void
		cluster.refreshMetadata.mockImplementation(
			() => new Promise(resolve => (answerMetadata = () => resolve(metadata)))
		)
		cluster.refreshMetadata.mockClear()
		const producer = new Producer(cluster, { retries: 0 })

		let outcome = 'pending'
		const first = producer.send('test-topic', { value: Buffer.from('a'), partition: 0 })
		const second = producer.send('test-topic', { value: Buffer.from('b'), partition: 0 })
		void first.then(
			() => (outcome = 'sent'),
			(error: Error) => (outcome = error.message)
		)
		second.catch(() => {})

		const disconnected = producer.disconnect()
		await vi.advanceTimersByTimeAsync(6_000)
		await disconnected

		expect(outcome).toBe('Producer disconnected')
		await expect(second).rejects.toThrow('Producer disconnected')

		// The answer arrives late: nothing may be produced any more, and no second lookup starts.
		answerMetadata()
		await vi.advanceTimersByTimeAsync(1_000)
		expect(broker.produce).not.toHaveBeenCalled()
		expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
	})

	it('does not retry a produce request that fails after disconnect() has returned', async () => {
		const cluster = createMockCluster()
		const broker = createMockBroker(1)
		cluster.getLeaderForPartition.mockResolvedValue(broker)
		const answer = (errorCode: ErrorCode) =>
			buildProduceResponse([{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode }] }])
		// The first request is not answered before the shutdown deadline.
		let answerFirst!: (errorCode: ErrorCode) => void
		broker.produce.mockImplementationOnce(
			() => new Promise(resolve => (answerFirst = errorCode => resolve(answer(errorCode))))
		)
		broker.produce.mockResolvedValue(answer(ErrorCode.None))
		const producer = new Producer(cluster, { retries: 3 })

		const sent = producer.send('test-topic', { value: Buffer.from('a'), partition: 0 })
		sent.catch(() => {})
		await vi.advanceTimersByTimeAsync(50)
		expect(broker.produce).toHaveBeenCalledTimes(1)

		const disconnected = producer.disconnect()
		await vi.advanceTimersByTimeAsync(6_000)
		await disconnected
		await expect(sent).rejects.toThrow('Producer disconnected')

		// The late answer asks for a retry: the record was reported as failed, so it must not be
		// written after all.
		answerFirst(ErrorCode.NotLeaderOrFollower)
		await vi.advanceTimersByTimeAsync(10_000)
		expect(broker.produce).toHaveBeenCalledTimes(1)
	})
})
