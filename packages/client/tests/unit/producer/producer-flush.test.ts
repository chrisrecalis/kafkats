import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

describe('Producer.flush()', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('waits for an in-flight fire-and-forget send to actually be sent', async () => {
		// Regression: flush() must wait for sends that have not been awaited — including
		// across the async hops and drain microtasks before the batch reaches the broker.
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 0,
		})

		let producePromiseResolved = false
		mockBroker.produce.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 10))
			producePromiseResolved = true
			return buildProduceResponse([
				{
					name: 'test-topic',
					partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 5n }],
				},
			])
		})

		// Fire-and-forget: do not await the send. flush() must still wait for it.
		const sent = producer.send('test-topic', { value: Buffer.from('hello') })
		void sent.catch(() => {})

		await producer.flush()

		expect(producePromiseResolved).toBe(true)
		expect(mockBroker.produce).toHaveBeenCalledTimes(1)

		await sent
	})
})
