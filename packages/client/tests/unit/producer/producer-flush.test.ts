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

	it('waits for deferred batches in pendingBatches to actually be sent', async () => {
		// Regression: flush() must await drains scheduled in queueMicrotask
		// (and any partition-level re-queue chains) before resolving.
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

		const queuedMessage = {
			topic: 'test-topic',
			partition: 0,
			key: null,
			value: Buffer.from('hello'),
			headers: {},
			timestamp: BigInt(Date.now()),
			resolve: vi.fn(),
			reject: vi.fn(),
		}
		;(producer as unknown as { pendingBatches: unknown[] }).pendingBatches.push({
			topic: 'test-topic',
			partition: 0,
			messages: [queuedMessage],
			sizeBytes: 5,
			createdAt: Date.now(),
		})
		;(producer as unknown as { scheduleDrain: () => void }).scheduleDrain()

		await producer.flush()

		expect(producePromiseResolved).toBe(true)
		expect(mockBroker.produce).toHaveBeenCalledTimes(1)
	})
})
