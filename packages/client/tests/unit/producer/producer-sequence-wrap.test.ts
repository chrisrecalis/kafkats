import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

const TOPIC = 'test-topic'
const MAX_SEQUENCE = 0x7fffffff

describe('Producer sequence-wrap split accounting (A7)', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)

		mockBroker.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 9n,
			producerEpoch: 0,
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

	it('keeps accumulator pending-batch accounting balanced across a split', async () => {
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 1,
			retryBackoffMs: 1,
			idempotent: true,
		})
		const internals = producer as unknown as {
			sequences: Map<string, number>
			accumulator: { pendingCount: number }
		}

		// Initialize the idempotent producer.
		await producer.send(TOPIC, { value: Buffer.from('init'), partition: 0 })

		// Force the next batch to straddle the sequence wrap boundary so it splits:
		// baseSequence = MAX-1, 3 records -> only 2 fit, 1-record remainder re-queued.
		internals.sequences.set(`${TOPIC}:0`, MAX_SEQUENCE - 1)

		await producer.send(TOPIC, [
			{ value: Buffer.from('a'), partition: 0 },
			{ value: Buffer.from('b'), partition: 0 },
			{ value: Buffer.from('c'), partition: 0 },
		])
		await producer.flush()

		// Let the trailing batchCompleted() calls settle before checking the counter.
		await new Promise(resolve => setTimeout(resolve, 10))

		// Two produce requests: the truncated batch and the remainder.
		expect(mockBroker.produce).toHaveBeenCalledTimes(3)
		expect(internals.accumulator.pendingCount).toBe(0)
	})
})
