import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer produce-response handling', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	describe('SendResult.timestamp (A4)', () => {
		it('falls back to the record timestamp when logAppendTimeMs is -1 (CreateTime topic)', async () => {
			const producer = new Producer(mockCluster, { lingerMs: 0, retries: 0 })

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: TOPIC,
						partitions: [
							{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n, logAppendTimeMs: -1n },
						],
					},
				])
			)

			const before = Date.now()
			const result = await producer.send(TOPIC, { value: Buffer.from('x') })
			const after = Date.now()

			expect(result.timestamp.getTime()).toBeGreaterThanOrEqual(before)
			expect(result.timestamp.getTime()).toBeLessThanOrEqual(after)
		})

		it('preserves an explicit message timestamp when logAppendTimeMs is -1', async () => {
			const producer = new Producer(mockCluster, { lingerMs: 0, retries: 0 })

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: TOPIC,
						partitions: [
							{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n, logAppendTimeMs: -1n },
						],
					},
				])
			)

			const custom = new Date('2024-01-15T10:30:00.000Z')
			const result = await producer.send(TOPIC, { value: Buffer.from('x'), timestamp: custom })

			expect(result.timestamp.getTime()).toBe(custom.getTime())
		})

		it('uses logAppendTimeMs when the broker returns one (LogAppendTime topic)', async () => {
			const producer = new Producer(mockCluster, { lingerMs: 0, retries: 0 })

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: TOPIC,
						partitions: [
							{
								partitionIndex: 0,
								errorCode: ErrorCode.None,
								baseOffset: 0n,
								logAppendTimeMs: 987654321n,
							},
						],
					},
				])
			)

			const result = await producer.send(TOPIC, { value: Buffer.from('x') })

			expect(result.timestamp.getTime()).toBe(987654321)
		})
	})

	describe('missing partition in produce response (A5)', () => {
		it('does not fence a non-idempotent producer when the response omits the partition', async () => {
			const producer = new Producer(mockCluster, { lingerMs: 0, retries: 1, retryBackoffMs: 1 })

			mockBroker.produce.mockResolvedValue(buildProduceResponse([{ name: TOPIC, partitions: [] }]))

			// Retriable failure: the retry budget is exhausted, no fencing.
			await expect(producer.send(TOPIC, { value: Buffer.from('x') })).rejects.toThrow('max retries exceeded')

			// A non-idempotent producer has no producer identity — it must never InitProducerId.
			expect(mockBroker.initProducerId).not.toHaveBeenCalled()
			expect(mockCluster.getAnyBroker).not.toHaveBeenCalled()

			// The producer must remain usable (not fenced) once the broker recovers.
			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 7n }] },
				])
			)
			const result = await producer.send(TOPIC, { value: Buffer.from('y') })
			expect(result.offset).toBe(7n)
		})

		it('still fences an idempotent producer when the response omits the partition', async () => {
			let initCount = 0
			mockBroker.initProducerId.mockImplementation(async () => {
				initCount++
				return { throttleTimeMs: 0, errorCode: ErrorCode.None, producerId: 42n, producerEpoch: initCount }
			})

			const producer = new Producer(mockCluster, { lingerMs: 0, retries: 1, retryBackoffMs: 1, idempotent: true })

			mockBroker.produce.mockResolvedValueOnce(buildProduceResponse([{ name: TOPIC, partitions: [] }]))

			await expect(producer.send(TOPIC, { value: Buffer.from('x') })).rejects.toThrow(/Ambiguous produce outcome/)

			// The outcome is ambiguous for the sequence state — fence + reinit.
			await vi.waitFor(() => expect(initCount).toBe(2))
		})
	})
})
