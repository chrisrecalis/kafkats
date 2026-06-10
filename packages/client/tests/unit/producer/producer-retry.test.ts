import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { ConnectionClosedError, NetworkError } from '@/network/errors.js'
import { ProducerFencedError } from '@/client/errors.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

describe('Producer retry behavior', () => {
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

	describe('retriable errors', () => {
		it('retries on NOT_LEADER_OR_FOLLOWER and succeeds', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount === 1) {
					return buildProduceResponse([
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, errorCode: ErrorCode.NotLeaderOrFollower }],
						},
					])
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 100n }],
					},
				])
			})

			const result = await producer.send('test-topic', { value: Buffer.from('test') })

			expect(callCount).toBe(2)
			expect(result.offset).toBe(100n)
			expect(mockCluster.refreshMetadata).toHaveBeenCalled()
		})

		it('re-routes NOT_LEADER_OR_FOLLOWER retries to the refreshed partition leader', async () => {
			const oldLeader = createMockBroker(1)
			const newLeader = createMockBroker(2)
			mockCluster.getLeaderForPartition
				.mockImplementationOnce(async () => oldLeader)
				.mockImplementationOnce(async () => newLeader)

			oldLeader.produce.mockImplementation(async () =>
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.NotLeaderOrFollower }],
					},
				])
			)
			newLeader.produce.mockImplementation(async () =>
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 101n }],
					},
				])
			)

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			const result = await producer.send('test-topic', { value: Buffer.from('test') })

			expect(result.offset).toBe(101n)
			expect(oldLeader.produce).toHaveBeenCalledTimes(1)
			expect(newLeader.produce).toHaveBeenCalledTimes(1)
			expect(mockCluster.refreshMetadata).toHaveBeenCalledWith(['test-topic'])
		})

		it('retries on REQUEST_TIMED_OUT', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount < 3) {
					return buildProduceResponse([
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, errorCode: ErrorCode.RequestTimedOut }],
						},
					])
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 200n }],
					},
				])
			})

			const result = await producer.send('test-topic', { value: Buffer.from('test') })

			expect(callCount).toBe(3)
			expect(result.offset).toBe(200n)
		})

		it('fails after max retries exceeded', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 2,
				retryBackoffMs: 1,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.NotLeaderOrFollower }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				'max retries exceeded'
			)
			expect(mockBroker.produce).toHaveBeenCalledTimes(3) // Initial + 2 retries
		})
	})

	describe('non-retriable errors', () => {
		it('fails immediately on MESSAGE_TOO_LARGE', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.MessageTooLarge }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow('MessageTooLarge')
			expect(mockBroker.produce).toHaveBeenCalledTimes(1)
		})

		it('fails immediately on TOPIC_AUTHORIZATION_FAILED', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.TopicAuthorizationFailed }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				'TopicAuthorizationFailed'
			)
			expect(mockBroker.produce).toHaveBeenCalledTimes(1)
		})

		it('fails immediately on INVALID_RECORD', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.InvalidRecord }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow('InvalidRecord')
			expect(mockBroker.produce).toHaveBeenCalledTimes(1)
		})
	})

	describe('connection errors', () => {
		it('retries on ConnectionClosedError', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount === 1) {
					throw new ConnectionClosedError('Connection lost')
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 400n }],
					},
				])
			})

			const result = await producer.send('test-topic', { value: Buffer.from('test') })

			expect(callCount).toBe(2)
			expect(result.offset).toBe(400n)
		})

		it('retries on NetworkError', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount === 1) {
					throw new NetworkError('ETIMEDOUT')
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 500n }],
					},
				])
			})

			const result = await producer.send('test-topic', { value: Buffer.from('test') })

			expect(callCount).toBe(2)
			expect(result.offset).toBe(500n)
		})

		it('fails after max retries on persistent connection errors', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 2,
				retryBackoffMs: 1,
			})

			mockBroker.produce.mockRejectedValue(new ConnectionClosedError('Connection lost'))

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				ConnectionClosedError
			)
			expect(mockBroker.produce).toHaveBeenCalledTimes(3)
		})
	})

	describe('idempotent producer - fencing errors', () => {
		it('handles PRODUCER_FENCED by failing with ProducerFencedError', async () => {
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
			mockBroker.initProducerId.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				producerId: 1000n,
				producerEpoch: 1,
			})

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				idempotent: true,
			})

			// First produce succeeds to initialize
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }],
					},
				])
			)

			// Initialize producer
			await producer.send('test-topic', { value: Buffer.from('init') })

			// Reset mock
			mockBroker.produce.mockReset()

			// Now simulate fencing on next produce
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.ProducerFenced }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				ProducerFencedError
			)
		})

		it('OUT_OF_ORDER_SEQUENCE_NUMBER fences the producer for re-init', async () => {
			let initCount = 0
			mockBroker.initProducerId.mockImplementation(async () => {
				initCount++
				return {
					throttleTimeMs: 0,
					errorCode: ErrorCode.None,
					producerId: 2000n + BigInt(initCount),
					producerEpoch: initCount,
				}
			})
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				idempotent: true,
			})

			// First send succeeds — initializes producer (initCount == 1).
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }],
					},
				])
			)
			await producer.send('test-topic', { value: Buffer.from('init') })
			expect(initCount).toBe(1)

			mockBroker.produce.mockReset()

			// Second send: broker returns OOO.
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.OutOfOrderSequenceNumber }],
					},
				])
			)
			await expect(producer.send('test-topic', { value: Buffer.from('msg') })).rejects.toThrow(
				/Out of order sequence/
			)

			// Third send: must trigger re-initialization (new producerId/epoch).
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 5n }],
					},
				])
			)
			await producer.send('test-topic', { value: Buffer.from('after-recovery') })
			expect(initCount).toBe(2)
		})

		it('handles INVALID_PRODUCER_EPOCH as fencing', async () => {
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
			mockBroker.initProducerId.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				producerId: 1001n,
				producerEpoch: 1,
			})

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				idempotent: true,
			})

			// First produce succeeds to initialize
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }],
					},
				])
			)

			await producer.send('test-topic', { value: Buffer.from('init') })

			mockBroker.produce.mockReset()

			// Simulate invalid epoch
			mockBroker.produce.mockResolvedValueOnce(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.InvalidProducerEpoch }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				ProducerFencedError
			)
		})
	})

	describe('idempotent producer - duplicate handling', () => {
		it('treats DUPLICATE_SEQUENCE_NUMBER as success', async () => {
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
			mockBroker.initProducerId.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				producerId: 1002n,
				producerEpoch: 1,
			})

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				idempotent: true,
			})

			// Return duplicate sequence error (means message was already committed)
			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [
							{
								partitionIndex: 0,
								errorCode: ErrorCode.DuplicateSequenceNumber,
								baseOffset: 600n,
							},
						],
					},
				])
			)

			// Should succeed - duplicate means it was already committed
			const result = await producer.send('test-topic', { value: Buffer.from('test') })
			expect(result.offset).toBe(600n)
		})
	})

	describe('idempotent producer - out of order sequence', () => {
		it('fails on OUT_OF_ORDER_SEQUENCE_NUMBER', async () => {
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
			mockBroker.initProducerId.mockResolvedValue({
				throttleTimeMs: 0,
				errorCode: ErrorCode.None,
				producerId: 1003n,
				producerEpoch: 1,
			})

			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				idempotent: true,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.OutOfOrderSequenceNumber }],
					},
				])
			)

			await expect(producer.send('test-topic', { value: Buffer.from('test') })).rejects.toThrow(
				'Out of order sequence'
			)
		})
	})

	describe('metadata refresh on errors', () => {
		it('refreshes metadata on NOT_LEADER_OR_FOLLOWER', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount === 1) {
					return buildProduceResponse([
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, errorCode: ErrorCode.NotLeaderOrFollower }],
						},
					])
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 700n }],
					},
				])
			})

			await producer.send('test-topic', { value: Buffer.from('test') })

			expect(mockCluster.refreshMetadata).toHaveBeenCalledWith(['test-topic'])
		})

		it('refreshes metadata on UNKNOWN_TOPIC_OR_PARTITION', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				retries: 3,
				retryBackoffMs: 1,
			})

			let callCount = 0
			mockBroker.produce.mockImplementation(async () => {
				callCount++
				if (callCount === 1) {
					return buildProduceResponse([
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, errorCode: ErrorCode.UnknownTopicOrPartition }],
						},
					])
				}
				return buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 800n }],
					},
				])
			})

			await producer.send('test-topic', { value: Buffer.from('test') })

			expect(mockCluster.refreshMetadata).toHaveBeenCalledWith(['test-topic'])
		})
	})

	describe('producer state', () => {
		it('throws error when sending after disconnect', async () => {
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
			})

			mockBroker.produce.mockResolvedValue(
				buildProduceResponse([
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }],
					},
				])
			)

			// Send first message successfully
			await producer.send('test-topic', { value: Buffer.from('test1') })

			// Disconnect
			await producer.disconnect()

			// Try to send after disconnect
			await expect(producer.send('test-topic', { value: Buffer.from('test2') })).rejects.toThrow(
				'Producer is not running'
			)
		})
	})
})
