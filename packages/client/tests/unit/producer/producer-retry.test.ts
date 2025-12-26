import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { ConnectionClosedError, NetworkError } from '@/network/errors.js'
import { ProducerFencedError } from '@/client/errors.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'

/**
 * Create a mock Cluster for Producer testing
 */
function createMockCluster() {
	const mockBrokers = new Map<number, ReturnType<typeof createMockBroker>>()

	const mockCluster = {
		getLeaderForPartition: vi.fn(),
		refreshMetadata: vi.fn().mockResolvedValue({
			clusterId: 'test',
			controllerId: 1,
			brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
			topics: new Map([
				[
					'test-topic',
					{
						name: 'test-topic',
						partitions: new Map([[0, { partitionIndex: 0, leaderId: 1 }]]),
					},
				],
			]),
			updatedAt: Date.now(),
		}),
		getCoordinator: vi.fn(),
		getAnyBroker: vi.fn(),
		getLogger: vi.fn().mockReturnValue({
			child: () => ({
				debug: vi.fn(),
				info: vi.fn(),
				warn: vi.fn(),
				error: vi.fn(),
			}),
			debug: vi.fn(),
			info: vi.fn(),
			warn: vi.fn(),
			error: vi.fn(),
		}),
		getMockBroker: (nodeId: number) => mockBrokers.get(nodeId),
		setMockBroker: (nodeId: number, broker: ReturnType<typeof createMockBroker>) => {
			mockBrokers.set(nodeId, broker)
		},
	}

	return mockCluster as unknown as Cluster & {
		getLeaderForPartition: ReturnType<typeof vi.fn>
		refreshMetadata: ReturnType<typeof vi.fn>
		getCoordinator: ReturnType<typeof vi.fn>
		getAnyBroker: ReturnType<typeof vi.fn>
		getMockBroker: (nodeId: number) => ReturnType<typeof createMockBroker> | undefined
		setMockBroker: (nodeId: number, broker: ReturnType<typeof createMockBroker>) => void
	}
}

/**
 * Create a mock Broker for testing
 */
function createMockBroker(nodeId: number) {
	return {
		nodeId,
		host: `broker-${nodeId}.test`,
		port: 9092,
		isConnected: true,
		produce: vi.fn(),
		initProducerId: vi.fn(),
		getApiVersion: vi.fn().mockReturnValue(9),
	} as unknown as Broker & { produce: ReturnType<typeof vi.fn>; initProducerId: ReturnType<typeof vi.fn> }
}

/**
 * Build a ProduceResponse with specified error codes
 */
function buildProduceResponse(
	topics: Array<{
		name: string
		partitions: Array<{ partitionIndex: number; errorCode: ErrorCode; baseOffset?: bigint }>
	}>
): ProduceResponse {
	return {
		topics: topics.map(t => ({
			name: t.name,
			partitions: t.partitions.map(p => ({
				partitionIndex: p.partitionIndex,
				errorCode: p.errorCode,
				baseOffset: p.baseOffset ?? 0n,
				logAppendTimeMs: BigInt(Date.now()),
				logStartOffset: 0n,
				recordErrors: [],
				errorMessage: null,
			})),
		})),
		throttleTimeMs: 0,
	}
}

describe('Producer retry behavior', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.setMockBroker(1, mockBroker)
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

		it('handles INVALID_PRODUCER_EPOCH as fencing', async () => {
			mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
			mockBroker.initProducerId.mockResolvedValue({
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
