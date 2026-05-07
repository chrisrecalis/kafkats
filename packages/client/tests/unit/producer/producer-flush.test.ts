import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'

function createMockCluster() {
	const mockBrokers = new Map<number, ReturnType<typeof createMockBroker>>()
	const cluster = {
		getLeaderForPartition: vi.fn(),
		refreshMetadata: vi.fn().mockResolvedValue({
			clusterId: 'test',
			controllerId: 1,
			brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
			topics: new Map([
				[
					'test-topic',
					{ name: 'test-topic', partitions: new Map([[0, { partitionIndex: 0, leaderId: 1 }]]) },
				],
			]),
			updatedAt: Date.now(),
		}),
		getCoordinator: vi.fn(),
		getAnyBroker: vi.fn(),
		getLogger: vi.fn().mockReturnValue({
			child: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
			debug: vi.fn(),
			info: vi.fn(),
			warn: vi.fn(),
			error: vi.fn(),
		}),
		setMockBroker: (id: number, b: ReturnType<typeof createMockBroker>) => mockBrokers.set(id, b),
	}
	return cluster as unknown as Cluster & {
		getLeaderForPartition: ReturnType<typeof vi.fn>
		refreshMetadata: ReturnType<typeof vi.fn>
		setMockBroker: (id: number, b: ReturnType<typeof createMockBroker>) => void
	}
}

function createMockBroker(nodeId: number) {
	return {
		nodeId,
		host: `broker-${nodeId}.test`,
		port: 9092,
		isConnected: true,
		produce: vi.fn(),
		getApiVersion: vi.fn().mockReturnValue(9),
	} as unknown as Broker & { produce: ReturnType<typeof vi.fn> }
}

function buildProduceResponse(opts: {
	topic: string
	partition: number
	errorCode: ErrorCode
	baseOffset?: bigint
}): ProduceResponse {
	return {
		topics: [
			{
				name: opts.topic,
				partitions: [
					{
						partitionIndex: opts.partition,
						errorCode: opts.errorCode,
						baseOffset: opts.baseOffset ?? 0n,
						logAppendTimeMs: BigInt(Date.now()),
						logStartOffset: 0n,
						recordErrors: [],
						errorMessage: null,
					},
				],
			},
		],
		throttleTimeMs: 0,
	}
}

describe('Producer.flush()', () => {
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

	it('waits for deferred batches in pendingBatches to actually be sent', async () => {
		// The bug: producer.flush() snapshots `inflight` synchronously after
		// accumulator.flush() emits 'batchReady'. The handler pushes to
		// pendingBatches and schedules a drain microtask. If pendingBatches
		// already contains deferred batches (e.g. multi-batch-per-partition
		// re-queue path), Promise.all([...inflight]) misses them — the drain
		// runs after flush() has already resolved.
		//
		// Reproduce the deferred-batch state directly: push a batch into
		// pendingBatches, schedule a drain, then call flush(). Without the
		// fix, flush returns before the drain runs and the broker.produce
		// call is never observed.
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 0,
		})

		let producePromiseResolved = false
		mockBroker.produce.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 10))
			producePromiseResolved = true
			return buildProduceResponse({
				topic: 'test-topic',
				partition: 0,
				errorCode: ErrorCode.None,
				baseOffset: 5n,
			})
		})

		// Hand-craft a pending batch as if drainPendingBatches had re-queued it.
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
		// Schedule a drain (mimicking what scheduleDrain does after a re-queue).
		;(producer as unknown as { scheduleDrain: () => void }).scheduleDrain()

		// flush() must wait for the deferred drain to run and the resulting
		// broker.produce promise to complete.
		await producer.flush()

		expect(producePromiseResolved).toBe(true)
		expect(mockBroker.produce).toHaveBeenCalledTimes(1)
	})
})
