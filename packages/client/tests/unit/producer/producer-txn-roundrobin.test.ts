import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'

describe('Transactional producer with round-robin partitioner', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>
	let mockTxnCoordinator: ReturnType<typeof createMockBroker>

	function createMockCluster() {
		const cluster = {
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
							partitions: new Map([
								[0, { partitionIndex: 0, leaderId: 1 }],
								[1, { partitionIndex: 1, leaderId: 1 }],
								[2, { partitionIndex: 2, leaderId: 1 }],
								[3, { partitionIndex: 3, leaderId: 1 }],
							]),
						},
					],
				]),
				updatedAt: Date.now(),
			}),
			getCoordinator: vi.fn(),
			invalidateCoordinator: vi.fn(),
			getAnyBroker: vi.fn(),
			getLogger: vi.fn().mockReturnValue({
				child: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
				debug: vi.fn(),
				info: vi.fn(),
				warn: vi.fn(),
				error: vi.fn(),
			}),
		}
		return cluster as unknown as Cluster & {
			getLeaderForPartition: ReturnType<typeof vi.fn>
			refreshMetadata: ReturnType<typeof vi.fn>
			getCoordinator: ReturnType<typeof vi.fn>
			invalidateCoordinator: ReturnType<typeof vi.fn>
			getAnyBroker: ReturnType<typeof vi.fn>
		}
	}

	function createMockBroker(nodeId: number) {
		return {
			nodeId,
			host: `broker-${nodeId}.test`,
			port: 9092,
			isConnected: true,
			produce: vi.fn(),
			initProducerId: vi.fn(),
			addPartitionsToTxn: vi.fn(),
			endTxn: vi.fn(),
			getApiVersion: vi.fn().mockReturnValue(9),
		} as unknown as Broker & {
			produce: ReturnType<typeof vi.fn>
			initProducerId: ReturnType<typeof vi.fn>
			addPartitionsToTxn: ReturnType<typeof vi.fn>
			endTxn: ReturnType<typeof vi.fn>
		}
	}

	function buildProduceResponse(partitions: number[]): ProduceResponse {
		return {
			topics: [
				{
					name: 'test-topic',
					partitions: partitions.map(partitionIndex => ({
						partitionIndex,
						errorCode: ErrorCode.None,
						baseOffset: 100n,
						logAppendTimeMs: BigInt(Date.now()),
						logStartOffset: 0n,
						recordErrors: [],
						errorMessage: null,
					})),
				},
			],
			throttleTimeMs: 0,
		}
	}

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockTxnCoordinator = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		mockCluster.getCoordinator.mockResolvedValue(mockTxnCoordinator)
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)

		mockTxnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		mockTxnCoordinator.addPartitionsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			results: [
				{
					name: 'test-topic',
					resultsByPartition: [
						{ partitionIndex: 0, errorCode: ErrorCode.None },
						{ partitionIndex: 1, errorCode: ErrorCode.None },
						{ partitionIndex: 2, errorCode: ErrorCode.None },
						{ partitionIndex: 3, errorCode: ErrorCode.None },
					],
				},
			],
		})
		mockTxnCoordinator.endTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('registers the same partitions with the txn coordinator that the actual produce uses', async () => {
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 1,
			transactionalId: 'tx-1',
			partitioner: 'round-robin',
		})

		// Capture which partitions actually receive records.
		const writtenPartitions: number[] = []
		mockBroker.produce.mockImplementation(async (req: { topics: Array<{ partitions: Array<{ partitionIndex: number }> }> }) => {
			for (const t of req.topics) {
				for (const p of t.partitions) {
					writtenPartitions.push(p.partitionIndex)
				}
			}
			return buildProduceResponse(req.topics[0]!.partitions.map(p => p.partitionIndex))
		})

		await producer.transaction(async tx => {
			await tx.send('test-topic', [
				{ value: Buffer.from('m1') },
				{ value: Buffer.from('m2') },
			])
		})

		// addPartitionsToTxn was called with a set of partitions.
		expect(mockTxnCoordinator.addPartitionsToTxn).toHaveBeenCalled()
		const txnCall = mockTxnCoordinator.addPartitionsToTxn.mock.calls[0]![0]
		const registeredPartitions = txnCall.topics[0]!.partitions as number[]

		// The partitions registered with the txn coordinator must MATCH the
		// partitions that actually received records. Otherwise records land
		// outside the transaction.
		registeredPartitions.sort((a, b) => a - b)
		const dedupedWritten = [...new Set(writtenPartitions)].sort((a, b) => a - b)
		expect(dedupedWritten).toEqual(registeredPartitions)
	})
})
