import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { Cluster } from '@/client/cluster.js'
import { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('Producer.commitTransactionOffsets retry', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let txnCoordinator: ReturnType<typeof createMockBroker>
	let groupCoordinator: ReturnType<typeof createMockBroker>

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
						{ name: 'test-topic', partitions: new Map([[0, { partitionIndex: 0, leaderId: 1 }]]) },
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
			addOffsetsToTxn: vi.fn().mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None }),
			endTxn: vi.fn(),
			txnOffsetCommit: vi.fn(),
			getApiVersion: vi.fn().mockReturnValue(9),
		} as unknown as Broker & {
			initProducerId: ReturnType<typeof vi.fn>
			addPartitionsToTxn: ReturnType<typeof vi.fn>
			addOffsetsToTxn: ReturnType<typeof vi.fn>
			endTxn: ReturnType<typeof vi.fn>
			txnOffsetCommit: ReturnType<typeof vi.fn>
		}
	}

	beforeEach(() => {
		mockCluster = createMockCluster()
		txnCoordinator = createMockBroker(1)
		groupCoordinator = createMockBroker(2)

		mockCluster.getCoordinator.mockImplementation(async (type: string) =>
			type === 'TRANSACTION' ? txnCoordinator : groupCoordinator
		)
		mockCluster.getAnyBroker.mockResolvedValue(txnCoordinator)

		txnCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		txnCoordinator.endTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('retries TxnOffsetCommit on RebalanceInProgress (retriable, non-coordinator)', async () => {
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			retries: 3,
			retryBackoffMs: 1,
			transactionalId: 'tx-1',
		})

		// First call: retriable error. Second call: success.
		let callCount = 0
		groupCoordinator.txnOffsetCommit.mockImplementation(async () => {
			callCount++
			if (callCount === 1) {
				return {
					throttleTimeMs: 0,
					topics: [
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, errorCode: ErrorCode.RebalanceInProgress }],
						},
					],
				}
			}
			return {
				throttleTimeMs: 0,
				topics: [
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }],
					},
				],
			}
		})

		await producer.transaction(async tx => {
			await tx.sendOffsets({
				groupId: 'consumer-group-1',
				offsets: [{ topic: 'test-topic', partition: 0, offset: 42n }],
			})
		})

		expect(callCount).toBe(2)
	})
})
