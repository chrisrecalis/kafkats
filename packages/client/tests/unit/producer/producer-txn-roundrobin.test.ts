import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Transactional producer with round-robin partitioner', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>
	let mockTxnCoordinator: ReturnType<typeof createMockBroker>

	beforeEach(() => {
		mockCluster = createMockCluster({ partitionCount: 4 })
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
					name: TOPIC,
					resultsByPartition: [0, 1, 2, 3].map(partitionIndex => ({
						partitionIndex,
						errorCode: ErrorCode.None,
					})),
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

		const writtenPartitions: number[] = []
		mockBroker.produce.mockImplementation(async (req: ProduceRequest) => {
			for (const t of req.topics) {
				for (const p of t.partitions) {
					writtenPartitions.push(p.partitionIndex)
				}
			}
			return buildProduceResponse([
				{
					name: TOPIC,
					partitions: req.topics[0]!.partitions.map(p => ({
						partitionIndex: p.partitionIndex,
						errorCode: ErrorCode.None,
						baseOffset: 100n,
					})),
				},
			])
		})

		await producer.transaction(async tx => {
			await tx.send(TOPIC, [{ value: Buffer.from('m1') }, { value: Buffer.from('m2') }])
		})

		expect(mockTxnCoordinator.addPartitionsToTxn).toHaveBeenCalled()
		const txnCall = mockTxnCoordinator.addPartitionsToTxn.mock.calls[0]![0]
		const registeredPartitions = txnCall.topics[0]!.partitions as number[]

		// Partitions registered with the coordinator must match the partitions
		// that received records, otherwise records land outside the transaction.
		registeredPartitions.sort((a, b) => a - b)
		const dedupedWritten = [...new Set(writtenPartitions)].sort((a, b) => a - b)
		expect(dedupedWritten).toEqual(registeredPartitions)
	})
})
