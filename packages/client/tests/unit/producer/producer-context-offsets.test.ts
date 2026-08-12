import { beforeEach, describe, expect, it } from 'vitest'

import type { ConsumeContext } from '@/consumer/types.js'
import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createMockBroker, createMockCluster, type MockBroker } from './_helpers.js'

describe('ProducerTransaction context-bound offsets', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let transactionCoordinator: MockBroker
	let groupCoordinator: MockBroker

	beforeEach(() => {
		mockCluster = createMockCluster()
		transactionCoordinator = createMockBroker(1)
		groupCoordinator = createMockBroker(2)

		mockCluster.getCoordinator.mockImplementation(async type =>
			type === 'TRANSACTION' ? transactionCoordinator : groupCoordinator
		)
		mockCluster.getAnyBroker.mockResolvedValue(transactionCoordinator)

		transactionCoordinator.initProducerId.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 1n,
			producerEpoch: 0,
		})
		transactionCoordinator.addOffsetsToTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
		transactionCoordinator.endTxn.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
		})
		groupCoordinator.txnOffsetCommit.mockResolvedValue({
			throttleTimeMs: 0,
			topics: [
				{
					name: 'input',
					partitions: [{ partitionIndex: 2, errorCode: ErrorCode.None }],
				},
			],
		})
	})

	function makeProducer(): Producer {
		return new Producer(mockCluster, {
			lingerMs: 0,
			retries: 1,
			transactionalId: 'tx-1',
		})
	}

	function groupContext(): ConsumeContext {
		return {
			signal: new AbortController().signal,
			topic: 'input',
			partition: 2,
			offset: 41n,
			groupId: 'group-1',
			consumerGroupMetadata: {
				groupId: 'group-1',
				generationId: 7,
				memberId: 'member-1',
				groupInstanceId: 'instance-1',
			},
		}
	}

	it('derives the next offset and commits it with the delivery-time group membership', async () => {
		const context = groupContext()

		await makeProducer().transaction(async tx => {
			await tx.sendOffsets(context)
		})

		expect(transactionCoordinator.addOffsetsToTxn).toHaveBeenCalledWith(
			expect.objectContaining({ groupId: 'group-1' })
		)
		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledWith(
			expect.objectContaining({
				groupId: 'group-1',
				generationId: 7,
				memberId: 'member-1',
				groupInstanceId: 'instance-1',
				topics: [
					{
						name: 'input',
						partitions: [expect.objectContaining({ partitionIndex: 2, committedOffset: 42n })],
					},
				],
			})
		)
	})

	it('uses explicit accumulated offsets with the context membership', async () => {
		const context = groupContext()

		await makeProducer().transaction(async tx => {
			await tx.sendOffsets(context, [{ topic: 'input', partition: 2, offset: 100n }])
		})

		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledWith(
			expect.objectContaining({
				generationId: 7,
				topics: [
					{
						name: 'input',
						partitions: [expect.objectContaining({ partitionIndex: 2, committedOffset: 100n })],
					},
				],
			})
		)
	})

	it('uses the unfenced group form for a manual-assignment context', async () => {
		const context: ConsumeContext = {
			signal: new AbortController().signal,
			topic: 'input',
			partition: 2,
			offset: 41n,
			groupId: 'manual-group',
		}

		await makeProducer().transaction(async tx => {
			await tx.sendOffsets(context)
		})

		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledWith(
			expect.objectContaining({
				groupId: 'manual-group',
				generationId: -1,
				memberId: '',
				groupInstanceId: null,
			})
		)
	})

	it('preserves the group membership when a context is spread', async () => {
		const copiedContext = { ...groupContext(), offset: 99n }

		await makeProducer().transaction(async tx => {
			await tx.sendOffsets(copiedContext)
		})

		expect(groupCoordinator.txnOffsetCommit).toHaveBeenCalledWith(
			expect.objectContaining({
				generationId: 7,
				memberId: 'member-1',
				topics: [
					{
						name: 'input',
						partitions: [expect.objectContaining({ partitionIndex: 2, committedOffset: 100n })],
					},
				],
			})
		)
	})
})
