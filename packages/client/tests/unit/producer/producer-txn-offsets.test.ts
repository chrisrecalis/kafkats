import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ConsumeContext } from '@/consumer/types.js'
import { Producer } from '@/producer/producer.js'
import type { GroupMetadata } from '@/producer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { CommitFailedError } from '@/client/errors.js'
import { createTransactionalMocks } from './_helpers.js'

const TOPIC = 'test-topic'

describe('Producer transactional offsets', () => {
	let mocks: ReturnType<typeof createTransactionalMocks>

	beforeEach(() => {
		mocks = createTransactionalMocks(TOPIC)
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	function makeProducer() {
		return new Producer(mocks.cluster, { lingerMs: 0, retries: 2, retryBackoffMs: 1, transactionalId: 'tx-1' })
	}

	function member(overrides: Partial<GroupMetadata> = {}): GroupMetadata {
		return { groupId: 'group-1', generationId: 3, memberId: 'member-1', groupInstanceId: null, ...overrides }
	}

	describe('TxnOffsetCommit deduplication', () => {
		it('commits the highest offset per partition, and the last one given for an equal offset', async () => {
			await makeProducer().transaction(async tx => {
				await tx.sendOffsets({
					consumerGroupMetadata: member(),
					offsets: [
						{ topic: TOPIC, partition: 0, offset: 5n, metadata: 'first' },
						{ topic: TOPIC, partition: 0, offset: 7n, metadata: 'second' },
						{ topic: TOPIC, partition: 1, offset: 3n, metadata: 'old' },
					],
				})
				await tx.sendOffsets({
					consumerGroupMetadata: member(),
					offsets: [
						{ topic: TOPIC, partition: 0, offset: 6n, metadata: 'lower' },
						{ topic: TOPIC, partition: 0, offset: 7n, metadata: 'latest' },
						{ topic: TOPIC, partition: 1, offset: 3n, metadata: 'new' },
					],
				})
			})

			const [request] = mocks.groupCoordinator.txnOffsetCommit.mock.calls[0]!
			expect(request.topics).toEqual([
				{
					name: TOPIC,
					partitions: [
						expect.objectContaining({
							partitionIndex: 0,
							committedOffset: 7n,
							committedMetadata: 'latest',
						}),
						expect.objectContaining({ partitionIndex: 1, committedOffset: 3n, committedMetadata: 'new' }),
					],
				},
			])
		})
	})

	describe('fencing check of sendOffsets', () => {
		it.each([
			['a negative generation', member({ generationId: -1, memberId: '' }), ErrorCode.IllegalGeneration],
			['an empty member id', member({ memberId: '' }), ErrorCode.UnknownMemberId],
		])('rejects group metadata with %s before sending anything', async (_, consumerGroupMetadata, errorCode) => {
			let error: unknown
			const aborted = await makeProducer()
				.transaction(async tx => {
					error = await tx
						.sendOffsets({ consumerGroupMetadata, offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
						.catch((e: unknown) => e)
				})
				.catch((e: unknown) => e)

			expect(error).toBeInstanceOf(CommitFailedError)
			// Like a failed send, it makes the transaction abort, also when the callback caught it.
			expect(aborted).toMatchObject({ outcome: 'aborted', cause: error })
			expect(error).toMatchObject({
				groupId: 'group-1',
				errorCode,
				cause: expect.objectContaining({ errorCode }),
			})
			expect(mocks.txnCoordinator.addOffsetsToTxn).not.toHaveBeenCalled()
			expect(mocks.groupCoordinator.txnOffsetCommit).not.toHaveBeenCalled()
		})

		it('rejects a context whose group metadata has no generation', async () => {
			const context: ConsumeContext = {
				signal: new AbortController().signal,
				topic: TOPIC,
				partition: 0,
				offset: 4n,
				groupId: 'group-1',
				consumerGroupMetadata: member({ generationId: -1 }),
			}

			const error = await makeProducer()
				.transaction(async tx => {
					await tx.sendOffsets(context)
				})
				.catch((e: unknown) => e)

			expect(error).toMatchObject({ outcome: 'aborted', cause: expect.any(CommitFailedError) })
			expect(mocks.txnCoordinator.addOffsetsToTxn).not.toHaveBeenCalled()
		})

		it('keeps the groupId form unfenced', async () => {
			await makeProducer().transaction(async tx => {
				await tx.sendOffsets({ groupId: 'group-1', offsets: [{ topic: TOPIC, partition: 0, offset: 1n }] })
			})

			expect(mocks.groupCoordinator.txnOffsetCommit).toHaveBeenCalledWith(
				expect.objectContaining({ groupId: 'group-1', generationId: -1, memberId: '' })
			)
		})
	})
})
