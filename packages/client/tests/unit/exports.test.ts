import { describe, expect, expectTypeOf, it } from 'vitest'

import * as root from '@/index.js'
import type {
	Admin,
	AssignedPartition,
	BeginTransactionOptions,
	CommitOffset,
	CommittedOffset,
	Consumer,
	ConsumerConfig,
	ConsumerEvents,
	ConsumerGroupMetadata,
	DeleteRecordsOptions,
	DeleteRecordsResult,
	DisconnectOptions,
	OffsetRequestOptions,
	OffsetResetEvent,
	OffsetResetPolicy,
	Producer,
	ProducerCallOptions,
	ProducerTransaction,
	RecordsToDelete,
	TopicPartition,
	TopicPartitionOffset,
	TransactionHandle,
	TransactionOptions,
	TransactionState,
} from '@/index.js'
import { murmur2, toPositive } from '@/producer/partitioners/murmur2.js'

// The package has only the "." export: what the root does not export cannot be used.
describe('package root exports', () => {
	it('exports the key hash of the default partitioner', () => {
		expect(root.murmur2).toBe(murmur2)
		expect(root.toPositive).toBe(toPositive)
	})

	it('types the consumer offset, assignment and reset APIs', () => {
		expectTypeOf<Consumer['commit']>().toEqualTypeOf<
			(offsets: readonly CommitOffset[], options?: OffsetRequestOptions) => Promise<void>
		>()
		expectTypeOf<Consumer['committed']>().toEqualTypeOf<
			(
				partitions: readonly TopicPartition[],
				options?: OffsetRequestOptions
			) => Promise<Map<string, CommittedOffset | null>>
		>()
		expectTypeOf<Consumer['groupMetadata']>().toEqualTypeOf<() => ConsumerGroupMetadata | undefined>()
		expectTypeOf<CommitOffset>().toExtend<TopicPartitionOffset>()
		expectTypeOf<ConsumerConfig['autoOffsetReset']>().toEqualTypeOf<OffsetResetPolicy | undefined>()
		expectTypeOf<ConsumerConfig['onPartitionsAssigned']>().toEqualTypeOf<
			((partitions: AssignedPartition[]) => Promise<void>) | undefined
		>()
		expectTypeOf<ConsumerEvents['partitionsAssigned']>().toEqualTypeOf<[AssignedPartition[]]>()
		expectTypeOf<ConsumerEvents['offsetReset']>().toEqualTypeOf<[OffsetResetEvent]>()
		expectTypeOf<ConsumerEvents['offsetOutOfRange']>().toEqualTypeOf<[TopicPartitionOffset]>()
		expectTypeOf<ConsumerEvents['rebalanceComplete']>().toEqualTypeOf<[]>()
	})

	it('types the producer transaction handle and bounded calls', () => {
		expectTypeOf<Producer['beginTransaction']>().toEqualTypeOf<
			(options?: BeginTransactionOptions) => Promise<TransactionHandle>
		>()
		expectTypeOf<Producer['transaction']>().parameter(1).toEqualTypeOf<TransactionOptions | undefined>()
		expectTypeOf<TransactionHandle>().toExtend<ProducerTransaction>()
		expectTypeOf<TransactionHandle['commit']>().toEqualTypeOf<(options?: ProducerCallOptions) => Promise<void>>()
		expectTypeOf<Producer['transactionState']>().toEqualTypeOf<TransactionState>()
		expectTypeOf<Producer['disconnect']>().toEqualTypeOf<(options?: DisconnectOptions) => Promise<void>>()
		expectTypeOf<Producer['partitionCount']>().toEqualTypeOf<
			(topic: string, options?: ProducerCallOptions) => Promise<number>
		>()
	})

	it('types admin.deleteRecords()', () => {
		expectTypeOf<Admin['deleteRecords']>().toEqualTypeOf<
			(records: readonly RecordsToDelete[], options?: DeleteRecordsOptions) => Promise<DeleteRecordsResult[]>
		>()
	})
})
