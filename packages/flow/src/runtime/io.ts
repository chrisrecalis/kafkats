/**
 * The ports the runtime talks to Kafka through. `kafka-io.ts` implements them over @kafkats/client, the
 * TestDriver over deterministic fakes. Every async operation takes `{ signal }`.
 */
import type { ConsumerGroupMetadata, Message, SendResult, TopicPartition } from '@kafkats/client'
import type { RecordHeaders } from '@/processor/record.js'
import type { InternalTopicSpec, PartitionCounts } from '@/topology/nodes.js'

/** A committed offset and its metadata (the task's commit metadata, 4.8.5). */
export interface CommittedOffset {
	readonly offset: bigint
	readonly metadata: string | null
}

/** An offset to commit: the next offset to read, with the task's commit metadata. */
export interface OffsetAndMetadata {
	readonly topic: string
	readonly partition: number
	readonly offset: bigint
	readonly metadata: string
}

export interface AssignedPartition extends TopicPartition {
	/** Start position resolved by the client (reset policy). */
	readonly offset: bigint
	readonly committed: CommittedOffset | null
}

export interface GroupConsumerHooks {
	/** The client invokes it synchronously from the loss event; the sync prefix runs before any other code. */
	onPartitionsLost(partitions: readonly TopicPartition[]): Promise<void>
	onPartitionsRevoked(partitions: readonly TopicPartition[]): Promise<void>
	onPartitionsAssigned(partitions: readonly AssignedPartition[]): Promise<void>
}

export interface GroupConsumerPort {
	/** Starts the group run; resolves when the run ends (stop or failure). The handler only enqueues. */
	run(
		topics: readonly string[],
		onBatch: (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => void,
		hooks: GroupConsumerHooks
	): Promise<void>
	stop(options: { signal: AbortSignal }): Promise<void>
	pause(tps: readonly TopicPartition[]): void
	resume(tps: readonly TopicPartition[]): void
	/** Drops buffered and pending batches of tp. */
	seek(tp: TopicPartition, offset: bigint): void
	/** Offset of the earliest undelivered record of tp, else the fetch position. */
	nextOffset(tp: TopicPartition): bigint | undefined
	currentLag(tp: TopicPartition): bigint | undefined
	commit(offsets: readonly OffsetAndMetadata[], options: { signal: AbortSignal }): Promise<void>
	committed(
		tps: readonly TopicPartition[],
		options: { signal: AbortSignal }
	): Promise<Map<string, CommittedOffset | null>>
	/** Undefined from a loss until the next join. */
	groupMetadata(): ConsumerGroupMetadata | undefined
	readonly rebalancePending: boolean
	on(event: 'rebalancePending' | 'rebalanceComplete', listener: () => void): void
	on(event: 'offsetOutOfRange', listener: (e: { topic: string; partition: number; offset: bigint }) => void): void
	on(
		event: 'offsetReset',
		listener: (e: {
			topic: string
			partition: number
			from: bigint | null
			to: bigint
			reason: 'no-commit' | 'out-of-range'
		}) => void
	): void
	on(event: 'error', listener: (e: Error) => void): void
}

export interface ProducedMessage {
	readonly key: Buffer | null
	readonly value: Buffer | null
	readonly partition?: number
	readonly timestamp?: Date
	readonly headers?: RecordHeaders
}

/** An open transaction (the client's transaction handle). */
export interface TransactionPort {
	send(topic: string, message: ProducedMessage): Promise<SendResult>
	sendOffsets(params: {
		consumerGroupMetadata: ConsumerGroupMetadata
		offsets: readonly OffsetAndMetadata[]
	}): Promise<void>
	commit(options: { signal: AbortSignal }): Promise<void>
	abort(options: { signal: AbortSignal }): Promise<void>
}

export interface ProducerPort {
	/** At-least-once sends. */
	send(topic: string, message: ProducedMessage): Promise<SendResult>
	/**
	 * Exactly-once: initializes the transactional producer now (KS `initTransactions`), which fences the producers with
	 * its transactional id and aborts the transaction the last of them left open.
	 */
	initTransactions(options: { signal: AbortSignal }): Promise<void>
	/** Exactly-once. */
	beginTransaction(options: { signal: AbortSignal }): Promise<TransactionPort>
	partitionCount(topic: string, options: { signal: AbortSignal }): Promise<number>
	close(options: { signal: AbortSignal }): Promise<void>
}

export interface AdminPort {
	describeTopics(topics: readonly string[], options: { signal: AbortSignal }): Promise<Map<string, number>>
	createTopics(
		specs: readonly InternalTopicSpec[],
		counts: PartitionCounts,
		options: { signal: AbortSignal }
	): Promise<void>
	listOffsets(
		tps: readonly TopicPartition[],
		spec: 'earliest' | 'latest',
		isolation: 'read_committed' | 'read_uncommitted',
		options: { signal: AbortSignal }
	): Promise<Map<string, bigint>>
	deleteRecords(
		offsets: readonly { topic: string; partition: number; offset: bigint }[],
		options: { signal: AbortSignal }
	): Promise<void>
}

/** Manual assignment, read_committed, reset policy 'none' with an out-of-range signal, no commits. */
export interface RestoreConsumerPort {
	assign(partitions: readonly { topic: string; partition: number; offset: bigint }[]): void
	run(
		onBatch: (tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]) => Promise<void>,
		onOutOfRange: (tp: TopicPartition, offset: bigint) => void
	): Promise<void>
	pause(tps: readonly TopicPartition[]): void
	position(tp: TopicPartition): bigint | undefined
	stop(options: { signal: AbortSignal }): Promise<void>
}

export type RestoreConsumerFactory = (groupId: string) => RestoreConsumerPort

export interface Clock {
	now(): number
	/** Calls `fn` once at `at` (clock time); returns a function that cancels the timer. */
	setTimer(at: number, fn: () => void): () => void
}

export interface RuntimeIO {
	/** A new consumer per Run. */
	readonly consumer: () => GroupConsumerPort
	/** A new producer per Run and per output reset under exactly-once. */
	readonly producer: () => ProducerPort
	readonly admin: AdminPort
	/** groupId `<applicationId>-restore-<taskId>-<n>` */
	readonly restoreConsumers: RestoreConsumerFactory
	/** groupId `<applicationId>-global-<store>-<processId>` */
	readonly globalConsumers: RestoreConsumerFactory
	readonly clock: Clock
}
