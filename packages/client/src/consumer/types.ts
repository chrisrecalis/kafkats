/**
 * Consumer types and interfaces
 */

import type { Cluster } from '@/client/cluster.js'
import type { TopicDefinition, DecoderLike } from '@/topic.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import type { PriorityStrategy } from './priority.js'
export { normalizeDecoder } from '@/topic.js'
export type { Decoder, DecoderLike } from '@/topic.js'

// ==================== Subscription Types ====================

/**
 * Topic subscription configuration
 */
export interface TopicSubscription<V = Buffer, K = Buffer, N extends string = string> {
	topic: N
	decoder: DecoderLike<V>
	keyDecoder?: DecoderLike<K>
}

/**
 * A subscription-like type that can be either TopicSubscription or TopicDefinition
 */
export type SubscriptionLike<V, K, N extends string = string> = TopicSubscription<V, K, N> | TopicDefinition<V, K, N>

/**
 * Single subscription or tuple of subscriptions
 * Accepts topic name strings, TopicSubscription, TopicDefinition, or arrays of any of them
 * Uses `any` to avoid variance issues with Codec<T> contravariance
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type SubscriptionInput = string | SubscriptionLike<any, any> | readonly (string | SubscriptionLike<any, any>)[]

/**
 * Extract the message value type from a subscription
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export type MsgOf<S> = S extends string
	? Buffer
	: S extends TopicSubscription<infer V, any>
		? V
		: S extends TopicDefinition<infer V, any>
			? V
			: S extends readonly unknown[]
				? MsgOf<S[number]>
				: never

/**
 * Extract the message key type from a subscription
 */
export type KeyOf<S> = S extends string
	? Buffer
	: S extends TopicSubscription<any, infer K>
		? K
		: S extends TopicDefinition<any, infer K>
			? K
			: S extends readonly unknown[]
				? KeyOf<S[number]>
				: never

/** Extract the topic-name type from a subscription. */
export type TopicOf<S> = S extends string
	? S
	: S extends TopicSubscription<any, any, infer N>
		? N
		: S extends TopicDefinition<any, any, infer N>
			? N
			: S extends readonly unknown[]
				? TopicOf<S[number]>
				: never
/* eslint-enable @typescript-eslint/no-explicit-any */

/**
 * Check if a subscription is a TopicDefinition (not a TopicSubscription)
 * TopicDefinition has key/value codecs, TopicSubscription has decoder
 */
export function isTopicDefinition<V, K, N extends string>(
	sub: SubscriptionLike<V, K, N>
): sub is TopicDefinition<V, K, N> {
	// TopicSubscription has 'decoder', TopicDefinition has key/value codecs
	return !('decoder' in sub)
}

/**
 * Convert SubscriptionLike to TopicSubscription
 * TopicDefinition uses its value codec as the decoder (defaults to Buffer)
 */
export function toTopicSubscription(sub: string): TopicSubscription<Buffer, Buffer>
export function toTopicSubscription<V, K, N extends string>(sub: SubscriptionLike<V, K, N>): TopicSubscription<V, K, N>
export function toTopicSubscription<V, K, N extends string>(
	sub: SubscriptionLike<V, K, N> | string
): TopicSubscription<V | Buffer, K | Buffer, N | string> {
	if (typeof sub === 'string') {
		return {
			topic: sub,
			decoder: (b: Buffer) => b,
			keyDecoder: (b: Buffer) => b,
		}
	}

	if (isTopicDefinition(sub)) {
		const keyDecoder = sub.key ? (b: Buffer) => sub.key!.decode(b) : (b: Buffer) => b
		if (sub.value) {
			return {
				topic: sub.topic,
				decoder: (b: Buffer) => sub.value!.decode(b),
				keyDecoder,
			}
		}
		return {
			topic: sub.topic,
			decoder: (b: Buffer) => b,
			keyDecoder,
		}
	}
	return sub
}

// ==================== Message Types ====================

/**
 * A consumed message with decoded value
 */
export interface Message<V = Buffer, K = Buffer> {
	topic: string
	partition: number
	offset: bigint
	timestamp: bigint
	key: K | null
	value: V
	headers: Record<string, Buffer>
}

/**
 * Consumer-group membership snapshot captured when a record is delivered.
 */
export interface ConsumerGroupMetadata {
	readonly groupId: string
	readonly generationId: number
	readonly memberId: string
	readonly groupInstanceId?: string | null
}

/**
 * Context provided to message handlers.
 *
 * Pass this object to a transactional producer's `sendOffsets()` method
 * to commit offsets under the consumer-group membership that delivered them.
 */
export interface ConsumeContext {
	signal: AbortSignal
	topic: string
	partition: number
	offset: bigint
	groupId: string
	/** Present for consumer-group delivery. Captured when the record or batch is delivered. */
	consumerGroupMetadata?: Readonly<ConsumerGroupMetadata>
}

// ==================== Handler Types ====================

/**
 * Handler for single message mode
 */
export type MessageHandler<V = Buffer, K = Buffer> = (message: Message<V, K>, ctx: ConsumeContext) => Promise<void>

/**
 * Handler for batch message mode
 */
export type BatchHandler<V = Buffer, K = Buffer> = (batch: Message<V, K>[], ctx: BatchContext) => Promise<void>

/** Context of a batch handler. `offset` is the offset of the last record of the batch. */
export interface BatchContext extends ConsumeContext {
	/**
	 * Mark the records of this batch up to and including `offset` as processed, so their offsets
	 * can be committed before the handler returns. A handler that works through a large batch
	 * calls this as it goes; otherwise nothing of the batch is committed until it has finished.
	 *
	 * A handler that returns normally has its whole batch marked, unless the run was stopped meanwhile
	 * (`signal` aborted): it may have returned early, so only what it resolved itself is marked.
	 */
	resolveOffset(offset: bigint): void
}

// ==================== Run Options ====================

/**
 * Options for runEach() - single message processing
 */
export interface RunEachOptions<T extends string = string> {
	partitionConcurrency?: number
	autoCommit?: boolean
	commitOffsets?: boolean
	autoCommitIntervalMs?: number
	signal?: AbortSignal
	/**
	 * Manually assign the consumer to these partitions instead of joining a group.
	 *
	 * - Skips JoinGroup/SyncGroup and does not emit rebalance events.
	 * - Offsets (if commitOffsets is enabled) are still committed under `groupId`,
	 *   but without group generation/member metadata.
	 */
	assignment?: ManualAssignment[]
	/** Controls topic ordering for both buffered delivery and background fetches. */
	priority?: PriorityStrategy<T>
}

/**
 * Options for runBatch() - batch message processing
 */
export interface RunBatchOptions<T extends string = string> {
	partitionConcurrency?: number
	autoCommit?: boolean
	commitOffsets?: boolean
	autoCommitIntervalMs?: number
	signal?: AbortSignal
	/**
	 * Manually assign the consumer to these partitions instead of joining a group.
	 *
	 * - Skips JoinGroup/SyncGroup and does not emit rebalance events.
	 * - Offsets (if commitOffsets is enabled) are still committed under `groupId`,
	 *   but without group generation/member metadata.
	 */
	assignment?: ManualAssignment[]
	/** Controls topic ordering for both buffered delivery and background fetches. */
	priority?: PriorityStrategy<T>
}

/**
 * Options for stream() - async iterator mode
 */
export interface StreamOptions<T extends string = string> {
	commitOffsets?: boolean
	autoCommitIntervalMs?: number
	signal?: AbortSignal
	/**
	 * Manually assign the consumer to these partitions instead of joining a group.
	 *
	 * - Skips JoinGroup/SyncGroup and does not emit rebalance events.
	 * - Offsets (if commitOffsets is enabled) are still committed under `groupId`,
	 *   but without group generation/member metadata.
	 */
	assignment?: ManualAssignment[]
	/** Controls topic ordering for both buffered delivery and background fetches. */
	priority?: PriorityStrategy<T>
}

// ==================== Consumer Configuration ====================

/**
 * Auto offset reset strategy
 * Defines behavior when there is no committed offset or offset is out of range
 */
export type AutoOffsetReset = 'earliest' | 'latest' | 'none'

/** Where partitions without a valid position restart: one policy, or a function giving each partition's. */
export type OffsetResetPolicy = AutoOffsetReset | ((topic: string, partition: number) => AutoOffsetReset)

/**
 * Isolation level for fetch requests
 * - 'read_uncommitted': Read all messages including uncommitted transactional messages
 * - 'read_committed': Only read committed transactional messages (default)
 */
export type IsolationLevel = 'read_uncommitted' | 'read_committed'

/**
 * Partition assignment strategy
 * Determines which assignors are proposed during JoinGroup
 */
export type PartitionAssignmentStrategy = 'cooperative-sticky' | 'sticky' | 'range'

/**
 * Consumer configuration
 */
export interface ConsumerConfig {
	groupId: string
	groupInstanceId?: string
	sessionTimeoutMs?: number
	rebalanceTimeoutMs?: number
	heartbeatIntervalMs?: number
	/**
	 * Maximum bytes fetched per partition in one Fetch request, and the maximum (estimated) bytes
	 * delivered in one partition batch. Prefetched responses for a partition are coalesced up to this
	 * size; the rest stays buffered for the next batch. A batch always holds at least one record, so
	 * a record larger than this is still delivered (default: 1MB).
	 */
	maxBytesPerPartition?: number
	minBytes?: number
	maxWaitMs?: number
	/**
	 * Where a partition starts when it has no committed offset, and where it goes on when its position is out of
	 * range (the records were deleted). A policy, or a function returning each partition's policy:
	 * - 'earliest': the earliest available offset;
	 * - 'latest' (default): the end of the partition, the last stable offset under 'read_committed';
	 * - 'none': without a committed offset the run fails with `NoOffsetForPartitionError`; out of range, the
	 *   partition is paused and `offsetOutOfRange` is emitted: `seek()` it, then `resume()` it. Without an
	 *   `offsetOutOfRange` listener the run fails with `OffsetOutOfRangeError` instead.
	 *
	 * 'earliest' and 'latest' emit `offsetReset`.
	 */
	autoOffsetReset?: OffsetResetPolicy
	/**
	 * Controls which messages are visible to the consumer:
	 * - 'read_uncommitted': Read all messages including uncommitted transactional messages
	 * - 'read_committed': Only read committed transactional messages (default)
	 */
	isolationLevel?: IsolationLevel
	/**
	 * Automatically check the CRC32C of consumed record batches. Guards against on-the-wire or
	 * on-disk corruption; the check adds a per-batch pass over the payload, so it may be disabled
	 * when seeking extreme performance. Matches the Java consumer's `check.crcs` (default true).
	 */
	checkCrcs?: boolean
	/**
	 * Partition assignment strategy preference.
	 * - 'cooperative-sticky': Incremental rebalance (Kafka 2.4+, default)
	 * - 'sticky': Minimize movement, eager rebalance
	 * - 'range': Simple per-topic assignment
	 *
	 * When set to 'cooperative-sticky' (default), proposes multiple protocols
	 * in preference order for compatibility: cooperative-sticky → sticky → range.
	 * The broker selects the first mutually-supported protocol.
	 *
	 * Set to 'range' or 'sticky' for pre-2.4 brokers that don't support cooperative.
	 */
	partitionAssignmentStrategy?: PartitionAssignmentStrategy
	/**
	 * Optional trace hook for lightweight benchmarking instrumentation.
	 * Intended for diagnostics only and may be omitted in production usage.
	 */
	trace?: ConsumerTraceFn
	/**
	 * Awaited at the start of every rebalance, before any partition is revoked.
	 * Use for transactional commits (e.g. EOS pipelines) that must complete
	 * while the consumer still owns its in-flight assignment. The rebalance
	 * protocol blocks on this promise; throwing rejects the rebalance.
	 */
	onBeforeRebalance?: () => Promise<void>
	/**
	 * Awaited when partitions are taken away by a rebalance, after their handlers returned and before the
	 * consumer commits their offsets (if it commits offsets) and stops fetching them. Their offsets can still be
	 * committed here (`commit()`), under the generation that owned them. Under the eager protocol it runs for
	 * every partition before the member rejoins; under the cooperative protocol, for the revoked ones after it.
	 * Not called for partitions reported to `onPartitionsLost`. Throwing fails the rebalance and the run.
	 */
	onPartitionsRevoked?: (partitions: TopicPartition[]) => Promise<void>
	/**
	 * Awaited when partitions are assigned (on the first join, and by each rebalance: every partition under the
	 * eager protocol, the added ones under the cooperative protocol), before any of them is fetched: `pause()` and
	 * `seek()` here take effect before their first record. Partitions paused here stay paused. Throwing fails the
	 * assignment and the run.
	 */
	onPartitionsAssigned?: (partitions: AssignedPartition[]) => Promise<void>
	/**
	 * Called when this member lost its partitions with its session (the group expired it or fenced it): their
	 * offsets can no longer be committed. It is invoked synchronously as the loss is detected, so its synchronous
	 * part runs before any other code (handlers of these partitions may still be running); the member rejoins
	 * only once it resolved. Throwing fails the rejoin and the run.
	 */
	onPartitionsLost?: (partitions: TopicPartition[]) => Promise<void>
	/**
	 * Maximum time in ms to retry coordinator-related offset fetch and commit operations before
	 * failing (default: 60000). The fetch of a rebalance's committed offsets is tried again after
	 * that time, like while the cluster is unreachable, instead of failing the rebalance.
	 */
	defaultApiTimeoutMs?: number
}

/**
 * Internal resolved consumer configuration
 * All fields are required except groupInstanceId which remains optional
 */
export interface ResolvedConsumerConfig {
	groupId: string
	groupInstanceId?: string
	sessionTimeoutMs: number
	rebalanceTimeoutMs: number
	heartbeatIntervalMs: number
	maxBytesPerPartition: number
	minBytes: number
	maxWaitMs: number
	autoOffsetReset: OffsetResetPolicy
	isolationLevel: IsolationLevel
	checkCrcs: boolean
	partitionAssignmentStrategy: PartitionAssignmentStrategy
	defaultApiTimeoutMs: number
	onBeforeRebalance?: () => Promise<void>
	onPartitionsRevoked?: (partitions: TopicPartition[]) => Promise<void>
	onPartitionsAssigned?: (partitions: AssignedPartition[]) => Promise<void>
	onPartitionsLost?: (partitions: TopicPartition[]) => Promise<void>
}

export type ConsumerTraceStage = 'fetch_request' | 'decode_records' | 'handler'

export interface ConsumerTraceEvent {
	stage: ConsumerTraceStage
	durationMs: number
	recordCount?: number
	bytes?: number
	brokerId?: number
	topic?: string
	partition?: number
}

export type ConsumerTraceFn = (event: ConsumerTraceEvent) => void

/**
 * Default consumer configuration values
 */
export const DEFAULT_CONSUMER_CONFIG = {
	sessionTimeoutMs: 30000,
	rebalanceTimeoutMs: 60000,
	heartbeatIntervalMs: 3000,
	maxBytesPerPartition: 1048576, // 1MB
	minBytes: 1,
	maxWaitMs: 5000,
	autoOffsetReset: 'latest' as AutoOffsetReset,
	isolationLevel: 'read_committed' as IsolationLevel,
	checkCrcs: true,
	partitionAssignmentStrategy: 'cooperative-sticky' as PartitionAssignmentStrategy,
	defaultApiTimeoutMs: 60000,
} as const

/**
 * Default options for runEach()
 */
export const DEFAULT_RUN_EACH_OPTIONS = {
	partitionConcurrency: 1,
	autoCommit: true,
	commitOffsets: true,
	autoCommitIntervalMs: 5000,
} as const

/**
 * Default options for runBatch()
 */
export const DEFAULT_RUN_BATCH_OPTIONS = {
	partitionConcurrency: 1,
	autoCommit: true,
	commitOffsets: true,
	autoCommitIntervalMs: 5000,
} as const

// ==================== Internal Types ====================

export interface TopicPartition {
	topic: string
	partition: number
}

export interface TopicPartitionOffset extends TopicPartition {
	offset: bigint
}

/**
 * An offset to commit for a partition: `offset` is the next offset to read (the last processed offset + 1).
 */
export interface CommitOffset extends TopicPartitionOffset {
	/** Stored with the offset and read back with it (`null` or absent: none). */
	metadata?: string | null
	/** Leader epoch of the last processed record, if known. */
	leaderEpoch?: number
}

/**
 * An offset committed for a partition, as the group coordinator returns it.
 */
export interface CommittedOffset {
	/** The next offset to read. */
	offset: bigint
	/** The metadata committed with the offset; `null` when none was. */
	metadata: string | null
	/** The leader epoch committed with the offset, when the committer gave one. */
	leaderEpoch?: number
}

/** A partition assigned to this consumer, with where it starts. */
export interface AssignedPartition extends TopicPartition {
	/** The offset its first fetch starts at: the committed offset, or where the reset policy put it. */
	offset: bigint
	/** Its committed offset, `null` when it has none. */
	committed: CommittedOffset | null
}

/** A partition's position moved by its offset reset policy (see `ConsumerConfig.autoOffsetReset`). */
export interface OffsetResetEvent extends TopicPartition {
	/** The position that was out of range; `null` when the partition had no committed offset. */
	from: bigint | null
	/** The position it restarts at. */
	to: bigint
	reason: 'no-commit' | 'out-of-range'
}

/**
 * A batch of records from a single partition
 */
export interface PartitionBatch extends TopicPartition {
	records: DecodedRecord[]
	/**
	 * FetchManager assignment epoch captured when the batch was buffered. Used to detect
	 * stale batches drained before a rebalance removed (and possibly re-added) the
	 * partition, or before a seek or pause of it — such batches must not be delivered.
	 * See FetchManager.isBatchAssigned.
	 */
	assignmentEpoch?: number
}

export interface ManualAssignment extends TopicPartition {
	/**
	 * Optional starting offset for this partition.
	 * If omitted, the consumer resolves the start offset from committed offsets (if present)
	 * or falls back to `autoOffsetReset`.
	 */
	offset?: bigint
}

export enum ConsumerGroupState {
	UNJOINED = 'UNJOINED',
	JOINING = 'JOINING',
	AWAITING_SYNC = 'AWAITING_SYNC',
	STABLE = 'STABLE',
	LEAVING = 'LEAVING',
}

/** Where a consumer stands at one moment (see `Consumer.metrics()`). */
export interface ConsumerMetrics {
	partitions: ConsumerPartitionMetrics[]
}

export interface ConsumerPartitionMetrics {
	topic: string
	partition: number
	/** The next offset this consumer will fetch. */
	position: bigint
	/**
	 * The first offset of the earliest batch fetched but not delivered yet, else `position`. A batch is delivered
	 * as a whole, once its handler is called (`runBatch`) or its first record is handed out (`runEach`,
	 * `stream`): see `Consumer.nextOffset()` for when this is an offset to commit.
	 */
	nextOffset?: bigint
	/**
	 * How far this consumer could read as of its last fetch response: the high watermark, or the
	 * last stable offset under `read_committed`. `endOffset - position` is how far its fetching is
	 * behind. Undefined before the first response and while the partition is paused, because a
	 * paused partition is not fetched and its end is not known.
	 */
	endOffset?: bigint
	/** Bytes fetched for this partition that no handler has received yet. */
	bufferedBytes: number
	/** The number of records `bufferedBytes` holds. */
	bufferedRecords: number
	paused: boolean
}

export interface ConsumerGroupEvents {
	rebalance: []
	joined: [assignment: TopicPartition[]]
	left: []
	error: [error: Error]
	/** Emitted on fatal session loss (UNKNOWN_MEMBER_ID, FENCED_INSTANCE_ID) - offsets cannot be committed */
	sessionLost: [partitions: TopicPartition[]]
}

export interface ConsumerEvents {
	running: []
	stopped: []
	error: [error: Error]
	/** Emitted when a rebalance starts, after `onBeforeRebalance` returned. */
	rebalance: []
	/** Emitted when the group asks for a rebalance, before the running handlers have returned (see `rebalancePending`). */
	rebalancePending: []
	/** Emitted when a rebalance finished (the member rejoined and took its new partitions), unless another one is pending. */
	rebalanceComplete: []
	/** Emitted when partitions were assigned, once `onPartitionsAssigned` returned. */
	partitionsAssigned: [partitions: AssignedPartition[]]
	/** Emitted when partitions are revoked during rebalance (offsets can still be committed) */
	partitionsRevoked: [partitions: TopicPartition[]]
	/** Emitted on fatal session loss - offsets cannot be committed for these partitions */
	partitionsLost: [partitions: TopicPartition[]]
	/** Emitted when fetching advances; offset is the next fetch position after this range. */
	fetchPosition: [position: TopicPartitionOffset, recordCount: number]
	/**
	 * Emitted when a partition's position is out of range and its reset policy is 'none': the partition is paused.
	 * `seek()` it to where it goes on, then `resume()` it. With no listener for this event the run fails with
	 * `OffsetOutOfRangeError` instead.
	 */
	offsetOutOfRange: [position: TopicPartitionOffset]
	/** Emitted when a partition's position was set by its reset policy ('earliest' or 'latest'). */
	offsetReset: [reset: OffsetResetEvent]
}

export interface InternalConsumerContext {
	cluster: Cluster
	groupId: string
	groupInstanceId?: string
	memberId: string
	generationId: number
	signal: AbortSignal
}
