/**
 * Producer type definitions
 */

import type { CompressionType } from '@/protocol/records/compression.js'
import type { TopicDefinition as TopicDefinitionImport } from '@/topic.js'

// Re-export topic types from shared module
export type { Decoder, DecoderLike, TopicDefinition, TopicOptions } from '@/topic.js'
export { topic, normalizeDecoder } from '@/topic.js'

// Local type alias for use within this file
type TopicDefinition<V, K> = TopicDefinitionImport<V, K>

// ==================== Producer Configuration ====================

/**
 * Acknowledgment modes for produce requests
 */
export type Acks = 'all' | 'leader' | 'none'

/**
 * Built-in partitioner names
 */
export type PartitionerName = 'murmur2' | 'round-robin'

/**
 * Custom partitioner function signature
 *
 * @param topic - Topic name
 * @param key - Message key (null if not provided)
 * @param value - Encoded message value
 * @param partitionCount - Number of partitions for the topic
 * @returns Partition index, or -1 to use sticky partitioner for null keys
 */
export type PartitionerFunction = (topic: string, key: Buffer | null, value: Buffer, partitionCount: number) => number

/**
 * Partitioner can be a built-in name or custom function
 */
export type Partitioner = PartitionerName | PartitionerFunction

/**
 * Compression type names for configuration
 */
export type CompressionName = 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'

/**
 * Producer configuration options
 */
export interface ProducerConfig {
	/** Wait up to N ms to accumulate messages before sending (default: 5) */
	lingerMs?: number
	/** Flush when batch reaches N bytes (default: 16384) */
	maxBatchBytes?: number
	/** Required acknowledgments (default: 'all') */
	acks?: Acks
	/** Number of retries on retriable errors (default: 3) */
	retries?: number
	/** Initial backoff between retries in ms (default: 100) */
	retryBackoffMs?: number
	/** Maximum backoff between retries in ms (default: 1000) */
	maxRetryBackoffMs?: number
	/** Compression type (default: 'none') */
	compression?: CompressionName
	/** Partitioner strategy (default: 'murmur2') */
	partitioner?: Partitioner
	/** Request timeout in ms (default: 30000) */
	requestTimeoutMs?: number
	/** Transactional ID for transactional producer (optional) */
	transactionalId?: string
	/**
	 * Optional trace hook for lightweight benchmarking instrumentation.
	 * Intended for diagnostics only and may be omitted in production usage.
	 */
	trace?: ProducerTraceFn
	/**
	 * Enable idempotent producer mode for exactly-once delivery semantics (default: false)
	 *
	 * When enabled:
	 * - Producer will be assigned a unique ID and epoch by the broker
	 * - Per-partition sequence numbers ensure duplicate detection
	 * - Requires acks='all', retries >= 1, maxInFlight <= 5
	 */
	idempotent?: boolean
	/**
	 * Maximum number of in-flight requests per broker connection (default: 5)
	 *
	 * When idempotent mode is enabled, this must be <= 5 (Kafka protocol limit).
	 * Higher values increase throughput but may cause reordering if retries occur.
	 */
	maxInFlight?: number
	/**
	 * Transaction timeout in ms (default: 60000)
	 *
	 * This timeout governs TWO behaviors:
	 * 1. **Broker-side**: Passed to InitProducerId; broker aborts transaction if
	 *    not committed/aborted within this time
	 * 2. **Client-side**: Used by transaction() to timeout the user callback
	 *
	 * Keep these in sync to avoid surprises. Only used when transactionalId is set.
	 */
	transactionTimeoutMs?: number
}

/**
 * Internal resolved producer configuration with all defaults applied
 */
export interface ResolvedProducerConfig {
	lingerMs: number
	maxBatchBytes: number
	acks: number // Resolved to -1, 1, or 0
	retries: number
	retryBackoffMs: number
	maxRetryBackoffMs: number
	compression: CompressionType
	partitioner: PartitionerFunction
	requestTimeoutMs: number
	transactionalId: string | null
	idempotent: boolean
	maxInFlight: number
	transactionTimeoutMs: number
}

export type ProducerTraceStage = 'encode_batch' | 'produce_request'

export interface ProducerTraceEvent {
	stage: ProducerTraceStage
	durationMs: number
	recordCount?: number
	bytes?: number
	brokerId?: number
	topic?: string
	partition?: number
}

export type ProducerTraceFn = (event: ProducerTraceEvent) => void

// ==================== Message Types ====================

/**
 * Message to be produced (typed by topic value type)
 */
export interface ProducerMessage<V = Buffer, K = Buffer | string> {
	/** Message key (optional) */
	key?: K | null
	/** Message value */
	value: V | null
	/** Message headers (optional) */
	headers?: Record<string, string | Buffer>
	/** Explicit partition override - bypasses partitioner (optional) */
	partition?: number
	/** Message timestamp - defaults to current time (optional) */
	timestamp?: Date
}

/**
 * Result of a successful produce operation
 */
export interface SendResult {
	/** Topic the message was sent to */
	topic: string
	/** Partition the message was sent to */
	partition: number
	/** Offset of the message in the partition */
	offset: bigint
	/** Timestamp of the message (broker time if LogAppendTime) */
	timestamp: Date
}

// ==================== Internal Types ====================

/**
 * Internal representation of a queued message awaiting send
 */
export interface QueuedMessage {
	topic: string
	partition: number
	key: Buffer | null
	value: Buffer | null
	headers: Record<string, Buffer>
	timestamp: bigint
	resolve: (result: SendResult) => void
	reject: (error: Error) => void
}

/**
 * Batch of messages for a single topic-partition
 */
export interface PartitionBatch {
	topic: string
	partition: number
	messages: QueuedMessage[]
	sizeBytes: number
	createdAt: number
}

/**
 * Producer state
 */
export type ProducerState = 'idle' | 'running' | 'stopping'

/**
 * Idempotent producer state machine
 *
 * State transitions:
 * - idle: Initial state, no producer ID yet
 * - initializing: InitProducerId in progress, send() blocks/waits
 * - running: Ready to produce, has valid producerId/epoch
 * - fenced: Producer was fenced, must re-initialize before producing
 * - error: Unrecoverable error (e.g., auth failure)
 */
export type IdempotentState = 'idle' | 'initializing' | 'running' | 'fenced' | 'error'

/**
 * Producer events
 */
export interface ProducerEvents {
	error: [error: Error]
}

// ==================== Transaction Types ====================

/**
 * Transaction state machine
 *
 * State transitions:
 * - idle: No active transaction
 * - in_transaction: Between begin and commit/abort
 * - committing: EndTxn(committed=true) in progress
 * - aborting: EndTxn(committed=false) in progress
 * - error: Fatal error occurred
 */
export type TransactionState = 'idle' | 'in_transaction' | 'committing' | 'aborting' | 'error'

/**
 * Topic-partition offset for transactional offset commit
 */
export interface TopicPartitionOffset {
	/** Topic name */
	topic: string
	/** Partition index */
	partition: number
	/** Offset to commit */
	offset: bigint
	/** Leader epoch (optional) */
	leaderEpoch?: number
	/** Metadata (optional) */
	metadata?: string
}

/**
 * Consumer group metadata for exactly-once semantics
 *
 * When consuming and producing within a transaction (consume-transform-produce),
 * include the consumer's group metadata to ensure offsets are committed atomically
 * with the consumer's current group membership.
 */
export interface GroupMetadata {
	/** Consumer group ID */
	groupId: string
	/** Consumer's current generation ID */
	generationId: number
	/** Consumer's member ID */
	memberId: string
	/** Consumer's static group instance ID (optional) */
	groupInstanceId?: string | null
}

/**
 * Parameters for sendOffsets within a transaction
 */
export interface SendOffsetsParams {
	/** Consumer group ID (for producer-only transactions) */
	groupId?: string
	/** Consumer group metadata (for full EOS with consumer) */
	consumerGroupMetadata?: GroupMetadata
	/** Offsets to commit */
	offsets: TopicPartitionOffset[]
}

/**
 * Transaction options
 */
export interface TransactionOptions {
	/** Transaction timeout in ms (default: 60000) */
	timeoutMs?: number
}

/**
 * Represents an active transaction context
 */
export interface ProducerTransaction {
	/**
	 * Send messages within the transaction
	 */
	send(
		topic: string,
		messages: ProducerMessage<Buffer, Buffer | string> | ProducerMessage<Buffer, Buffer | string>[]
	): Promise<SendResult | SendResult[]>
	send<V, K>(
		topicDef: TopicDefinition<V, K>,
		messages: ProducerMessage<V, K> | ProducerMessage<V, K>[]
	): Promise<SendResult | SendResult[]>

	/**
	 * Commit consumer offsets as part of the transaction
	 * (for consume-transform-produce patterns)
	 */
	sendOffsets(params: SendOffsetsParams): Promise<void>

	/**
	 * Signal that is aborted when the transaction times out or is aborted
	 *
	 * Use this to cancel long-running operations when the transaction fails:
	 * - Aborted on timeout (after `transactionTimeoutMs`)
	 * - Aborted if user code throws an error
	 * - Aborted if commit fails and transaction is rolled back
	 *
	 * **Important**: Operations started after the transaction leaves `in_transaction`
	 * state will throw `InvalidTxnStateError`. Avoid fire-and-forget async tasks
	 * inside transactions.
	 *
	 * @example
	 * ```typescript
	 * await producer.transaction(async (tx) => {
	 *   // Pass signal to fetch for cancellation
	 *   const data = await fetch(url, { signal: tx.signal });
	 *   await tx.send(topic, { value: data });
	 * });
	 * ```
	 */
	readonly signal: AbortSignal
}
