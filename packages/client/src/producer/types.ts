/**
 * Producer type definitions
 */

import type { CompressionType } from '@/protocol/records/compression.js'
import type { TopicDefinition as TopicDefinitionImport } from '@/topic.js'
import type { ConsumeContext, ConsumerGroupMetadata } from '@/consumer/types.js'

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
	 * Number of transactions that may be in flight at once (default: 1)
	 *
	 * The Kafka protocol allows one open transaction per transactional ID, so
	 * values above 1 make this producer manage a pool of N internal transactional
	 * producers ("lanes"). Lane 0 uses `transactionalId` verbatim; lanes 1..N-1
	 * append `-1`..`-{N-1}`. transaction() calls are admitted first-in first-out:
	 * each call takes a free lane and waits when all lanes are busy.
	 *
	 * - Transactions begin in call order but may commit in any order.
	 * - Transactional-ID zombie fencing applies per lane. For consume-transform-produce,
	 *   pass `consumerGroupMetadata` to sendOffsets() (KIP-447) so zombie commits are
	 *   fenced by consumer group generation regardless of which lane they ran on.
	 * - Once a lane is fenced (or otherwise in the 'error' state), so is the pool: it begins
	 *   no transaction any more, like a single producer in that state; replace it.
	 * - Requires transactionalId. Only affects transaction().
	 */
	transactionConcurrency?: number
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
	 *    (a beginTransaction() handle has none: its signal bounds it until commit() or abort() is
	 *    called, and those take their own timeoutMs and signal)
	 *
	 * Keep these in sync to avoid surprises. Only used when transactionalId is set.
	 */
	transactionTimeoutMs?: number
	/**
	 * Maximum time in ms to block while initializing a transactional or idempotent producer,
	 * retrying coordinator discovery and InitProducerId before failing with a `TimeoutError`
	 * (default: 60000), requests in flight included. A timed-out initialization is tried again by
	 * the next send or transaction, which first awaits an InitProducerId left unanswered (Java).
	 * A coordinator lookup that answers once the initialization gave up (timed out, the signal of
	 * the `beginTransaction()` waiting for it aborted, or the producer disconnected) sends no
	 * InitProducerId: it would bump the epoch of the transactional id behind a producer that
	 * replaced this one. One already sent may still be applied, like in the Java client.
	 *
	 * It also bounds the epoch bump of an abort (InitProducerId after a failed epoch). A bump that
	 * times out is not tried again: the abort rejects and the producer is in the 'error' state.
	 */
	maxBlockMs?: number
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
	transactionConcurrency: number
	idempotent: boolean
	maxInFlight: number
	transactionTimeoutMs: number
	maxBlockMs: number
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

/** What is waiting inside a producer at one moment (see `Producer.metrics()`). */
export interface ProducerMetrics {
	/** Batches, records and bytes ready to send but waiting behind a request in flight for their partition. */
	queuedBatches: number
	queuedRecords: number
	queuedBytes: number
	/** Produce requests sent and not answered yet. */
	inFlightRequests: number
	/** `send()` calls whose messages are not all acknowledged yet. */
	unacknowledgedSends: number
}

/**
 * Producer events
 */
export interface ProducerEvents {
	error: [error: Error]
	/**
	 * Emitted when transaction() has to wait for capacity behind other
	 * transactions. `queued` is the number of transactions ahead of this one
	 * (including the active one). Sustained queueing means throughput is
	 * commit-latency-bound.
	 */
	'transaction:queued': [info: { queued: number }]
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
 * An offset committed within a transaction (`sendOffsets`): `offset` is the next offset to read (the last processed
 * offset + 1)
 */
export interface TransactionOffset {
	/** Topic name */
	topic: string
	/** Partition index */
	partition: number
	/** Offset to commit */
	offset: bigint
	/** Leader epoch (optional) */
	leaderEpoch?: number
	/** Stored with the offset and read back with it, e.g. by `consumer.committed()` (optional) */
	metadata?: string
}

/**
 * Consumer group metadata for exactly-once semantics
 *
 * When consuming and producing within a transaction (consume-transform-produce),
 * include the consumer's group metadata to ensure offsets are committed atomically
 * with the consumer's current group membership.
 */
export type GroupMetadata = ConsumerGroupMetadata

/**
 * Parameters for sendOffsets within a transaction
 */
export interface SendOffsetsParams {
	/** Consumer group ID (for producer-only transactions) */
	groupId?: string
	/** Consumer group metadata (for full EOS with consumer) */
	consumerGroupMetadata?: GroupMetadata
	/** Offsets to commit */
	offsets: TransactionOffset[]
}

/**
 * Transaction options
 */
export interface TransactionOptions {
	/** Transaction timeout in ms (default: 60000) */
	timeoutMs?: number
}

/**
 * Bounds of an asynchronous producer call
 */
export interface ProducerCallOptions {
	/** Reject with a `TimeoutError` once this many ms passed */
	timeoutMs?: number
	/** Aborting it rejects the call with the signal's reason */
	signal?: AbortSignal
}

/**
 * Options of `producer.beginTransaction()`
 */
export interface BeginTransactionOptions {
	/**
	 * Aborting it rejects a call still waiting to begin (also stopping the producer's initialization that call waits
	 * for: no InitProducerId is sent after it, though one already sent may still be applied), and aborts the
	 * transaction once it is open, until `commit()` or `abort()` is called on the handle; from then on it has no
	 * effect, and those calls are bounded only by their own `timeoutMs` and `signal`. Without it, an open transaction
	 * has no client-side timeout (the broker's `transactionTimeoutMs` applies).
	 */
	signal?: AbortSignal
}

/**
 * Options of `producer.disconnect()`
 */
export interface DisconnectOptions {
	/** Upper bound in ms for the flush and for ending an open transaction (default: 5000) */
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
	 *
	 * With consumer group metadata (directly or from the context), the commit is fenced by the
	 * member's generation (KIP-447): metadata with a negative generation or an empty member id
	 * is rejected with `CommitFailedError` before anything is sent. The `{ groupId }` form is
	 * not fenced.
	 *
	 * The offsets belong to this transaction only: a commit waits for a registration still in
	 * progress (AddOffsetsToTxn) and commits its offsets with the transaction. Like a failed send,
	 * a call of an open transaction that fails (also one not awaited, also for its arguments)
	 * makes the commit abort. Once the transaction
	 * aborts, a registration in progress sends nothing more and rejects with
	 * `TransactionAbortedError`, also when its request is answered afterwards.
	 */
	sendOffsets(context: ConsumeContext): Promise<void>
	sendOffsets(context: ConsumeContext, offsets: TransactionOffset[]): Promise<void>
	sendOffsets(params: SendOffsetsParams): Promise<void>

	/**
	 * Signal that is aborted when the transaction times out or is aborted, with a
	 * `TransactionAbortedError` as its reason
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

/**
 * An open transaction, from `producer.beginTransaction()`
 *
 * It holds the producer's turn until `commit()` or `abort()` ended it (or the signal given to
 * `beginTransaction()` aborted it): other `beginTransaction()` and `transaction()` calls wait.
 * Its `signal` is aborted when the transaction ends without committing.
 */
export interface TransactionHandle extends ProducerTransaction {
	/**
	 * Commit: flush the transaction's sends, wait for its offset registrations, commit its offsets
	 * (TxnOffsetCommit), EndTxn(commit)
	 *
	 * Rejects with `TransactionAbortedError` when the transaction did not commit:
	 * - `outcome: 'aborted'`: it was aborted, as `abort()` does (also when a send failed, or the
	 *   producer's epoch did: then nothing can commit); or `timeoutMs` passed or `signal` aborted
	 *   before EndTxn(commit) was sent, and then nothing more is sent: the broker's transaction
	 *   timeout ends the transaction and the producer enters the 'error' state.
	 * - `outcome: 'unknown'`: EndTxn(commit) was sent but not confirmed, so the broker may have
	 *   committed. The producer enters the 'error' state.
	 *
	 * Rejects with `ProducerFencedError` once the producer is fenced (see `abort()`), else with
	 * `InvalidTxnStateError` once the transaction is committing or ended.
	 */
	commit(options?: ProducerCallOptions): Promise<void>
	/**
	 * Abort: reject the queued sends, wait for those in flight, EndTxn(abort)
	 *
	 * Offset registrations, and sends whose records were not taken for sending yet, start no
	 * request any more and reject with `TransactionAbortedError`, also when a registration already
	 * sent is answered afterwards (what it added is not used); their records are dropped. A produce
	 * request already sent, and its retries, is awaited (EndTxn follows it), and its send resolves
	 * or rejects with the broker's answer, like in the Java client (the abort marker discards its
	 * records). Nothing of the transaction reaches the next one. EndTxn is sent once the
	 * coordinator may have begun the transaction, also when the answer to a registration was lost
	 * (then a coordinator that never began it answers INVALID_TXN_STATE, and the abort resolves).
	 *
	 * When the producer's epoch failed during the transaction (an out-of-order sequence, a send
	 * whose outcome its retries left unknown, InvalidProducerEpoch on a produce, or
	 * INVALID_PRODUCER_ID_MAPPING from the coordinator: the transactional id expired), its sends
	 * were rejected and nothing more went out; after EndTxn (skipped for INVALID_PRODUCER_ID_MAPPING,
	 * which the coordinator answers EndTxn too), InitProducerId bumps the epoch (Java parity; once the
	 * id expired, it gets a new producer id), and the producer runs the next transaction under the
	 * new one (`disconnect()` skips the bump, also one in progress).
	 *
	 * If that does not finish (EndTxn or the bump fails, `timeoutMs` passes or `signal` aborts),
	 * the producer enters the 'error' state and this rejects. A fenced producer (another one with
	 * the same transactional id took over, or the broker aborted the transaction after
	 * `transactionTimeoutMs`: answers PRODUCER_FENCED, or INVALID_PRODUCER_EPOCH from a
	 * coordinator) is in the 'error' state for good, never initialized again: this sends nothing
	 * (the broker aborted the transaction) and rejects with the `ProducerFencedError`, also once
	 * the transaction ended, like every later call of the handle and every later begin; replace
	 * the producer. Otherwise, on a transaction that already ended or is ending, it resolves once
	 * that aborted it, and rejects with `InvalidTxnStateError` while committing, once committed, and
	 * once it ended in the 'error' state: its abort did not finish, or its commit's outcome is
	 * unknown (the broker may have committed it; Java refuses that abort too).
	 */
	abort(options?: ProducerCallOptions): Promise<void>
}
