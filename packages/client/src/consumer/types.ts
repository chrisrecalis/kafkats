/**
 * Consumer types and interfaces
 */

import type { Cluster } from '@/client/cluster.js'
import type { TopicDefinition, DecoderLike } from '@/topic.js'
export { normalizeDecoder } from '@/topic.js'
export type { Decoder, DecoderLike } from '@/topic.js'

// ==================== Subscription Types ====================

/**
 * Topic subscription configuration
 */
export interface TopicSubscription<V = Buffer, K = Buffer> {
	topic: string
	decoder: DecoderLike<V>
	keyDecoder?: DecoderLike<K>
}

/**
 * A subscription-like type that can be either TopicSubscription or TopicDefinition
 */
export type SubscriptionLike<V, K> = TopicSubscription<V, K> | TopicDefinition<V, K>

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
/* eslint-enable @typescript-eslint/no-explicit-any */

/**
 * Check if a subscription is a TopicDefinition (not a TopicSubscription)
 * TopicDefinition has key/value codecs, TopicSubscription has decoder
 */
export function isTopicDefinition<V, K>(sub: SubscriptionLike<V, K>): sub is TopicDefinition<V, K> {
	// TopicSubscription has 'decoder', TopicDefinition has key/value codecs
	return !('decoder' in sub)
}

/**
 * Convert SubscriptionLike to TopicSubscription
 * TopicDefinition uses its value codec as the decoder (defaults to Buffer)
 */
export function toTopicSubscription(sub: string): TopicSubscription<Buffer, Buffer>
export function toTopicSubscription<V, K>(sub: SubscriptionLike<V, K>): TopicSubscription<V, K>
export function toTopicSubscription<V, K>(
	sub: SubscriptionLike<V, K> | string
): TopicSubscription<V | Buffer, K | Buffer> {
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
 * Context provided to message handlers
 */
export interface ConsumeContext {
	signal: AbortSignal
	topic: string
	partition: number
	offset: bigint
}

// ==================== Handler Types ====================

/**
 * Handler for single message mode
 */
export type MessageHandler<V = Buffer, K = Buffer> = (message: Message<V, K>, ctx: ConsumeContext) => Promise<void>

/**
 * Handler for batch message mode
 */
export type BatchHandler<V = Buffer, K = Buffer> = (batch: Message<V, K>[], ctx: ConsumeContext) => Promise<void>

// ==================== Run Options ====================

/**
 * Options for runEach() - single message processing
 */
export interface RunEachOptions {
	partitionConcurrency?: number
	autoCommit?: boolean
	commitOffsets?: boolean
	autoCommitIntervalMs?: number
	signal?: AbortSignal
}

/**
 * Options for runBatch() - batch message processing
 */
export interface RunBatchOptions {
	partitionConcurrency?: number
	autoCommit?: boolean
	commitOffsets?: boolean
	autoCommitIntervalMs?: number
	signal?: AbortSignal
	maxBatchSize?: number
	maxBatchWaitMs?: number
}

// ==================== Consumer Configuration ====================

/**
 * Auto offset reset strategy
 * Defines behavior when there is no committed offset or offset is out of range
 */
export type AutoOffsetReset = 'earliest' | 'latest' | 'none'

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
	maxBytesPerPartition?: number
	minBytes?: number
	maxWaitMs?: number
	/**
	 * What to do when there is no committed offset (group first initialized)
	 * or when the offset is out of range:
	 * - 'earliest': Reset to earliest available offset
	 * - 'latest': Reset to latest offset (default)
	 * - 'none': Throw an error if no committed offset exists
	 */
	autoOffsetReset?: AutoOffsetReset
	/**
	 * Controls which messages are visible to the consumer:
	 * - 'read_uncommitted': Read all messages including uncommitted transactional messages
	 * - 'read_committed': Only read committed transactional messages (default)
	 */
	isolationLevel?: IsolationLevel
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
	autoOffsetReset: AutoOffsetReset
	isolationLevel: IsolationLevel
	partitionAssignmentStrategy: PartitionAssignmentStrategy
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
	partitionAssignmentStrategy: 'cooperative-sticky' as PartitionAssignmentStrategy,
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
	maxBatchSize: 100,
	maxBatchWaitMs: 50,
} as const

// ==================== Internal Types ====================

export interface TopicPartition {
	topic: string
	partition: number
}

export interface TopicPartitionOffset extends TopicPartition {
	offset: bigint
}

export enum ConsumerGroupState {
	UNJOINED = 'UNJOINED',
	JOINING = 'JOINING',
	AWAITING_SYNC = 'AWAITING_SYNC',
	STABLE = 'STABLE',
	LEAVING = 'LEAVING',
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
	rebalance: []
	partitionsAssigned: [partitions: TopicPartition[]]
	/** Emitted when partitions are revoked during rebalance (offsets can still be committed) */
	partitionsRevoked: [partitions: TopicPartition[]]
	/** Emitted on fatal session loss - offsets cannot be committed for these partitions */
	partitionsLost: [partitions: TopicPartition[]]
}

export interface InternalConsumerContext {
	cluster: Cluster
	groupId: string
	groupInstanceId?: string
	memberId: string
	generationId: number
	signal: AbortSignal
}
