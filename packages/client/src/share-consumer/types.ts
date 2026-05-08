/**
 * Share consumer types and interfaces.
 *
 * Implements Kafka Share Groups (KIP-932), generally available since Kafka 4.2.
 * Supports ShareFetch/ShareAcknowledge v1 (Kafka 4.1+) and v2 (Kafka 4.2+).
 */

import type { TopicDefinition, DecoderLike } from '@/topic.js'

// ==================== Subscription Types ====================

/** Acquire mode for ShareFetch (KIP-1206, Kafka 4.2+). */
export type ShareConsumerAcquireMode = 'batch_optimized' | 'record_limit'

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

/** Topic-partition assignment */
export interface TopicPartition {
	topic: string
	partition: number
}

/** Context provided to message handlers */
export interface ConsumeContext {
	signal: AbortSignal
	topic: string
	partition: number
	offset: bigint
}

/** Share consumer configuration */
export interface ShareConsumerConfig {
	/**
	 * Share group ID (Kafka share group == group.id)
	 */
	groupId: string
	/**
	 * Optional rack ID hint for rack-aware assignment.
	 */
	rackId?: string
	/**
	 * Fetch wait time (ShareFetch.max_wait_ms)
	 */
	maxWaitMs?: number
	/**
	 * Minimum bytes (ShareFetch.min_bytes)
	 */
	minBytes?: number
	/**
	 * Maximum bytes (ShareFetch.max_bytes)
	 */
	maxBytes?: number
	/**
	 * Maximum records to return (ShareFetch v1+)
	 */
	maxRecords?: number
	/**
	 * Suggested batch size for acquired records / acknowledgements (ShareFetch v1+)
	 */
	batchSize?: number
	/**
	 * Acquire mode (KIP-1206, ShareFetch v2+, Kafka 4.2+).
	 * - `batch_optimized` (default): broker may return more than `maxRecords` to align batch boundaries.
	 * - `record_limit`: broker strictly caps the response at `maxRecords`. Requires v2; throws on older brokers.
	 */
	acquireMode?: ShareConsumerAcquireMode
}

/** A consumed share message with decoded value */
export interface ShareMessage<V = Buffer, K = Buffer> {
	topic: string
	partition: number
	/**
	 * Delivery offset for the record.
	 */
	offset: bigint
	timestamp: bigint
	key: K | null
	value: V
	headers: Record<string, Buffer>
	/**
	 * Delivery attempt count when available (protocol-dependent).
	 */
	deliveryCount?: number
	/**
	 * Acknowledge this record as successfully processed (ACCEPT).
	 */
	ack(): Promise<void>
	/**
	 * Release this record for another delivery attempt (RELEASE).
	 */
	release(): Promise<void>
	/**
	 * Reject this record and do not make it eligible for redelivery (REJECT).
	 */
	reject(): Promise<void>
	/**
	 * Renew the acquisition lock (KIP-1222, Kafka 4.2+).
	 *
	 * Use for long-running handlers that need more time than the broker's lock timeout.
	 * The message remains in-flight after `renew()`; the handler must still call `ack`/`release`/`reject`
	 * (or `runEach()` will auto-ack on success). Multiple renews per message are allowed.
	 *
	 * Throws if the broker does not support ShareAcknowledge v2.
	 */
	renew(): Promise<void>
}

/** Handler for ShareConsumer message processing */
export type ShareMessageHandler<V = Buffer, K = Buffer> = (
	message: ShareMessage<V, K>,
	ctx: ConsumeContext
) => Promise<void>

/**
 * Subscription input for ShareConsumer.
 *
 * Supports the same subscription shapes as `Consumer`.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export type ShareSubscriptionInput =
	| string
	| SubscriptionLike<any, any>
	| readonly (string | SubscriptionLike<any, any>)[]

export type ShareMsgOf<S> = S extends string
	? Buffer
	: S extends TopicSubscription<infer V, any>
		? V
		: S extends TopicDefinition<infer V, any>
			? V
			: S extends readonly unknown[]
				? ShareMsgOf<S[number]>
				: never

export type ShareKeyOf<S> = S extends string
	? Buffer
	: S extends TopicSubscription<any, infer K>
		? K
		: S extends TopicDefinition<any, infer K>
			? K
			: S extends readonly unknown[]
				? ShareKeyOf<S[number]>
				: never
/* eslint-enable @typescript-eslint/no-explicit-any */

/** Options for runEach() - single message processing */
export interface ShareRunEachOptions {
	/**
	 * Maximum number of records to process concurrently.
	 *
	 * Share Groups are designed for queue-like scaling where concurrency is not limited by partitions.
	 */
	concurrency?: number
	signal?: AbortSignal
	/**
	 * Number of successful messages to batch into a single ShareAcknowledge request per partition.
	 */
	ackBatchSize?: number
	/**
	 * Backoff applied when no records are returned.
	 */
	idleBackoffMs?: number
}

/** Share consumer events */
export interface ShareConsumerEvents {
	running: []
	stopped: []
	error: [Error]
	partitionsAssigned: [TopicPartition[]]
	partitionsRevoked: [TopicPartition[]]
}

export const DEFAULT_SHARE_CONSUMER_CONFIG: Required<
	Pick<ShareConsumerConfig, 'maxWaitMs' | 'minBytes' | 'maxBytes' | 'maxRecords' | 'batchSize' | 'acquireMode'>
> = {
	maxWaitMs: 5000,
	minBytes: 1,
	maxBytes: 1048576,
	maxRecords: 500,
	batchSize: 100,
	acquireMode: 'batch_optimized',
}

export const DEFAULT_SHARE_RUN_EACH_OPTIONS: Required<Pick<ShareRunEachOptions, 'concurrency'>> &
	Required<Pick<ShareRunEachOptions, 'ackBatchSize' | 'idleBackoffMs'>> = {
	concurrency: 10,
	ackBatchSize: 1000,
	idleBackoffMs: 200,
}
