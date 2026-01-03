import { randomBytes } from 'node:crypto'

import type { RecordHeader } from '@/protocol/records/index.js'

import type { SubscriptionLike, TopicPartition, TopicSubscription } from './types.js'
import { toTopicSubscription } from './types.js'

const EMPTY_HEADERS: Record<string, Buffer> = Object.freeze({}) as Record<string, Buffer>

export function randomKafkaUuid(): string {
	// Kafka uses base64url (no padding) for UUID string representations.
	return randomBytes(16).toString('base64url')
}

export function convertHeaders(headers: RecordHeader[]): Record<string, Buffer> {
	if (headers.length === 0) {
		return EMPTY_HEADERS
	}
	const result: Record<string, Buffer> = {}
	for (const h of headers) {
		if (h.value !== null) {
			result[h.key] = h.value
		}
	}
	return result
}

export function isControlBatch(attributes: number): boolean {
	// RecordBatch attributes: bit 5 indicates control batch.
	return (attributes & 0x20) !== 0
}

export const SHARE_SESSION_INITIAL_EPOCH = 0
const SHARE_SESSION_FINAL_EPOCH = -1
const SHARE_SESSION_MAX_EPOCH = 0x7fffffff

export function nextShareSessionEpoch(epoch: number): number {
	if (epoch < 0) {
		return SHARE_SESSION_FINAL_EPOCH
	}
	if (epoch === SHARE_SESSION_MAX_EPOCH) {
		return 1
	}
	return epoch + 1
}

export function toTopicPartitionKey(tp: TopicPartition): string {
	return `${tp.topic}:${tp.partition}`
}

export function toShareTopicSubscription(sub: string): TopicSubscription<Buffer, Buffer>
export function toShareTopicSubscription<V, K>(sub: SubscriptionLike<V, K>): TopicSubscription<V, K>
export function toShareTopicSubscription<V, K>(
	sub: SubscriptionLike<V, K> | string
): TopicSubscription<V | Buffer, K | Buffer> {
	// Runtime can handle either shape; overloads above provide correct inference for callers.
	return toTopicSubscription(sub as unknown as SubscriptionLike<V, K>)
}
