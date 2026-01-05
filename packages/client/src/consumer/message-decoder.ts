/**
 * Message decoding utilities for the consumer
 *
 * Provides shared utilities for decoding Kafka records into messages.
 * Used by all consumer run modes (each, batch, stream).
 */

import type { DecodedRecord, RecordHeader } from '@/protocol/records/index.js'
import type { TopicSubscription, Message } from './types.js'
import { normalizeDecoder } from './types.js'

// Shared empty objects for common cases (optimization)
export const EMPTY_BUFFER = Buffer.alloc(0)
export const EMPTY_HEADERS: Record<string, Buffer> = Object.freeze({}) as Record<string, Buffer>

/**
 * Pre-computed decoder maps for O(1) lookup per topic
 */
export interface DecoderMaps {
	valueDecoder: Map<string, (buf: Buffer) => unknown>
	keyDecoder: Map<string, (buf: Buffer) => unknown>
}

/**
 * Default decoder that returns the buffer as-is
 */
const defaultDecoder = (b: Buffer): Buffer => b

/**
 * Build decoder maps from subscriptions for O(1) lookup per record
 */
export function buildDecoderMaps(subscriptions: TopicSubscription<unknown, unknown>[]): DecoderMaps {
	const valueDecoder = new Map<string, (buf: Buffer) => unknown>()
	const keyDecoder = new Map<string, (buf: Buffer) => unknown>()

	for (const sub of subscriptions) {
		valueDecoder.set(sub.topic, normalizeDecoder(sub.decoder))
		if (sub.keyDecoder) {
			keyDecoder.set(sub.topic, normalizeDecoder(sub.keyDecoder))
		}
	}

	return { valueDecoder, keyDecoder }
}

/**
 * Get the value decoder for a topic, falling back to default
 */
export function getValueDecoder(decoders: DecoderMaps, topic: string): (buf: Buffer) => unknown {
	return decoders.valueDecoder.get(topic) ?? defaultDecoder
}

/**
 * Get the key decoder for a topic, falling back to default
 */
export function getKeyDecoder(decoders: DecoderMaps, topic: string): (buf: Buffer) => unknown {
	return decoders.keyDecoder.get(topic) ?? defaultDecoder
}

/**
 * Convert RecordHeader[] to Record<string, Buffer>
 */
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

/**
 * Decode a record into a Message using the provided decoder maps
 */
export function decodeRecord(
	topic: string,
	partition: number,
	record: DecodedRecord,
	decoders: DecoderMaps
): Message<unknown, unknown> {
	const valueDecoder = getValueDecoder(decoders, topic)
	const keyDecoder = getKeyDecoder(decoders, topic)

	return {
		topic,
		partition,
		offset: record.offset,
		timestamp: record.timestamp,
		key: record.key === null ? null : keyDecoder(record.key),
		// Preserve tombstones (null values) without decoding.
		// Note: types do not currently model nullable values in Message<V, K>.
		value: record.value === null ? null : valueDecoder(record.value ?? EMPTY_BUFFER),
		headers: convertHeaders(record.headers),
	}
}
