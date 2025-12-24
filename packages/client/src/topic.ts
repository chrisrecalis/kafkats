/**
 * Topic definition and serialization types
 *
 * Provides a unified topic() helper that works for both producer and consumer.
 * Supports various serialization patterns including zod schemas.
 */

import type { Codec } from '@/codec.js'

// ==================== Decoder Types ====================

/**
 * Decoder interface for deserializing message values
 * Supports both decode() and parse() methods (for zod compatibility)
 */
export interface Decoder<T> {
	decode?(buffer: Buffer): T
	parse?(buffer: Buffer): T
}

/**
 * Decoder can be an object with decode/parse method or a function
 */
export type DecoderLike<T> = Decoder<T> | ((buffer: Buffer) => T)

/**
 * Normalize a DecoderLike to a decode function
 */
export function normalizeDecoder<T>(decoder: DecoderLike<T>): (buffer: Buffer) => T {
	if (typeof decoder === 'function') {
		return decoder
	}
	// Support both decode() and parse() (zod uses parse)
	if (decoder.decode) {
		return buffer => decoder.decode!(buffer)
	}
	if (decoder.parse) {
		return buffer => decoder.parse!(buffer)
	}
	throw new Error('Decoder must have a decode() or parse() method')
}

// ==================== Topic Definition ====================

/**
 * Topic definition with optional key/value codecs.
 * If none are provided, key/value default to Buffer.
 */
export interface TopicDefinition<V = Buffer, K = Buffer> {
	readonly topic: string
	readonly key?: Codec<K>
	readonly value?: Codec<V>
}

/**
 * Options for creating a topic definition
 * If none are provided, key/value default to Buffer.
 */
export type TopicOptions<V = Buffer, K = Buffer> = {
	key?: Codec<K>
	value?: Codec<V>
}

/**
 * Create a typed topic definition
 *
 * Use this helper to define topics with type-safe serialization.
 * Supports functions, objects with encode/decode methods, or zod schemas.
 *
 * @example
 * ```typescript
 * // Custom value codec
 * const orderTopic = topic<Order>('orders', {
 *   value: {
 *     encode: (order) => Buffer.from(JSON.stringify(order)),
 *     decode: (buf) => JSON.parse(buf.toString()),
 *   },
 * })
 *
 * // With zod schema
 * const orderTopic = topic('orders', {
 *   value: zodCodec(orderSchema),
 * })
 *
 * // Key/value codec
 * const keyedOrders = topic('orders', {
 *   key: codec.string(),
 *   value: codec.json<Order>(),
 * })
 * ```
 *
 * @param name - Topic name
 * @param options - Key/value codecs
 * @returns A TopicDefinition for use with Producer.send() or Consumer.run()
 */
export function topic<V = Buffer, K = Buffer>(name: string, options: TopicOptions<V, K> = {}): TopicDefinition<V, K> {
	return {
		topic: name,
		key: options.key,
		value: options.value,
	}
}
