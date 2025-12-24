/**
 * Individual record encoding/decoding for Kafka RecordBatch v2
 *
 * Record format:
 * - length: varint (total record length after this field)
 * - attributes: int8 (currently unused, must be 0)
 * - timestampDelta: varint (relative to batch baseTimestamp)
 * - offsetDelta: varint (relative to batch baseOffset)
 * - keyLength: varint (-1 for null)
 * - key: bytes
 * - valueLength: varint (-1 for null)
 * - value: bytes
 * - headersCount: varint
 * - headers: array of { keyLength, key, valueLength, value }
 */

import { Encoder } from '@/protocol/primitives/index.js'
import type { IEncoder, IDecoder } from '@/protocol/primitives/index.js'
import { varIntSize } from '@/protocol/primitives/varint.js'

/**
 * Record header (key-value pair)
 */
export interface RecordHeader {
	key: string
	value: Buffer | null
}

// Shared empty array for records without headers (optimization)
const EMPTY_HEADERS: RecordHeader[] = []

/**
 * Record within a batch (for encoding)
 * Named KafkaRecord to avoid conflict with TypeScript's built-in Record type
 */
export interface KafkaRecord {
	attributes: number // Currently unused, must be 0
	timestampDelta: number // Relative to batch baseTimestamp
	offsetDelta: number // Relative to batch baseOffset
	key: Buffer | null
	value: Buffer | null
	headers: RecordHeader[]
}

/**
 * Decoded record with absolute offset and timestamp
 */
export interface DecodedRecord {
	offset: bigint
	timestamp: bigint
	key: Buffer | null
	value: Buffer | null
	headers: RecordHeader[]
}

export interface DecodeRecordInBatchOptions {
	/**
	 * Validate that we consumed exactly the record length.
	 *
	 * Disabling this avoids extra offset bookkeeping in the hot path. Kafka record
	 * encoding is stable and length-prefixing already bounds reads.
	 *
	 * @default true
	 */
	verifyLength?: boolean
}

/**
 * Encode a single record into a buffer
 *
 * @param record - The record to encode
 * @returns The encoded record as a buffer
 */
export function encodeRecord(record: KafkaRecord): Buffer {
	const headerKeyBuffers: Buffer[] = record.headers.map(h => Buffer.from(h.key, 'utf-8'))

	let bodySize = 1 // attributes (int8)
	bodySize += varIntSize(record.timestampDelta)
	bodySize += varIntSize(record.offsetDelta)

	// Key size
	if (record.key === null) {
		bodySize += varIntSize(-1)
	} else {
		bodySize += varIntSize(record.key.length)
		bodySize += record.key.length
	}

	// Value size
	if (record.value === null) {
		bodySize += varIntSize(-1)
	} else {
		bodySize += varIntSize(record.value.length)
		bodySize += record.value.length
	}

	// Headers size
	bodySize += varIntSize(record.headers.length)
	for (let i = 0; i < record.headers.length; i++) {
		const keyBytes = headerKeyBuffers[i]!
		const header = record.headers[i]!
		bodySize += varIntSize(keyBytes.length)
		bodySize += keyBytes.length

		if (header.value === null) {
			bodySize += varIntSize(-1)
		} else {
			bodySize += varIntSize(header.value.length)
			bodySize += header.value.length
		}
	}

	const totalSize = varIntSize(bodySize) + bodySize
	const encoder = new Encoder(totalSize)

	encoder.writeVarInt(bodySize)
	encoder.writeInt8(record.attributes)
	encoder.writeVarInt(record.timestampDelta)
	encoder.writeVarInt(record.offsetDelta)

	// Key (varInt length, -1 for null)
	if (record.key === null) {
		encoder.writeVarInt(-1)
	} else {
		encoder.writeVarInt(record.key.length)
		encoder.writeRaw(record.key)
	}

	// Value (varInt length, -1 for null)
	if (record.value === null) {
		encoder.writeVarInt(-1)
	} else {
		encoder.writeVarInt(record.value.length)
		encoder.writeRaw(record.value)
	}

	// Headers
	encoder.writeVarInt(record.headers.length)
	for (let i = 0; i < record.headers.length; i++) {
		const keyBytes = headerKeyBuffers[i]!
		const header = record.headers[i]!
		encoder.writeVarInt(keyBytes.length)
		encoder.writeRaw(keyBytes)

		if (header.value === null) {
			encoder.writeVarInt(-1)
		} else {
			encoder.writeVarInt(header.value.length)
			encoder.writeRaw(header.value)
		}
	}

	return encoder.toBuffer()
}

/**
 * Encode a record directly to an encoder
 *
 * @param encoder - The encoder to write to
 * @param record - The record to encode
 */
export function encodeRecordTo(encoder: IEncoder, record: KafkaRecord): void {
	encoder.writeRaw(encodeRecord(record))
}

/**
 * Decode a single record from a decoder
 *
 * @param decoder - The decoder to read from
 * @param baseOffset - The batch base offset
 * @param baseTimestamp - The batch base timestamp
 * @returns The decoded record
 */
export function decodeRecord(decoder: IDecoder, baseOffset: bigint, baseTimestamp: bigint): DecodedRecord {
	const length = decoder.readVarInt()
	const recordEnd = decoder.offset() + length

	const attributes = decoder.readInt8()
	if (attributes !== 0) {
		// Currently unused, but we accept any value for forward compatibility
	}

	const timestampDelta = decoder.readVarInt()
	const offsetDelta = decoder.readVarInt()

	// Key
	const keyLength = decoder.readVarInt()
	const key = keyLength < 0 ? null : decoder.readRaw(keyLength)

	// Value
	const valueLength = decoder.readVarInt()
	const value = valueLength < 0 ? null : decoder.readRaw(valueLength)

	// Headers (optimization: use shared empty array for common case)
	const headerCount = decoder.readVarInt()
	let headers: RecordHeader[]
	if (headerCount === 0) {
		headers = EMPTY_HEADERS
	} else {
		headers = new Array<RecordHeader>(headerCount)
		for (let i = 0; i < headerCount; i++) {
			const headerKeyLength = decoder.readVarInt()
			const headerKey = decoder.readRaw(headerKeyLength).toString('utf-8')

			const headerValueLength = decoder.readVarInt()
			const headerValue = headerValueLength < 0 ? null : decoder.readRaw(headerValueLength)

			headers[i] = { key: headerKey, value: headerValue }
		}
	}

	// Verify we consumed exactly the right number of bytes
	const actualEnd = decoder.offset()
	if (actualEnd !== recordEnd) {
		throw new Error(`Record length mismatch: expected to end at ${recordEnd}, but ended at ${actualEnd}`)
	}

	return {
		offset: baseOffset + BigInt(offsetDelta),
		timestamp: baseTimestamp + BigInt(timestampDelta),
		key,
		value,
		headers,
	}
}

/**
 * Decode a record when the absolute offset is already known (fast path).
 *
 * This avoids allocating/converting BigInts for `offsetDelta` when offsets are
 * sequential within a batch (the common case).
 */
export function decodeRecordInBatch(
	decoder: IDecoder,
	offset: bigint,
	baseTimestamp: bigint,
	options: DecodeRecordInBatchOptions = {}
): DecodedRecord {
	const length = decoder.readVarInt()

	let recordEnd = 0
	const verifyLength = options.verifyLength !== false
	if (verifyLength) {
		recordEnd = decoder.offset() + length
	}

	// Attributes (currently unused)
	decoder.readInt8()

	const timestampDelta = decoder.readVarInt()
	// Offset delta is still present in the stream; read and discard.
	decoder.readVarInt()

	// Key
	const keyLength = decoder.readVarInt()
	const key = keyLength < 0 ? null : decoder.readRaw(keyLength)

	// Value
	const valueLength = decoder.readVarInt()
	const value = valueLength < 0 ? null : decoder.readRaw(valueLength)

	// Headers (optimization: use shared empty array for common case)
	const headerCount = decoder.readVarInt()
	let headers: RecordHeader[]
	if (headerCount === 0) {
		headers = EMPTY_HEADERS
	} else {
		headers = new Array<RecordHeader>(headerCount)
		for (let i = 0; i < headerCount; i++) {
			const headerKeyLength = decoder.readVarInt()
			const headerKey = decoder.readRaw(headerKeyLength).toString('utf-8')

			const headerValueLength = decoder.readVarInt()
			const headerValue = headerValueLength < 0 ? null : decoder.readRaw(headerValueLength)

			headers[i] = { key: headerKey, value: headerValue }
		}
	}

	// Optional length verification (rarely needed outside tests/debugging)
	if (verifyLength) {
		const actualEnd = decoder.offset()
		if (actualEnd !== recordEnd) {
			throw new Error(`Record length mismatch: expected to end at ${recordEnd}, but ended at ${actualEnd}`)
		}
	}

	const timestamp = timestampDelta === 0 ? baseTimestamp : baseTimestamp + BigInt(timestampDelta)

	return {
		offset,
		timestamp,
		key,
		value,
		headers,
	}
}

/**
 * Helper to create a Record from a simple key-value pair
 *
 * @param key - The message key (string, Buffer, or null)
 * @param value - The message value (string, Buffer, or null)
 * @param headers - Optional headers
 * @param offsetDelta - Offset delta within the batch
 * @param timestampDelta - Timestamp delta from batch base timestamp
 * @returns A Record ready for encoding
 */
export function createRecord(
	key: string | Buffer | null,
	value: string | Buffer | null,
	headers: { [key: string]: string | Buffer | null } = {},
	offsetDelta: number = 0,
	timestampDelta: number = 0
): KafkaRecord {
	return {
		attributes: 0,
		timestampDelta,
		offsetDelta,
		key: key === null ? null : Buffer.isBuffer(key) ? key : Buffer.from(key, 'utf-8'),
		value: value === null ? null : Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf-8'),
		headers: Object.entries(headers).map(([k, v]) => ({
			key: k,
			value: v === null ? null : Buffer.isBuffer(v) ? v : Buffer.from(v, 'utf-8'),
		})),
	}
}
