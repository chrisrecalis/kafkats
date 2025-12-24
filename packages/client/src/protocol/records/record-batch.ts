/**
 * RecordBatch v2 format (magic=2) for Kafka 3.x
 *
 * RecordBatch layout:
 * - baseOffset: int64
 * - batchLength: int32 (from partitionLeaderEpoch to end of records)
 * - partitionLeaderEpoch: int32
 * - magic: int8 (must be 2)
 * - crc: int32 (CRC32C from attributes to end)
 * - attributes: int16
 * - lastOffsetDelta: int32
 * - baseTimestamp: int64
 * - maxTimestamp: int64
 * - producerId: int64
 * - producerEpoch: int16
 * - baseSequence: int32
 * - recordCount: int32
 * - records: [Record] (optionally compressed)
 */

import { Encoder, Decoder } from '@/protocol/primitives/index.js'
import { crc32c } from '@/utils/crc32c.js'
import {
	encodeRecord,
	decodeRecord,
	decodeRecordInBatch,
	type KafkaRecord,
	type DecodedRecord,
} from '@/protocol/records/record.js'
import { CompressionType, compressionCodecs, getCompressionTypeName } from '@/protocol/records/compression.js'

// Attribute bit masks
export const COMPRESSION_CODEC_MASK = 0x07 // bits 0-2
export const TIMESTAMP_TYPE_MASK = 0x08 // bit 3: 0=CreateTime, 1=LogAppendTime
export const IS_TRANSACTIONAL_MASK = 0x10 // bit 4
export const IS_CONTROL_BATCH_MASK = 0x20 // bit 5
export const HAS_DELETE_HORIZON_MASK = 0x40 // bit 6

/**
 * RecordBatch structure
 */
export interface RecordBatch {
	baseOffset: bigint
	partitionLeaderEpoch: number
	magic: number // Must be 2
	attributes: number
	lastOffsetDelta: number
	baseTimestamp: bigint
	maxTimestamp: bigint
	producerId: bigint
	producerEpoch: number
	baseSequence: number
	records: KafkaRecord[]
}

/**
 * Decoded RecordBatch with resolved records
 */
export interface DecodedRecordBatch {
	baseOffset: bigint
	partitionLeaderEpoch: number
	attributes: number
	lastOffsetDelta: number
	baseTimestamp: bigint
	maxTimestamp: bigint
	producerId: bigint
	producerEpoch: number
	baseSequence: number
	records: DecodedRecord[]
}

/**
 * Options for encoding a record batch
 */
export interface EncodeRecordBatchOptions {
	compression?: CompressionType
	isTransactional?: boolean
	isControlBatch?: boolean
	timestampType?: 'CreateTime' | 'LogAppendTime'
}

/**
 * Options for decoding a record batch
 */
export interface DecodeRecordBatchOptions {
	/**
	 * Verify the CRC32C contained in the record batch.
	 *
	 * KafkaJS does not currently validate CRCs; disabling CRC verification can
	 * significantly improve consumer throughput for large batches.
	 *
	 * @default true
	 */
	verifyCrc?: boolean
	/**
	 * Validate record-length bookkeeping while decoding.
	 *
	 * @default true
	 */
	verifyRecordLength?: boolean
	/**
	 * Assume record offsets are sequential within the batch.
	 *
	 * This avoids per-record BigInt conversions for offset deltas and is safe for
	 * Kafka-produced batches.
	 *
	 * @default false
	 */
	assumeSequentialOffsets?: boolean
}

/**
 * Encode a RecordBatch to a buffer (synchronous version for no compression)
 *
 * @param batch - The batch to encode
 * @param options - Encoding options (compression must be None)
 * @returns The encoded batch as a buffer
 */
export function encodeRecordBatchSync(batch: RecordBatch, options: EncodeRecordBatchOptions = {}): Buffer {
	const compression = options.compression ?? CompressionType.None
	if (compression !== CompressionType.None) {
		throw new Error('encodeRecordBatchSync cannot be used with compression')
	}

	const recordsEncoder = new Encoder()
	for (const record of batch.records) {
		recordsEncoder.writeRaw(encodeRecord(record))
	}
	const recordsBuffer = recordsEncoder.toBuffer()

	let attributes = batch.attributes & ~COMPRESSION_CODEC_MASK
	if (options.isTransactional) {
		attributes |= IS_TRANSACTIONAL_MASK
	}
	if (options.isControlBatch) {
		attributes |= IS_CONTROL_BATCH_MASK
	}
	if (options.timestampType === 'LogAppendTime') {
		attributes |= TIMESTAMP_TYPE_MASK
	}

	// CRC covers everything from attributes to end of records
	const crcEncoder = new Encoder()
	crcEncoder.writeInt16(attributes)
	crcEncoder.writeInt32(batch.lastOffsetDelta)
	crcEncoder.writeInt64(batch.baseTimestamp)
	crcEncoder.writeInt64(batch.maxTimestamp)
	crcEncoder.writeInt64(batch.producerId)
	crcEncoder.writeInt16(batch.producerEpoch)
	crcEncoder.writeInt32(batch.baseSequence)
	crcEncoder.writeInt32(batch.records.length)
	crcEncoder.writeRaw(recordsBuffer)

	const crcData = crcEncoder.toBuffer()
	const crcValue = crc32c(crcData)

	const batchLength = 4 + 1 + 4 + crcData.length // partitionLeaderEpoch + magic + crc + data

	const encoder = new Encoder(8 + 4 + batchLength)
	encoder.writeInt64(batch.baseOffset)
	encoder.writeInt32(batchLength)
	encoder.writeInt32(batch.partitionLeaderEpoch)
	encoder.writeInt8(batch.magic)
	encoder.writeUInt32(crcValue) // CRC as unsigned
	encoder.writeRaw(crcData)

	return encoder.toBuffer()
}

/**
 * Encode a RecordBatch to a buffer
 *
 * @param batch - The batch to encode
 * @param options - Encoding options
 * @returns The encoded batch as a buffer
 */
export async function encodeRecordBatch(batch: RecordBatch, options: EncodeRecordBatchOptions = {}): Promise<Buffer> {
	const compression = options.compression ?? CompressionType.None

	// Fast path: no compression, use synchronous encoding
	if (compression === CompressionType.None) {
		return encodeRecordBatchSync(batch, options)
	}

	const recordsEncoder = new Encoder()
	for (const record of batch.records) {
		recordsEncoder.writeRaw(encodeRecord(record))
	}
	let recordsBuffer = recordsEncoder.toBuffer()

	let attributes = batch.attributes & ~COMPRESSION_CODEC_MASK
	attributes |= compression & COMPRESSION_CODEC_MASK
	if (options.isTransactional) {
		attributes |= IS_TRANSACTIONAL_MASK
	}
	if (options.isControlBatch) {
		attributes |= IS_CONTROL_BATCH_MASK
	}
	if (options.timestampType === 'LogAppendTime') {
		attributes |= TIMESTAMP_TYPE_MASK
	}

	const codec = compressionCodecs.get(compression)
	if (!codec) {
		throw new Error(`Compression codec not registered: ${getCompressionTypeName(compression)}`)
	}
	recordsBuffer = await codec.compress(recordsBuffer)

	// CRC covers everything from attributes to end of records
	const crcEncoder = new Encoder()
	crcEncoder.writeInt16(attributes)
	crcEncoder.writeInt32(batch.lastOffsetDelta)
	crcEncoder.writeInt64(batch.baseTimestamp)
	crcEncoder.writeInt64(batch.maxTimestamp)
	crcEncoder.writeInt64(batch.producerId)
	crcEncoder.writeInt16(batch.producerEpoch)
	crcEncoder.writeInt32(batch.baseSequence)
	crcEncoder.writeInt32(batch.records.length)
	crcEncoder.writeRaw(recordsBuffer)

	const crcData = crcEncoder.toBuffer()
	const crcValue = crc32c(crcData)

	const batchLength = 4 + 1 + 4 + crcData.length // partitionLeaderEpoch + magic + crc + data

	const encoder = new Encoder(8 + 4 + batchLength)
	encoder.writeInt64(batch.baseOffset)
	encoder.writeInt32(batchLength)
	encoder.writeInt32(batch.partitionLeaderEpoch)
	encoder.writeInt8(batch.magic)
	encoder.writeUInt32(crcValue) // CRC as unsigned
	encoder.writeRaw(crcData)

	return encoder.toBuffer()
}

/**
 * Decode a RecordBatch from a buffer
 *
 * @param buffer - The buffer containing the batch
 * @returns The decoded batch
 */
export async function decodeRecordBatch(buffer: Buffer): Promise<DecodedRecordBatch> {
	const decoder = new Decoder(buffer)
	return decodeRecordBatchFrom(decoder)
}

/**
 * Decode a RecordBatch from a decoder (synchronous version for no compression)
 *
 * @param decoder - The decoder to read from
 * @returns The decoded batch
 * @throws If compression is used
 */
export function decodeRecordBatchFromSync(
	decoder: Decoder,
	options: DecodeRecordBatchOptions = {}
): DecodedRecordBatch {
	const baseOffset = decoder.readInt64()
	const batchLength = decoder.readInt32()
	const batchEnd = decoder.offset() + batchLength

	const partitionLeaderEpoch = decoder.readInt32()
	const magic = decoder.readInt8()

	if (magic !== 2) {
		throw new Error(`Unsupported record batch magic: ${magic}, expected 2`)
	}

	const storedCrc = decoder.readUInt32()

	const crcStart = decoder.offset()
	const crcDataLength = batchEnd - crcStart
	const crcData = decoder.readRaw(crcDataLength)

	if (options.verifyCrc !== false) {
		const computedCrc = crc32c(crcData)
		if (storedCrc !== computedCrc) {
			throw new Error(`CRC mismatch: stored=${storedCrc.toString(16)}, computed=${computedCrc.toString(16)}`)
		}
	}

	const crcDecoder = new Decoder(crcData)

	const attributes = crcDecoder.readInt16()
	const lastOffsetDelta = crcDecoder.readInt32()
	const baseTimestamp = crcDecoder.readInt64()
	const maxTimestamp = crcDecoder.readInt64()
	const producerId = crcDecoder.readInt64()
	const producerEpoch = crcDecoder.readInt16()
	const baseSequence = crcDecoder.readInt32()
	const recordCount = crcDecoder.readInt32()

	const recordsBuffer = crcDecoder.readRaw(crcDecoder.remaining())

	const compressionType = (attributes & COMPRESSION_CODEC_MASK) as CompressionType
	if (compressionType !== CompressionType.None) {
		throw new Error('decodeRecordBatchFromSync cannot decode compressed batches')
	}

	const recordsDecoder = new Decoder(recordsBuffer)
	const records = new Array<DecodedRecord>(recordCount)
	if (options.assumeSequentialOffsets) {
		const verifyLength = options.verifyRecordLength !== false
		let offset = baseOffset
		for (let i = 0; i < recordCount; i++) {
			records[i] = decodeRecordInBatch(recordsDecoder, offset, baseTimestamp, { verifyLength })
			offset += 1n
		}
	} else {
		for (let i = 0; i < recordCount; i++) {
			records[i] = decodeRecord(recordsDecoder, baseOffset, baseTimestamp)
		}
	}

	return {
		baseOffset,
		partitionLeaderEpoch,
		attributes,
		lastOffsetDelta,
		baseTimestamp,
		maxTimestamp,
		producerId,
		producerEpoch,
		baseSequence,
		records,
	}
}

/**
 * Decode a RecordBatch from a decoder
 *
 * @param decoder - The decoder to read from
 * @returns The decoded batch
 */
export async function decodeRecordBatchFrom(
	decoder: Decoder,
	options: DecodeRecordBatchOptions = {}
): Promise<DecodedRecordBatch> {
	const baseOffset = decoder.readInt64()
	const batchLength = decoder.readInt32()
	const batchEnd = decoder.offset() + batchLength

	const partitionLeaderEpoch = decoder.readInt32()
	const magic = decoder.readInt8()

	if (magic !== 2) {
		throw new Error(`Unsupported record batch magic: ${magic}, expected 2`)
	}

	const storedCrc = decoder.readUInt32()

	const crcStart = decoder.offset()
	const crcDataLength = batchEnd - crcStart
	const crcData = decoder.readRaw(crcDataLength)

	if (options.verifyCrc !== false) {
		const computedCrc = crc32c(crcData)
		if (storedCrc !== computedCrc) {
			throw new Error(`CRC mismatch: stored=${storedCrc.toString(16)}, computed=${computedCrc.toString(16)}`)
		}
	}

	const crcDecoder = new Decoder(crcData)

	const attributes = crcDecoder.readInt16()
	const lastOffsetDelta = crcDecoder.readInt32()
	const baseTimestamp = crcDecoder.readInt64()
	const maxTimestamp = crcDecoder.readInt64()
	const producerId = crcDecoder.readInt64()
	const producerEpoch = crcDecoder.readInt16()
	const baseSequence = crcDecoder.readInt32()
	const recordCount = crcDecoder.readInt32()

	let recordsBuffer = crcDecoder.readRaw(crcDecoder.remaining())

	const compressionType = (attributes & COMPRESSION_CODEC_MASK) as CompressionType
	if (compressionType !== CompressionType.None) {
		const codec = compressionCodecs.get(compressionType)
		if (!codec) {
			throw new Error(`Compression codec not registered: ${getCompressionTypeName(compressionType)}`)
		}
		recordsBuffer = await codec.decompress(recordsBuffer)
	}

	const recordsDecoder = new Decoder(recordsBuffer)
	const records = new Array<DecodedRecord>(recordCount)
	if (options.assumeSequentialOffsets) {
		const verifyLength = options.verifyRecordLength !== false
		let offset = baseOffset
		for (let i = 0; i < recordCount; i++) {
			records[i] = decodeRecordInBatch(recordsDecoder, offset, baseTimestamp, { verifyLength })
			offset += 1n
		}
	} else {
		for (let i = 0; i < recordCount; i++) {
			records[i] = decodeRecord(recordsDecoder, baseOffset, baseTimestamp)
		}
	}

	return {
		baseOffset,
		partitionLeaderEpoch,
		attributes,
		lastOffsetDelta,
		baseTimestamp,
		maxTimestamp,
		producerId,
		producerEpoch,
		baseSequence,
		records,
	}
}

/**
 * Create a RecordBatch from a list of simple messages
 *
 * @param messages - Array of {key, value, headers} objects
 * @param baseOffset - Base offset for the batch (default: 0)
 * @param baseTimestamp - Base timestamp (default: now)
 * @param producerId - Producer ID (default: -1 for non-idempotent)
 * @param producerEpoch - Producer epoch (default: -1)
 * @param baseSequence - Base sequence number (default: -1)
 * @returns A RecordBatch ready for encoding
 */
export function createRecordBatch(
	messages: Array<{
		key?: string | Buffer | null
		value: string | Buffer | null
		headers?: { [key: string]: string | Buffer | null }
		timestamp?: number
	}>,
	baseOffset: bigint = 0n,
	baseTimestamp?: bigint,
	producerId: bigint = -1n,
	producerEpoch: number = -1,
	baseSequence: number = -1
): RecordBatch {
	const now = BigInt(Date.now())
	const actualBaseTimestamp = baseTimestamp ?? now

	let maxTimestamp = actualBaseTimestamp
	const records: KafkaRecord[] = []

	for (let i = 0; i < messages.length; i++) {
		const msg = messages[i]!
		const msgTimestamp = msg.timestamp !== undefined ? BigInt(msg.timestamp) : now
		const timestampDelta = Number(msgTimestamp - actualBaseTimestamp)

		if (msgTimestamp > maxTimestamp) {
			maxTimestamp = msgTimestamp
		}

		records.push({
			attributes: 0,
			timestampDelta,
			offsetDelta: i,
			key:
				msg.key === undefined || msg.key === null
					? null
					: Buffer.isBuffer(msg.key)
						? msg.key
						: Buffer.from(msg.key, 'utf-8'),
			value: msg.value === null ? null : Buffer.isBuffer(msg.value) ? msg.value : Buffer.from(msg.value, 'utf-8'),
			headers: Object.entries(msg.headers ?? {}).map(([k, v]) => ({
				key: k,
				value: v === null ? null : Buffer.isBuffer(v) ? v : Buffer.from(v, 'utf-8'),
			})),
		})
	}

	return {
		baseOffset,
		partitionLeaderEpoch: 0,
		magic: 2,
		attributes: 0,
		lastOffsetDelta: records.length - 1,
		baseTimestamp: actualBaseTimestamp,
		maxTimestamp,
		producerId,
		producerEpoch,
		baseSequence,
		records,
	}
}

/**
 * Check if a batch is transactional
 */
export function isTransactional(attributes: number): boolean {
	return (attributes & IS_TRANSACTIONAL_MASK) !== 0
}

/**
 * Check if a batch is a control batch
 */
export function isControlBatch(attributes: number): boolean {
	return (attributes & IS_CONTROL_BATCH_MASK) !== 0
}

/**
 * Get the compression type from attributes
 */
export function getCompressionType(attributes: number): CompressionType {
	return (attributes & COMPRESSION_CODEC_MASK) as CompressionType
}

/**
 * Check if timestamp type is LogAppendTime
 */
export function isLogAppendTime(attributes: number): boolean {
	return (attributes & TIMESTAMP_TYPE_MASK) !== 0
}
