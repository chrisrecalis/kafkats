import { describe, expect, it, vi } from 'vitest'

import {
	createRecordBatch,
	decodeRecordBatch,
	decodeRecordBatchFromSync,
	encodeRecordBatch,
	encodeRecordBatchSync,
	getCompressionType,
	isControlBatch,
	isLogAppendTime,
	isTransactional,
} from '@/protocol/records/record-batch.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

// Byte offset of the int32 recordCount field within an encoded record batch:
// baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) +
// attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) + maxTimestamp(8) +
// producerId(8) + producerEpoch(2) + baseSequence(4) = 57
const RECORD_COUNT_OFFSET = 57

function baseBatch() {
	return createRecordBatch([
		{ key: 'k1', value: 'v1' },
		{ key: 'k2', value: 'v2', headers: { h1: 'x' } },
	])
}

describe('record batch encoding', () => {
	it('encodes and decodes batches without compression', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch)
		const decoded = await decodeRecordBatch(encoded)
		expect(decoded.records).toHaveLength(2)
		expect(decoded.records[0]!.key?.toString()).toBe('k1')
		expect(decoded.records[1]!.headers[0]!.key).toBe('h1')
	})

	it('encodes and decodes with gzip compression', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch, { compression: CompressionType.Gzip })
		const decoded = await decodeRecordBatch(encoded)
		expect(decoded.records).toHaveLength(2)
		expect(decoded.records[0]!.value?.toString()).toBe('v1')
	})

	it('fails when compression codec is missing', async () => {
		const batch = baseBatch()
		await expect(encodeRecordBatch(batch, { compression: CompressionType.Lz4 })).rejects.toThrow(
			'Compression codec not registered'
		)
	})

	it('sets transaction and control flags in attributes', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch, { isTransactional: true, isControlBatch: true })
		const decoded = await decodeRecordBatch(encoded)
		expect(isTransactional(decoded.attributes)).toBe(true)
		expect(isControlBatch(decoded.attributes)).toBe(true)
	})

	it('marks LogAppendTime when requested', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch, { timestampType: 'LogAppendTime' })
		const decoded = await decodeRecordBatch(encoded)
		expect(isLogAppendTime(decoded.attributes)).toBe(true)
	})

	it('computes compression type from attributes', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch, { compression: CompressionType.Gzip })
		const decoded = await decodeRecordBatch(encoded)
		expect(getCompressionType(decoded.attributes)).toBe(CompressionType.Gzip)
	})

	it('throws on invalid magic', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch)
		const tampered = Buffer.from(encoded)
		// magic is after baseOffset (8) + batchLength (4) + leaderEpoch (4)
		tampered[16] = 1
		await expect(decodeRecordBatch(tampered)).rejects.toThrow('Unsupported record batch magic')
	})

	it('throws on CRC mismatch', async () => {
		const batch = baseBatch()
		const encoded = await encodeRecordBatch(batch)
		const tampered = Buffer.from(encoded)
		// flip a byte near the end
		tampered[tampered.length - 1]! ^= 0xff
		await expect(decodeRecordBatch(tampered)).rejects.toThrow('CRC mismatch')
	})

	it('rejects a negative record count instead of throwing a raw RangeError', () => {
		const encoded = encodeRecordBatchSync(baseBatch())
		const tampered = Buffer.from(encoded)
		tampered.writeInt32BE(-1, RECORD_COUNT_OFFSET)

		// verifyCrc:false so we reach the record-count validation rather than the CRC check.
		expect(() => decodeRecordBatchFromSync(new Decoder(tampered), { verifyCrc: false })).toThrow(
			'Invalid record count'
		)
	})

	it('rejects an oversized record count that exceeds the record data size', () => {
		const encoded = encodeRecordBatchSync(baseBatch())
		const tampered = Buffer.from(encoded)
		tampered.writeInt32BE(0x7fffffff, RECORD_COUNT_OFFSET)

		expect(() => decodeRecordBatchFromSync(new Decoder(tampered), { verifyCrc: false })).toThrow(
			'Invalid record count'
		)
	})

	it('createRecordBatch uses provided base timestamp', () => {
		vi.useFakeTimers()
		vi.setSystemTime(new Date('2024-01-01T00:00:00Z'))
		const batch = createRecordBatch([{ value: 'v' }], 0n, 100n)
		expect(batch.baseTimestamp).toBe(100n)
		vi.useRealTimers()
	})
})
