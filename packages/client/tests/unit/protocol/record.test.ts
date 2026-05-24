import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'
import {
	createRecord,
	decodeRecord,
	decodeRecordInBatch,
	encodeRecord,
	encodeRecordTo,
} from '@/protocol/records/record.js'

describe('record encoding', () => {
	it('encodes and decodes a record with headers', () => {
		const record = createRecord('k', 'v', { h1: 'x', h2: Buffer.from('y') }, 1, 2)
		const buffer = encodeRecord(record)
		const decoder = new Decoder(buffer)
		const decoded = decodeRecord(decoder, 10n, 100n)
		expect(decoded.offset).toBe(11n)
		expect(decoded.timestamp).toBe(102n)
		expect(decoded.key?.toString()).toBe('k')
		expect(decoded.value?.toString()).toBe('v')
		expect(decoded.headers).toEqual([
			{ key: 'h1', value: Buffer.from('x') },
			{ key: 'h2', value: Buffer.from('y') },
		])
	})

	it('supports null keys and values', () => {
		const record = createRecord(null, null)
		const buffer = encodeRecord(record)
		const decoder = new Decoder(buffer)
		const decoded = decodeRecord(decoder, 0n, 0n)
		expect(decoded.key).toBeNull()
		expect(decoded.value).toBeNull()
		expect(decoded.headers).toEqual([])
	})

	it('encodeRecordTo writes into an existing encoder', () => {
		const record = createRecord('k', 'v')
		const encoder = new Encoder()
		encodeRecordTo(encoder, record)
		const decoder = new Decoder(encoder.toBuffer())
		const decoded = decodeRecord(decoder, 0n, 0n)
		expect(decoded.key?.toString()).toBe('k')
		expect(decoded.value?.toString()).toBe('v')
	})

	it('throws when record length does not match payload', () => {
		const record = createRecord('k', 'v')
		const buffer = encodeRecord(record)
		const tampered = Buffer.from(buffer)
		// Force length prefix to zero to guarantee mismatch
		tampered[0] = 0
		const decoder = new Decoder(tampered)
		expect(() => decodeRecord(decoder, 0n, 0n)).toThrow('Record length mismatch')
	})

	it('preserves provided buffers without re-encoding', () => {
		const key = Buffer.from('k')
		const value = Buffer.from('v')
		const record = createRecord(key, value)
		const decoded = decodeRecord(new Decoder(encodeRecord(record)), 0n, 0n)
		expect(decoded.key).toEqual(key)
		expect(decoded.value).toEqual(value)
	})

	it('accepts header values set to null', () => {
		const record = createRecord('k', 'v', { h1: null })
		const decoded = decodeRecord(new Decoder(encodeRecord(record)), 0n, 0n)
		expect(decoded.headers).toEqual([{ key: 'h1', value: null }])
	})

	it('round-trips a timestampDelta larger than 32 bits (varlong)', () => {
		// 30 days in ms (2_592_000_000) exceeds the 32-bit zigzag range (2^31); the
		// RecordBatch v2 spec encodes timestampDelta as a varlong.
		const largeDelta = 30 * 24 * 60 * 60 * 1000
		expect(largeDelta).toBeGreaterThan(0x7fffffff)

		const record = createRecord('k', 'v', {}, 0, largeDelta)
		const decoded = decodeRecord(new Decoder(encodeRecord(record)), 0n, 1_700_000_000_000n)
		expect(decoded.timestamp).toBe(1_700_000_000_000n + BigInt(largeDelta))
	})

	it('round-trips a negative timestampDelta larger than 32 bits (out-of-order, varlong)', () => {
		const largeNegativeDelta = -(30 * 24 * 60 * 60 * 1000)
		const record = createRecord('k', 'v', {}, 0, largeNegativeDelta)
		const decoded = decodeRecord(new Decoder(encodeRecord(record)), 0n, 5_000_000_000_000n)
		expect(decoded.timestamp).toBe(5_000_000_000_000n + BigInt(largeNegativeDelta))
	})

	it('decodeRecordInBatch (fast path) handles a varlong timestampDelta', () => {
		const largeDelta = 40 * 24 * 60 * 60 * 1000
		const decoded = decodeRecordInBatch(
			new Decoder(encodeRecord(createRecord('k', 'v', {}, 0, largeDelta))),
			99n,
			1_000_000_000_000n
		)
		expect(decoded.offset).toBe(99n)
		expect(decoded.timestamp).toBe(1_000_000_000_000n + BigInt(largeDelta))
	})

	// Locks down wire compatibility: for int32-range deltas the varlong encoding is
	// byte-identical to the old varint encoding. 0x40000000 (2^30) is where the old
	// 32-bit zigzag first overflowed; int32 min/max are the boundaries.
	it.each([0, 1, -1, 12345, -12345, 0x3fffffff, 0x40000000, 0x7fffffff, -0x80000000])(
		'round-trips int32-range timestampDelta %d',
		delta => {
			const decoded = decodeRecord(new Decoder(encodeRecord(createRecord('k', 'v', {}, 0, delta))), 0n, 0n)
			expect(decoded.timestamp).toBe(BigInt(delta))
		}
	)
})
