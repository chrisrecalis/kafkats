import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'
import { createRecord, decodeRecord, encodeRecord, encodeRecordTo } from '@/protocol/records/record.js'

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
})
