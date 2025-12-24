import { describe, expect, it } from 'vitest'

import {
	decodeUVarInt,
	decodeVarInt,
	decodeVarLong,
	encodeUVarInt,
	encodeVarInt,
	encodeVarLong,
	varIntSize,
	varLongSize,
	uvarIntSize,
	zigZagDecode32,
	zigZagDecode64,
	zigZagEncode32,
	zigZagEncode64,
} from '@/protocol/primitives/varint.js'

describe('varint helpers', () => {
	it('zigzag encodes and decodes 32-bit values', () => {
		const values = [0, -1, 1, -2, 2, 123456, -123456]
		for (const value of values) {
			const encoded = zigZagEncode32(value)
			const decoded = zigZagDecode32(encoded)
			expect(decoded).toBe(value)
		}
	})

	it('zigzag encodes and decodes 64-bit values', () => {
		const values = [0n, -1n, 1n, -2n, 2n, 9876543210n, -9876543210n]
		for (const value of values) {
			const encoded = zigZagEncode64(value)
			const decoded = zigZagDecode64(encoded)
			expect(decoded).toBe(value)
		}
	})

	it('roundtrips VARINT and VARLONG', () => {
		const intValues = [0, -1, 1, 12345, -12345, 0x3fffffff, -0x3fffffff]
		for (const value of intValues) {
			const encoded = encodeVarInt(value)
			const decoded = decodeVarInt(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(varIntSize(value))
		}

		const maxLong = BigInt('0x7fffffffffffffff')
		const longValues = [0n, -1n, 1n, 9876543210n, -9876543210n, maxLong, -maxLong]
		for (const value of longValues) {
			const encoded = encodeVarLong(value)
			const decoded = decodeVarLong(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(varLongSize(value))
		}
	})

	it('roundtrips UVARINT and sizes', () => {
		const values = [0, 1, 127, 128, 300, 16384, 0x7fffffff]
		for (const value of values) {
			const encoded = encodeUVarInt(value)
			const decoded = decodeUVarInt(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(uvarIntSize(value))
		}
	})

	it('rejects negative UVARINT', () => {
		expect(() => encodeUVarInt(-1)).toThrow('UVARINT cannot encode negative numbers')
	})

	it('fails decoding UVARINT when buffer underflows', () => {
		expect(() => decodeUVarInt(Buffer.alloc(0))).toThrow('Buffer underflow while reading UVARINT')
	})

	it('fails decoding VARLONG when buffer underflows', () => {
		expect(() => decodeVarLong(Buffer.alloc(0))).toThrow('Buffer underflow while reading VARLONG')
	})

	it('rejects UVARINT that is too long', () => {
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => decodeUVarInt(bytes)).toThrow('UVARINT is too long for 32-bit integer')
	})

	it('rejects VARLONG that is too long', () => {
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => decodeVarLong(bytes)).toThrow('VARLONG is too long for 64-bit integer')
	})
})
