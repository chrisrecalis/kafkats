import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
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

	it('decodes UVARINT values >= 2^31 as unsigned, not as a negative int32', () => {
		// 0xFFFFFFFF (4_294_967_295) encodes to these 5 bytes.
		const maxU32 = Buffer.from([0xff, 0xff, 0xff, 0xff, 0x0f])
		expect(decodeUVarInt(maxU32).value).toBe(4_294_967_295)
		expect(new Decoder(maxU32).readUVarInt()).toBe(4_294_967_295)

		// 2^31 exactly (0x80000000) — the boundary where a signed int32 flips negative.
		const twoPow31 = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x08])
		expect(decodeUVarInt(twoPow31).value).toBe(2_147_483_648)
		expect(new Decoder(twoPow31).readUVarInt()).toBe(2_147_483_648)
	})

	it('Decoder.readVarLong rejects a VARLONG that overflows 64 bits', () => {
		// 10 continuation bytes never terminate within 64 bits. Without a shift guard the
		// decoder over-reads past the value (failing later with a misleading underflow);
		// it should reject it as too long instead.
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => new Decoder(bytes).readVarLong()).toThrow('VARLONG is too long for 64-bit integer')
	})

	it('Decoder.readVarLong still round-trips valid 64-bit values (guard does not over-reject)', () => {
		const values = [0n, 1n, -1n, 2n ** 62n, -(2n ** 62n), 9223372036854775807n, -9223372036854775808n]
		for (const v of values) {
			expect(new Decoder(encodeVarLong(v)).readVarLong()).toBe(v)
		}
	})
})
