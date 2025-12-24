import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

function encodeSample(): Buffer {
	const encoder = new Encoder(4)
	encoder.writeInt8(-1)
	encoder.writeInt16(1234)
	encoder.writeInt32(-123456)
	encoder.writeInt64(123456789n)
	encoder.writeUInt32(0xdeadbeef)
	encoder.writeVarInt(-12345)
	encoder.writeVarLong(-9876543210n)
	encoder.writeUVarInt(300)
	encoder.writeString('hello')
	encoder.writeNullableString(null)
	encoder.writeNullableString('world')
	encoder.writeCompactString('hi')
	encoder.writeCompactNullableString(null)
	encoder.writeCompactNullableString('there')
	encoder.writeBytes(Buffer.from([1, 2, 3]))
	encoder.writeNullableBytes(null)
	encoder.writeCompactBytes(Buffer.from([4, 5]))
	encoder.writeCompactNullableBytes(null)
	encoder.writeUUID('00112233-4455-6677-8899-aabbccddeeff')
	encoder.writeBoolean(true)
	encoder.writeArray([1, 2, 3], (value, enc) => enc.writeInt32(value))
	encoder.writeNullableArray<number>(null, (value, enc) => enc.writeInt32(value))
	encoder.writeCompactArray([7, 8], (value, enc) => enc.writeInt16(value))
	encoder.writeCompactNullableArray<number>(null, (value, enc) => enc.writeInt16(value))
	encoder.writeTaggedFields([
		{ tag: 2, data: Buffer.from('bb') },
		{ tag: 1, data: Buffer.from('aa') },
	])
	return encoder.toBuffer()
}

describe('Encoder/Decoder roundtrip', () => {
	it('encodes and decodes supported primitives', () => {
		const buffer = encodeSample()
		const decoder = new Decoder(buffer)

		expect(decoder.readInt8()).toBe(-1)
		expect(decoder.readInt16()).toBe(1234)
		expect(decoder.readInt32()).toBe(-123456)
		expect(decoder.readInt64()).toBe(123456789n)
		expect(decoder.readUInt32()).toBe(0xdeadbeef)
		expect(decoder.readVarInt()).toBe(-12345)
		expect(decoder.readVarLong()).toBe(-9876543210n)
		expect(decoder.readUVarInt()).toBe(300)
		expect(decoder.readString()).toBe('hello')
		expect(decoder.readNullableString()).toBeNull()
		expect(decoder.readNullableString()).toBe('world')
		expect(decoder.readCompactString()).toBe('hi')
		expect(decoder.readCompactNullableString()).toBeNull()
		expect(decoder.readCompactNullableString()).toBe('there')
		expect(decoder.readBytes()).toEqual(Buffer.from([1, 2, 3]))
		expect(decoder.readNullableBytes()).toBeNull()
		expect(decoder.readCompactBytes()).toEqual(Buffer.from([4, 5]))
		expect(decoder.readCompactNullableBytes()).toBeNull()
		expect(decoder.readUUID()).toBe('00112233-4455-6677-8899-aabbccddeeff')
		expect(decoder.readBoolean()).toBe(true)
		expect(decoder.readArray(dec => dec.readInt32())).toEqual([1, 2, 3])
		expect(decoder.readNullableArray(dec => dec.readInt32())).toBeNull()
		expect(decoder.readCompactArray(dec => dec.readInt16())).toEqual([7, 8])
		expect(decoder.readCompactNullableArray(dec => dec.readInt16())).toBeNull()

		const tagged = decoder.readTaggedFields()
		expect(tagged.map(field => field.tag)).toEqual([1, 2])
		expect(tagged[0]?.data.toString()).toBe('aa')
		expect(tagged[1]?.data.toString()).toBe('bb')
		expect(decoder.remaining()).toBe(0)
	})

	it('grows buffers as needed', () => {
		const encoder = new Encoder(1)
		encoder.writeInt32(1)
		encoder.writeInt32(2)
		encoder.writeInt32(3)
		const buffer = encoder.toBuffer()
		expect(buffer.length).toBe(12)
		const decoder = new Decoder(buffer)
		expect(decoder.readInt32()).toBe(1)
		expect(decoder.readInt32()).toBe(2)
		expect(decoder.readInt32()).toBe(3)
	})

	it('throws on invalid UUID format', () => {
		const encoder = new Encoder()
		expect(() => encoder.writeUUID('invalid')).toThrow('Invalid UUID format')
	})

	it('rejects null in non-nullable string', () => {
		const encoder = new Encoder()
		encoder.writeInt16(-1)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readString()).toThrow('Unexpected null in non-nullable string')
	})

	it('rejects null in non-nullable compact string', () => {
		const encoder = new Encoder()
		encoder.writeUVarInt(0)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readCompactString()).toThrow('Unexpected null in non-nullable compact string')
	})

	it('rejects null in non-nullable bytes', () => {
		const encoder = new Encoder()
		encoder.writeInt32(-1)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readBytes()).toThrow('Unexpected null in non-nullable bytes')
	})

	it('rejects null in non-nullable compact bytes', () => {
		const encoder = new Encoder()
		encoder.writeUVarInt(0)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readCompactBytes()).toThrow('Unexpected null in non-nullable compact bytes')
	})

	it('rejects null in non-nullable arrays', () => {
		const encoder = new Encoder()
		encoder.writeInt32(-1)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readArray(dec => dec.readInt8())).toThrow('Unexpected null in non-nullable array')
	})

	it('rejects null in non-nullable compact arrays', () => {
		const encoder = new Encoder()
		encoder.writeUVarInt(0)
		const decoder = new Decoder(encoder.toBuffer())
		expect(() => decoder.readCompactArray(dec => dec.readInt8())).toThrow(
			'Unexpected null in non-nullable compact array'
		)
	})

	it('can skip tagged fields', () => {
		const encoder = new Encoder()
		encoder.writeTaggedFields([{ tag: 1, data: Buffer.from([9, 9]) }])
		encoder.writeInt8(7)
		const decoder = new Decoder(encoder.toBuffer())
		decoder.skipTaggedFields()
		expect(decoder.readInt8()).toBe(7)
	})

	it('guards invalid seek and skip', () => {
		const decoder = new Decoder(Buffer.from([1, 2, 3]))
		expect(() => decoder.seek(4)).toThrow('Invalid seek position')
		expect(() => decoder.skip(4)).toThrow('Buffer underflow')
	})
})
