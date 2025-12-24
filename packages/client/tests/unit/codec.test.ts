import { describe, expect, it } from 'vitest'

import { buffer, codec, json, string } from '@/codec.js'

describe('codec', () => {
	describe('string()', () => {
		it('encodes a string to a UTF-8 buffer', () => {
			const c = string()
			const encoded = c.encode('hello')
			expect(Buffer.isBuffer(encoded)).toBe(true)
			expect(encoded.toString('utf-8')).toBe('hello')
		})

		it('decodes a UTF-8 buffer to a string', () => {
			const c = string()
			const decoded = c.decode(Buffer.from('world', 'utf-8'))
			expect(decoded).toBe('world')
		})

		it('handles empty strings', () => {
			const c = string()
			const encoded = c.encode('')
			expect(encoded.length).toBe(0)
			expect(c.decode(encoded)).toBe('')
		})

		it('handles unicode characters', () => {
			const c = string()
			const unicode = 'Hello 世界 🌍'
			const encoded = c.encode(unicode)
			expect(c.decode(encoded)).toBe(unicode)
		})

		it('handles multi-byte UTF-8 sequences', () => {
			const c = string()
			const text = 'café résumé naïve'
			expect(c.decode(c.encode(text))).toBe(text)
		})
	})

	describe('json()', () => {
		it('encodes an object to a JSON buffer', () => {
			const c = json<{ name: string }>()
			const encoded = c.encode({ name: 'test' })
			expect(Buffer.isBuffer(encoded)).toBe(true)
			expect(encoded.toString('utf-8')).toBe('{"name":"test"}')
		})

		it('decodes a JSON buffer to an object', () => {
			const c = json<{ id: number; active: boolean }>()
			const decoded = c.decode(Buffer.from('{"id":42,"active":true}'))
			expect(decoded).toEqual({ id: 42, active: true })
		})

		it('handles arrays', () => {
			const c = json<number[]>()
			const arr = [1, 2, 3]
			expect(c.decode(c.encode(arr))).toEqual(arr)
		})

		it('handles nested objects', () => {
			const c = json<{ user: { name: string; tags: string[] } }>()
			const obj = { user: { name: 'Alice', tags: ['admin', 'user'] } }
			expect(c.decode(c.encode(obj))).toEqual(obj)
		})

		it('handles null values', () => {
			const c = json<null>()
			expect(c.decode(c.encode(null))).toBeNull()
		})

		it('handles primitive values', () => {
			const numCodec = json<number>()
			expect(numCodec.decode(numCodec.encode(123))).toBe(123)

			const boolCodec = json<boolean>()
			expect(boolCodec.decode(boolCodec.encode(true))).toBe(true)

			const strCodec = json<string>()
			expect(strCodec.decode(strCodec.encode('test'))).toBe('test')
		})

		it('throws on invalid JSON during decode', () => {
			const c = json<unknown>()
			expect(() => c.decode(Buffer.from('not json'))).toThrow()
		})
	})

	describe('buffer()', () => {
		it('returns the same buffer on encode', () => {
			const c = buffer()
			const buf = Buffer.from([1, 2, 3, 4])
			const encoded = c.encode(buf)
			expect(encoded).toBe(buf)
		})

		it('returns the same buffer on decode', () => {
			const c = buffer()
			const buf = Buffer.from([5, 6, 7, 8])
			const decoded = c.decode(buf)
			expect(decoded).toBe(buf)
		})

		it('handles empty buffers', () => {
			const c = buffer()
			const empty = Buffer.alloc(0)
			expect(c.encode(empty)).toBe(empty)
			expect(c.decode(empty)).toBe(empty)
		})

		it('preserves binary data', () => {
			const c = buffer()
			const binary = Buffer.from([0x00, 0xff, 0x7f, 0x80])
			expect(c.decode(c.encode(binary))).toEqual(binary)
		})
	})

	describe('codec namespace', () => {
		it('exports string codec factory', () => {
			expect(codec.string).toBe(string)
		})

		it('exports json codec factory', () => {
			expect(codec.json).toBe(json)
		})

		it('exports buffer codec factory', () => {
			expect(codec.buffer).toBe(buffer)
		})
	})
})
