import { describe, expect, it } from 'vitest'
import { codec } from '../../src/codec.js'

describe('codec', () => {
	it('encodes and decodes strings', () => {
		const c = codec.string()
		const buf = c.encode('hello')
		expect(buf).toBeInstanceOf(Buffer)
		expect(c.decode(buf)).toBe('hello')
	})

	it('encodes and decodes json', () => {
		const c = codec.json<{ id: string }>()
		const buf = c.encode({ id: 'a1' })
		expect(c.decode(buf)).toEqual({ id: 'a1' })
	})

	it('passes through buffer', () => {
		const c = codec.buffer()
		const buf = Buffer.from('hi')
		expect(c.decode(buf)).toBe(buf)
		expect(c.encode(buf)).toBe(buf)
	})
})
