import { describe, expect, it } from 'vitest'

import { codec } from '@/codec.js'
import { normalizeDecoder, topic } from '@/topic.js'

describe('topic helpers', () => {
	it('normalizes decoder using decode', () => {
		const decoder = normalizeDecoder({ decode: (buffer: Buffer) => buffer.toString('utf-8') })
		expect(decoder(Buffer.from('x'))).toBe('x')
	})

	it('normalizes decoder using parse', () => {
		const decoder = normalizeDecoder({ parse: (buffer: Buffer) => buffer.toString('hex') })
		expect(decoder(Buffer.from('a'))).toBe('61')
	})

	it('throws when decoder lacks decode/parse', () => {
		expect(() => normalizeDecoder({} as never)).toThrow('Decoder must have a decode() or parse() method')
	})

	it('creates topic definitions', () => {
		const t = topic('name', { value: codec.string() })
		expect(t.topic).toBe('name')
		expect(typeof t.value?.encode).toBe('function')
	})

	it('accepts key/value codecs', () => {
		const t = topic('orders', { key: codec.string(), value: codec.json<{ id: string }>() })
		expect(t.key?.decode(Buffer.from('a'))).toBe('a')
		expect(t.value?.decode(Buffer.from('{"id":"1"}'))).toEqual({ id: '1' })
	})
})
