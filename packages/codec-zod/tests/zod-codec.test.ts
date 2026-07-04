import { describe, it, expect } from 'vitest'
import { z } from 'zod'

import { zodCodec } from '../src/index.js'

describe('zodCodec', () => {
	it('round-trips values decoded through a schema with transforms', () => {
		const schema = z.string().transform(s => new Date(s))
		const codec = zodCodec(schema)

		const decoded = codec.decode(Buffer.from(JSON.stringify('2024-01-02T03:04:05.000Z')))
		expect(decoded).toBeInstanceOf(Date)
		expect(decoded.toISOString()).toBe('2024-01-02T03:04:05.000Z')

		// A consumed (output-typed) value must be re-encodable: pre-fix encode ran
		// schema.parse(value) on the input-typed schema and threw a ZodError.
		const encoded = codec.encode(decoded)
		const roundTripped = codec.decode(encoded)
		expect(roundTripped).toBeInstanceOf(Date)
		expect(roundTripped.toISOString()).toBe('2024-01-02T03:04:05.000Z')
	})

	it('round-trips schemas with defaults', () => {
		const schema = z.object({
			name: z.string(),
			retries: z.number().default(3),
		})
		const codec = zodCodec(schema)

		const decoded = codec.decode(Buffer.from(JSON.stringify({ name: 'job' })))
		expect(decoded).toEqual({ name: 'job', retries: 3 })

		const encoded = codec.encode(decoded)
		expect(codec.decode(encoded)).toEqual({ name: 'job', retries: 3 })
	})

	it('still validates on decode with a plain schema', () => {
		const schema = z.object({ id: z.string(), amount: z.number() })
		const codec = zodCodec(schema)

		const value = { id: 'a1', amount: 10 }
		expect(codec.decode(codec.encode(value))).toEqual(value)

		expect(() => codec.decode(Buffer.from(JSON.stringify({ id: 'a1', amount: 'oops' })))).toThrow()
	})

	it('supports custom encode/decode options', () => {
		const schema = z.object({ id: z.string() })
		const codec = zodCodec(schema, {
			encode: value => Buffer.from(`custom:${JSON.stringify(value)}`),
			decode: buffer => JSON.parse(buffer.toString('utf-8').slice('custom:'.length)),
		})

		const encoded = codec.encode({ id: 'a1' })
		expect(encoded.toString('utf-8')).toBe('custom:{"id":"a1"}')
		expect(codec.decode(encoded)).toEqual({ id: 'a1' })
	})
})
