import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { listCodec } from '@/operators/list-value.js'

const hex = (...parts: string[]) => Buffer.from(parts.join(''), 'hex')

describe('listCodec', () => {
	const strings = listCodec(codec.string())

	it('writes [u32 count]([u32 length][bytes])* in order', () => {
		expect(strings.encode(['a', 'bc'])).toEqual(hex('00000002', '00000001', '61', '00000002', '6263'))
		expect(strings.encode([])).toEqual(hex('00000000'))
	})

	it('reads back what it wrote, empty values and duplicates included', () => {
		for (const values of [[], [''], ['x', '', 'x'], ['é', 'longer value']]) {
			expect(strings.decode(strings.encode(values))).toEqual(values)
		}
	})
})
