import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { changeCodec } from '@/operators/change.js'

const changes = changeCodec(codec.string())
const bytes = (...parts: (string | number)[]) =>
	Buffer.concat(parts.map(part => (typeof part === 'string' ? Buffer.from(part) : Buffer.of(part))))

describe('changeCodec: the Kafka Streams Change format of the KTable.groupBy repartition topic (6.15)', () => {
	it('writes KS ChangedSerializer bytes: [old][0x00], [new][0x01], [zigzag varint newLength][new][old][0x02]', () => {
		expect(changes.encode({ newValue: null, oldValue: 'old' })).toEqual(bytes('old', 0))
		expect(changes.encode({ newValue: 'new', oldValue: null })).toEqual(bytes('new', 1))
		expect(changes.encode({ newValue: 'ab', oldValue: 'c' })).toEqual(bytes(0x04, 'ab', 'c', 2))
		// 64 bytes: zigzag 128, a two-byte varint.
		const long = 'x'.repeat(64)
		expect(changes.encode({ newValue: long, oldValue: '' })).toEqual(bytes(0x80, 0x01, long, 2))
	})

	it('reads back what it writes, empty values included', () => {
		for (const change of [
			{ newValue: null, oldValue: 'old' },
			{ newValue: 'new', oldValue: null },
			{ newValue: 'ab', oldValue: 'c' },
			{ newValue: '', oldValue: '' },
			{ newValue: 'y'.repeat(300), oldValue: 'z' },
		]) {
			expect(changes.decode(changes.encode(change))).toEqual(change)
		}
	})

	it('a change without values cannot be written; unknown flags and truncated values cannot be read', () => {
		expect(() => changes.encode({ newValue: null, oldValue: null })).toThrow('a Change needs a new or an old value')
		expect(() => changes.decode(bytes('v', 3))).toThrow('unknown encoding flag 3 of a Change')
		expect(() => changes.decode(bytes(0x08, 'ab', 2))).toThrow("a Change's new value has 4 bytes, 2 follow")
	})
})
