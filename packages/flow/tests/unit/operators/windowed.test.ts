import { murmur2, toPositive } from '@kafkats/client'
import { describe, expect, it } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import { codec, type Codec } from '@/codec.js'
import { isWindowedCodec, type WindowedCodec, WindowedCodecs, windowedPartitioner } from '@/operators/windowed.js'

const str = codec.string()
const hex = (...parts: string[]) => Buffer.from(parts.join(''), 'hex')
const windowed = (key: string, start: number, end: number): Windowed<string> => ({ key, window: { start, end } })

describe('WindowedCodecs.timeWindowed', () => {
	const codec = WindowedCodecs.timeWindowed(str, 100)

	it('writes [key][start int64BE] (KS TimeWindowedSerializer)', () => {
		expect(codec.encode(windowed('a', 1000, 1100))).toEqual(hex('61', '00000000000003e8'))
		expect(codec.encode(windowed('', -1, 99))).toEqual(hex('ffffffffffffffff'))
	})

	it('reads the end as start + window size (KS TimeWindowedDeserializer)', () => {
		expect(codec.decode(hex('6b6579', '00000000000003e8'))).toEqual(windowed('key', 1000, 1100))
		expect(codec.decode(codec.encode(windowed('a', 0, 999)))).toEqual(windowed('a', 0, 100))
	})

	it('clamps the end to Number.MAX_SAFE_INTEGER (KS clamps to Long.MAX_VALUE)', () => {
		const start = Number.MAX_SAFE_INTEGER - 10
		expect(codec.decode(codec.encode(windowed('a', start, 0)))).toEqual(
			windowed('a', start, Number.MAX_SAFE_INTEGER)
		)
	})

	it('takes windows of size 0 (sliding windows with no time difference)', () => {
		const zero = WindowedCodecs.timeWindowed(str, 0)
		expect(zero.decode(zero.encode(windowed('a', 5, 5)))).toEqual(windowed('a', 5, 5))
	})

	it.each([-1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1])('rejects window size %s', size => {
		expect(() => WindowedCodecs.timeWindowed(str, size)).toThrow(RangeError)
	})

	it('rejects a key shorter than its window start', () => {
		expect(() => codec.decode(hex('00000000000000'))).toThrow(RangeError)
	})
})

describe('WindowedCodecs.sessionWindowed', () => {
	const codec = WindowedCodecs.sessionWindowed(str)

	it('writes [key][end int64BE][start int64BE] (KS SessionWindowedSerializer)', () => {
		expect(codec.encode(windowed('a', 100, 200))).toEqual(hex('61', '00000000000000c8', '0000000000000064'))
	})

	it('reads what it writes, including empty keys and negative times', () => {
		for (const session of [windowed('key', 100, 200), windowed('', -5, -5), windowed('a', 0, 0)]) {
			expect(codec.decode(codec.encode(session))).toEqual(session)
		}
		expect(codec.decode(hex('6b', '00000000000000c8', '0000000000000064'))).toEqual(windowed('k', 100, 200))
	})

	it('rejects a key shorter than its session bounds', () => {
		expect(() => codec.decode(hex('00000000000000c8', '00000000000064'))).toThrow(RangeError)
	})
})

describe('isWindowedCodec', () => {
	it('recognizes both factories and other implementations of WindowedCodec, nothing else', () => {
		const custom: WindowedCodec<string> = {
			inner: str,
			kind: 'time',
			encode: ({ key }) => str.encode(key),
			decode: buffer => windowed(str.decode(buffer), 0, 0),
		}
		expect(isWindowedCodec(WindowedCodecs.timeWindowed(str, 10))).toBe(true)
		expect(isWindowedCodec(WindowedCodecs.sessionWindowed(str))).toBe(true)
		expect(isWindowedCodec(custom as Codec<unknown>)).toBe(true)
		expect(isWindowedCodec(str as Codec<unknown>)).toBe(false)
		expect(isWindowedCodec(undefined)).toBe(false)
	})
})

describe('windowedPartitioner (KS WindowedStreamPartitioner)', () => {
	const basePartition = (key: string, count: number) => toPositive(murmur2(Buffer.from(key))) % count

	it('picks the partition of the base key, whatever the window', () => {
		for (const codec of [WindowedCodecs.timeWindowed(str, 10), WindowedCodecs.sessionWindowed(str)]) {
			const partition = windowedPartitioner(codec)
			for (const key of ['a', 'c', 'd', 'f', 'alice']) {
				for (const start of [0, 10, 20]) {
					expect(partition('t', windowed(key, start, start + 10), null, 4)).toBe(basePartition(key, 4))
				}
			}
		}
	})

	it('differs from hashing the whole windowed key', () => {
		const codec = WindowedCodecs.timeWindowed(str, 10)
		const partition = windowedPartitioner(codec)
		const keys = [0, 10, 20].map(start => windowed('a', start, start + 10))
		expect(keys.map(key => partition('t', key, null, 4))).toEqual([0, 0, 0])
		expect(keys.map(key => toPositive(murmur2(codec.encode(key))) % 4)).toEqual([2, 2, 3])
	})

	it('leaves a null key to the producer', () => {
		expect(windowedPartitioner(WindowedCodecs.sessionWindowed(str))('t', null, null, 4)).toBeUndefined()
	})
})
