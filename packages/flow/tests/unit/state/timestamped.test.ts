import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'

describe('valueAndTimestampCodec', () => {
	const timestamped = valueAndTimestampCodec(codec.string())

	it('encodes [int64BE timestamp][value bytes] (Kafka Streams ValueAndTimestampSerializer)', () => {
		expect(timestamped.encode({ value: 'ab', timestamp: 258 })).toEqual(
			Buffer.from([0, 0, 0, 0, 0, 0, 0x01, 0x02, 0x61, 0x62])
		)
		expect(timestamped.encode({ value: '', timestamp: 0 })).toEqual(Buffer.alloc(8))
	})

	it('decodes what it encodes, for epoch-millisecond timestamps', () => {
		const now = Date.now()
		expect(timestamped.decode(timestamped.encode({ value: 'v', timestamp: now }))).toEqual({
			value: 'v',
			timestamp: now,
		})
		expect(timestamped.decode(Buffer.from([0, 0, 0, 0, 0, 0, 0x01, 0x02, 0x61, 0x62]))).toEqual({
			value: 'ab',
			timestamp: 258,
		})
	})

	it('wraps any inner codec', () => {
		const json = valueAndTimestampCodec(codec.json<{ n: number }>())
		const bytes = json.encode({ value: { n: 1 }, timestamp: 7 })
		expect(bytes.subarray(8).toString()).toBe('{"n":1}')
		expect(json.decode(bytes)).toEqual({ value: { n: 1 }, timestamp: 7 })
	})
})
