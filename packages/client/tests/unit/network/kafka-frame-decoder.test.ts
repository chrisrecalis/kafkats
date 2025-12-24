import { describe, expect, it } from 'vitest'

import { KafkaFrameDecoder } from '@/network/kafka-frame-decoder.js'

function frame(payload: Buffer): Buffer {
	const buf = Buffer.allocUnsafe(4 + payload.length)
	buf.writeInt32BE(payload.length, 0)
	payload.copy(buf, 4)
	return buf
}

describe('KafkaFrameDecoder', () => {
	it('decodes a frame when length prefix spans chunks', () => {
		const decoder = new KafkaFrameDecoder()

		const payload = Buffer.from('hello')
		const buf = frame(payload)

		const first = buf.subarray(0, 2)
		const second = buf.subarray(2)

		expect(decoder.push(first)).toEqual([])
		const out = decoder.push(second)

		expect(out).toHaveLength(1)
		expect(out[0]!.toString('utf8')).toBe('hello')
	})

	it('decodes multiple frames in a single chunk', () => {
		const decoder = new KafkaFrameDecoder()

		const p1 = Buffer.from('a')
		const p2 = Buffer.from('bb')
		const chunk = Buffer.concat([frame(p1), frame(p2)])

		const out = decoder.push(chunk)
		expect(out).toHaveLength(2)
		expect(out[0]!.toString('utf8')).toBe('a')
		expect(out[1]!.toString('utf8')).toBe('bb')
	})

	it('decodes a frame spanning multiple chunks without extra messages', () => {
		const decoder = new KafkaFrameDecoder()

		const payload = Buffer.from('0123456789')
		const buf = frame(payload)

		const out1 = decoder.push(buf.subarray(0, 3))
		const out2 = decoder.push(buf.subarray(3, 9))
		const out3 = decoder.push(buf.subarray(9))

		expect(out1).toEqual([])
		expect(out2).toEqual([])
		expect(out3).toHaveLength(1)
		expect(out3[0]!.toString('utf8')).toBe('0123456789')
	})

	it('decodes a full frame and keeps leftover bytes for the next frame', () => {
		const decoder = new KafkaFrameDecoder()

		const p1 = Buffer.from('first')
		const p2 = Buffer.from('second')
		const combined = Buffer.concat([frame(p1), frame(p2)])

		// Split so that the first push contains the full first frame and part of the second
		const cut = frame(p1).length + 2
		const out1 = decoder.push(combined.subarray(0, cut))
		const out2 = decoder.push(combined.subarray(cut))

		expect(out1).toHaveLength(1)
		expect(out1[0]!.toString('utf8')).toBe('first')

		expect(out2).toHaveLength(1)
		expect(out2[0]!.toString('utf8')).toBe('second')
	})
})
