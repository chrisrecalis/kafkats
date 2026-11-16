import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { DELETE_RECORDS_VERSIONS, encodeDeleteRecordsRequest } from '@/protocol/messages/requests/delete-records.js'
import { decodeDeleteRecordsResponse } from '@/protocol/messages/responses/delete-records.js'

describe('DeleteRecords protocol', () => {
	it('supports only v2, the first flexible version', () => {
		expect(DELETE_RECORDS_VERSIONS).toEqual({ min: 2, max: 2 })
		expect(isFlexibleVersion(ApiKey.DeleteRecords, 1)).toBe(false)
		expect(isFlexibleVersion(ApiKey.DeleteRecords, 2)).toBe(true)
	})

	it('encodes a v2 request byte for byte', () => {
		const encoder = new Encoder()

		encodeDeleteRecordsRequest(encoder, 2, {
			topics: [{ name: 't', partitions: [{ partitionIndex: 1, offset: 50n }] }],
			timeoutMs: 30000,
		})

		// topics[1] { name 't', partitions[1] { index 1, offset 50, tags }, tags }, timeoutMs 30000, tags
		expect(encoder.toBuffer().toString('hex')).toBe('0202740200000001000000000000003200000000753000')
	})

	it('encodes every topic and partition of a request in order', () => {
		const encoder = new Encoder()
		encodeDeleteRecordsRequest(encoder, 2, {
			topics: [
				{
					name: 'orders',
					partitions: [
						{ partitionIndex: 0, offset: 7n },
						{ partitionIndex: 3, offset: -1n },
					],
				},
				{ name: 'audit', partitions: [{ partitionIndex: 2, offset: 9_000_000_000n }] },
			],
			timeoutMs: 1500,
		})

		const decoder = new Decoder(encoder.toBuffer())
		const topics = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const partitions = d.readCompactArray(pd => {
				const partition = { partitionIndex: pd.readInt32(), offset: pd.readInt64() }
				expect(pd.readUVarInt()).toBe(0)
				return partition
			})
			expect(d.readUVarInt()).toBe(0)
			return { name, partitions }
		})
		expect(topics).toEqual([
			{
				name: 'orders',
				partitions: [
					{ partitionIndex: 0, offset: 7n },
					{ partitionIndex: 3, offset: -1n },
				],
			},
			{ name: 'audit', partitions: [{ partitionIndex: 2, offset: 9_000_000_000n }] },
		])
		expect(decoder.readInt32()).toBe(1500)
		expect(decoder.readUVarInt()).toBe(0)
		expect(decoder.remaining()).toBe(0)
	})

	it('decodes a v2 response byte for byte', () => {
		// throttle 0, topics[1] { name 't', partitions[1] { index 1, lowWatermark 50, error 0, tags }, tags }, tags
		const decoder = new Decoder(Buffer.from('00000000020274020000000100000000000000320000000000', 'hex'))

		expect(decodeDeleteRecordsResponse(decoder, 2)).toEqual({
			throttleTimeMs: 0,
			topics: [{ name: 't', partitions: [{ partitionIndex: 1, lowWatermark: 50n, errorCode: ErrorCode.None }] }],
		})
		expect(decoder.remaining()).toBe(0)
	})

	it('decodes per-partition errors and skips tagged fields', () => {
		const encoder = new Encoder()
		encoder.writeInt32(25)
		encoder.writeCompactArray(['orders', 'audit'], (name, enc) => {
			enc.writeCompactString(name)
			enc.writeCompactArray([0, 1], (partitionIndex, penc) => {
				penc.writeInt32(partitionIndex)
				penc.writeInt64(partitionIndex === 0 ? 100n : -1n)
				penc.writeInt16(partitionIndex === 0 ? ErrorCode.None : ErrorCode.OffsetOutOfRange)
				penc.writeTaggedFields([{ tag: 0, data: Buffer.from([1, 2, 3]) }])
			})
			enc.writeEmptyTaggedFields()
		})
		encoder.writeTaggedFields([{ tag: 5, data: Buffer.from('x') }])

		const decoder = new Decoder(encoder.toBuffer())
		const response = decodeDeleteRecordsResponse(decoder, 2)

		expect(response.throttleTimeMs).toBe(25)
		expect(response.topics.map(t => t.name)).toEqual(['orders', 'audit'])
		for (const topic of response.topics) {
			expect(topic.partitions).toEqual([
				{ partitionIndex: 0, lowWatermark: 100n, errorCode: ErrorCode.None },
				{ partitionIndex: 1, lowWatermark: -1n, errorCode: ErrorCode.OffsetOutOfRange },
			])
		}
		expect(decoder.remaining()).toBe(0)
	})

	it('rejects versions other than v2', () => {
		const request = { topics: [], timeoutMs: 0 }
		expect(() => encodeDeleteRecordsRequest(new Encoder(), 1, request)).toThrow(
			'Unsupported DeleteRecords version: 1'
		)
		expect(() => decodeDeleteRecordsResponse(new Decoder(Buffer.alloc(0)), 3)).toThrow(
			'Unsupported DeleteRecords version: 3'
		)
	})
})
