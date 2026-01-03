import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

import { encodeShareGroupHeartbeatRequest } from '@/protocol/messages/requests/share-group-heartbeat.js'
import { decodeShareGroupHeartbeatResponse } from '@/protocol/messages/responses/share-group-heartbeat.js'

import { encodeShareFetchRequest } from '@/protocol/messages/requests/share-fetch.js'
import { decodeShareFetchResponse } from '@/protocol/messages/responses/share-fetch.js'

import { encodeShareAcknowledgeRequest } from '@/protocol/messages/requests/share-acknowledge.js'
import { decodeShareAcknowledgeResponse } from '@/protocol/messages/responses/share-acknowledge.js'

describe('share protocol codecs', () => {
	it('encodes ShareGroupHeartbeatRequest v1', () => {
		const enc = new Encoder()
		encodeShareGroupHeartbeatRequest(enc, 1, {
			groupId: 'g1',
			memberId: 'm1',
			memberEpoch: 0,
			rackId: 'r1',
			subscribedTopicNames: ['t1', 't2'],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readCompactString()).toBe('g1')
		expect(dec.readCompactString()).toBe('m1')
		expect(dec.readInt32()).toBe(0)
		expect(dec.readCompactNullableString()).toBe('r1')
		expect(dec.readCompactArray(d => d.readCompactString())).toEqual(['t1', 't2'])
		expect(dec.readUVarInt()).toBe(0) // tagged fields
	})

	it('decodes ShareGroupHeartbeatResponse v1', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeInt16(ErrorCode.None)
		enc.writeCompactNullableString(null) // errorMessage
		enc.writeCompactNullableString('m1') // memberId
		enc.writeInt32(1) // memberEpoch
		enc.writeInt32(250) // heartbeatIntervalMs
		enc.writeInt8(1) // assignment marker: present
		enc.writeCompactArray(
			[
				{ topicId: '00000000-0000-0000-0000-000000000001', partitions: [0, 1] },
				{ topicId: '00000000-0000-0000-0000-000000000002', partitions: [2] },
			],
			(t, e) => {
				e.writeUUID(t.topicId)
				e.writeCompactArray(t.partitions, (p, pe) => pe.writeInt32(p))
				e.writeEmptyTaggedFields()
			}
		)
		enc.writeEmptyTaggedFields() // assignment tagged fields
		enc.writeEmptyTaggedFields()

		const res = decodeShareGroupHeartbeatResponse(new Decoder(enc.toBuffer()), 1)
		expect(res.errorCode).toBe(ErrorCode.None)
		expect(res.memberId).toBe('m1')
		expect(res.memberEpoch).toBe(1)
		expect(res.heartbeatIntervalMs).toBe(250)
		expect(res.assignment?.length).toBe(2)
		expect(res.assignment?.[0]?.partitions).toEqual([0, 1])
	})

	it('decodes ShareGroupHeartbeatResponse v1 with null assignment', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeInt16(ErrorCode.None)
		enc.writeCompactNullableString(null) // errorMessage
		enc.writeCompactNullableString('m1') // memberId
		enc.writeInt32(1) // memberEpoch
		enc.writeInt32(250) // heartbeatIntervalMs
		enc.writeInt8(-1) // assignment marker: null
		enc.writeEmptyTaggedFields()

		const res = decodeShareGroupHeartbeatResponse(new Decoder(enc.toBuffer()), 1)
		expect(res.errorCode).toBe(ErrorCode.None)
		expect(res.assignment).toBeNull()
	})

	it('encodes ShareFetchRequest v1', () => {
		const enc = new Encoder()
		encodeShareFetchRequest(enc, 1, {
			groupId: 'g1',
			memberId: 'm1',
			shareSessionEpoch: 0,
			maxWaitMs: 5000,
			minBytes: 1,
			maxBytes: 1024,
			maxRecords: 500,
			batchSize: 100,
			topics: [
				{
					topicId: '00000000-0000-0000-0000-000000000001',
					partitions: [
						{
							partitionIndex: 0,
							acknowledgementBatches: [],
						},
					],
				},
			],
			forgottenTopicsData: [],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readCompactNullableString()).toBe('g1')
		expect(dec.readCompactNullableString()).toBe('m1')
		expect(dec.readInt32()).toBe(0)
		expect(dec.readInt32()).toBe(5000)
		expect(dec.readInt32()).toBe(1)
		expect(dec.readInt32()).toBe(1024)
		expect(dec.readInt32()).toBe(500)
		expect(dec.readInt32()).toBe(100)

		const topics = dec.readCompactArray(d => {
			const topicId = d.readUUID()
			const partitions = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const acks = pd.readCompactArray(ad => {
					const firstOffset = ad.readInt64()
					const lastOffset = ad.readInt64()
					const types = ad.readCompactArray(td => td.readInt8())
					ad.skipTaggedFields()
					return { firstOffset, lastOffset, types }
				})
				pd.skipTaggedFields()
				return { partitionIndex, acks }
			})
			d.skipTaggedFields()
			return { topicId, partitions }
		})

		expect(topics[0]?.topicId).toBe('00000000-0000-0000-0000-000000000001')
		expect(topics[0]?.partitions[0]?.partitionIndex).toBe(0)
		expect(dec.readCompactArray(d => d.readUUID())).toEqual([]) // forgottenTopicsData topic ids
		expect(dec.readUVarInt()).toBe(0) // tagged fields
	})

	it('decodes ShareFetchResponse v1', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeInt16(ErrorCode.None)
		enc.writeCompactNullableString(null) // errorMessage
		enc.writeInt32(1000) // acquisitionLockTimeoutMs
		enc.writeCompactArray(
			[
				{
					topicId: '00000000-0000-0000-0000-000000000001',
					partitions: [
						{
							partitionIndex: 0,
							errorCode: ErrorCode.None,
							errorMessage: null,
							acknowledgeErrorCode: ErrorCode.None,
							acknowledgeErrorMessage: null,
							leaderId: 1,
							leaderEpoch: 0,
							recordsData: Buffer.alloc(0),
							acquired: [{ firstOffset: 10n, lastOffset: 12n, deliveryCount: 2 }],
						},
					],
				},
			],
			(t, te) => {
				te.writeUUID(t.topicId)
				te.writeCompactArray(t.partitions, (p, pe) => {
					pe.writeInt32(p.partitionIndex)
					pe.writeInt16(p.errorCode)
					pe.writeCompactNullableString(p.errorMessage)
					pe.writeInt16(p.acknowledgeErrorCode)
					pe.writeCompactNullableString(p.acknowledgeErrorMessage)
					pe.writeInt32(p.leaderId)
					pe.writeInt32(p.leaderEpoch)
					pe.writeEmptyTaggedFields() // currentLeader tagged fields
					pe.writeCompactNullableBytes(p.recordsData)
					pe.writeCompactArray(p.acquired, (a, ae) => {
						ae.writeInt64(a.firstOffset)
						ae.writeInt64(a.lastOffset)
						ae.writeInt16(a.deliveryCount)
						ae.writeEmptyTaggedFields()
					})
					pe.writeEmptyTaggedFields()
				})
				te.writeEmptyTaggedFields()
			}
		)
		enc.writeCompactArray([], () => {}) // nodeEndpoints
		enc.writeEmptyTaggedFields()

		const res = decodeShareFetchResponse(new Decoder(enc.toBuffer()), 1)
		expect(res.errorCode).toBe(ErrorCode.None)
		expect(res.acquisitionLockTimeoutMs).toBe(1000)
		expect(res.topics[0]?.partitions[0]?.partitionIndex).toBe(0)
		expect(res.topics[0]?.partitions[0]?.acquiredRecords[0]?.deliveryCount).toBe(2)
	})

	it('encodes ShareAcknowledgeRequest v1', () => {
		const enc = new Encoder()
		encodeShareAcknowledgeRequest(enc, 1, {
			groupId: 'g1',
			memberId: 'm1',
			shareSessionEpoch: 0,
			topics: [
				{
					topicId: '00000000-0000-0000-0000-000000000001',
					partitions: [
						{
							partitionIndex: 0,
							acknowledgementBatches: [
								{
									firstOffset: 10n,
									lastOffset: 10n,
									acknowledgeTypes: [1],
								},
							],
						},
					],
				},
			],
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readCompactNullableString()).toBe('g1')
		expect(dec.readCompactNullableString()).toBe('m1')
		expect(dec.readInt32()).toBe(0)
		const topics = dec.readCompactArray(d => {
			const topicId = d.readUUID()
			const partitions = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const batches = pd.readCompactArray(bd => {
					const firstOffset = bd.readInt64()
					const lastOffset = bd.readInt64()
					const types = bd.readCompactArray(td => td.readInt8())
					bd.skipTaggedFields()
					return { firstOffset, lastOffset, types }
				})
				pd.skipTaggedFields()
				return { partitionIndex, batches }
			})
			d.skipTaggedFields()
			return { topicId, partitions }
		})
		expect(topics[0]?.topicId).toBe('00000000-0000-0000-0000-000000000001')
		expect(topics[0]?.partitions[0]?.partitionIndex).toBe(0)
		expect(topics[0]?.partitions[0]?.batches[0]?.types).toEqual([1])
		expect(dec.readUVarInt()).toBe(0)
	})

	it('decodes ShareAcknowledgeResponse v1', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeInt16(ErrorCode.None)
		enc.writeCompactNullableString(null) // errorMessage
		enc.writeCompactArray(
			[
				{
					topicId: '00000000-0000-0000-0000-000000000001',
					partitions: [
						{
							partitionIndex: 0,
							errorCode: ErrorCode.None,
							errorMessage: null,
							leaderId: 1,
							leaderEpoch: 0,
						},
					],
				},
			],
			(t, te) => {
				te.writeUUID(t.topicId)
				te.writeCompactArray(t.partitions, (p, pe) => {
					pe.writeInt32(p.partitionIndex)
					pe.writeInt16(p.errorCode)
					pe.writeCompactNullableString(p.errorMessage)
					pe.writeInt32(p.leaderId)
					pe.writeInt32(p.leaderEpoch)
					pe.writeEmptyTaggedFields() // currentLeader tagged fields
					pe.writeEmptyTaggedFields()
				})
				te.writeEmptyTaggedFields()
			}
		)
		enc.writeCompactArray([], () => {}) // nodeEndpoints
		enc.writeEmptyTaggedFields()

		const res = decodeShareAcknowledgeResponse(new Decoder(enc.toBuffer()), 1)
		expect(res.throttleTimeMs).toBe(0)
		expect(res.errorCode).toBe(ErrorCode.None)
		expect(res.topics[0]?.topicId).toBe('00000000-0000-0000-0000-000000000001')
		expect(res.topics[0]?.partitions[0]?.errorCode).toBe(ErrorCode.None)
	})
})
