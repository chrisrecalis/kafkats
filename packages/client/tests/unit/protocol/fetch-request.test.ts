import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import { encodeFetchRequestFlexible, type FetchRequest } from '@/protocol/messages/requests/fetch.js'

function baseRequest(overrides: Partial<FetchRequest> = {}): FetchRequest {
	return {
		maxWaitMs: 500,
		minBytes: 1,
		topics: [
			{
				topic: 'my-topic',
				partitions: [{ partitionIndex: 0, fetchOffset: 42n, partitionMaxBytes: 1048576 }],
			},
		],
		...overrides,
	}
}

// Walk a FetchRequest v12 body per the FetchRequest.json schema. In v12-14 the body starts at
// ReplicaId(int32); ClusterId is TAGGED field 0, carried in the request-level tagged section.
function decodeV12Body(buffer: Buffer) {
	const dec = new Decoder(buffer)
	const replicaId = dec.readInt32()
	const maxWaitMs = dec.readInt32()
	const minBytes = dec.readInt32()
	const maxBytes = dec.readInt32()
	const isolationLevel = dec.readInt8()
	const sessionId = dec.readInt32()
	const sessionEpoch = dec.readInt32()
	const topics = dec.readCompactArray(d => {
		const topic = d.readCompactString()
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const currentLeaderEpoch = pd.readInt32()
			const fetchOffset = pd.readInt64()
			const lastFetchedEpoch = pd.readInt32()
			const logStartOffset = pd.readInt64()
			const partitionMaxBytes = pd.readInt32()
			pd.skipTaggedFields()
			return {
				partitionIndex,
				currentLeaderEpoch,
				fetchOffset,
				lastFetchedEpoch,
				logStartOffset,
				partitionMaxBytes,
			}
		})
		d.skipTaggedFields()
		return { topic, partitions }
	})
	const forgottenTopics = dec.readCompactArray(d => {
		const topic = d.readCompactString()
		const partitions = d.readCompactArray(pd => pd.readInt32())
		d.skipTaggedFields()
		return { topic, partitions }
	})
	const rackId = dec.readCompactString()
	const taggedFields = dec.readTaggedFields()
	const remaining = dec.remaining()
	return {
		replicaId,
		maxWaitMs,
		minBytes,
		maxBytes,
		isolationLevel,
		sessionId,
		sessionEpoch,
		topics,
		forgottenTopics,
		rackId,
		taggedFields,
		remaining,
	}
}

describe('Fetch request flexible encoding (v12+)', () => {
	it('starts the v12 body with ReplicaId(int32), not an inline ClusterId string', () => {
		const enc = new Encoder()
		encodeFetchRequestFlexible(enc, 12, baseRequest())

		const body = decodeV12Body(enc.toBuffer())
		expect(body.replicaId).toBe(-1)
		expect(body.maxWaitMs).toBe(500)
		expect(body.minBytes).toBe(1)
		expect(body.sessionEpoch).toBe(-1)
		expect(body.topics).toEqual([
			{
				topic: 'my-topic',
				partitions: [
					{
						partitionIndex: 0,
						currentLeaderEpoch: -1,
						fetchOffset: 42n,
						lastFetchedEpoch: -1,
						logStartOffset: -1n,
						partitionMaxBytes: 1048576,
					},
				],
			},
		])
		expect(body.rackId).toBe('')
		// No clusterId set → empty tagged section
		expect(body.taggedFields).toEqual([])
		expect(body.remaining).toBe(0)
	})

	it('emits ClusterId as tagged field 0 when set', () => {
		const enc = new Encoder()
		encodeFetchRequestFlexible(enc, 12, baseRequest({ clusterId: 'my-cluster' }))

		const body = decodeV12Body(enc.toBuffer())
		expect(body.replicaId).toBe(-1)
		expect(body.taggedFields).toHaveLength(1)
		expect(body.taggedFields[0]!.tag).toBe(0)
		expect(new Decoder(body.taggedFields[0]!.data).readCompactString()).toBe('my-cluster')
		expect(body.remaining).toBe(0)
	})
})
