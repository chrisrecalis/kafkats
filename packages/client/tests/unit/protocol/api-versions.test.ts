import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import type { TaggedField } from '@/protocol/primitives/types.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { decodeApiVersionsResponse } from '@/protocol/messages/responses/api-versions.js'

// Build a v3+ ApiVersionsResponse body as a KRaft broker sends it:
// errorCode(int16) + apiKeys(compact array) + throttleTimeMs(int32) + tagged-fields section.
// Per KIP-584, SupportedFeatures (tag 0), FinalizedFeaturesEpoch (tag 1), FinalizedFeatures (tag 2)
// and ZkMigrationReady (tag 3) live in the TAGGED section — not in the message body.
function buildV3Response(taggedFields: TaggedField[]): Buffer {
	const enc = new Encoder()
	enc.writeInt16(0) // errorCode
	enc.writeCompactArray(
		[
			{ apiKey: ApiKey.ApiVersions, min: 0, max: 3 },
			{ apiKey: ApiKey.Fetch, min: 0, max: 16 },
		],
		(v, e) => {
			e.writeInt16(v.apiKey)
			e.writeInt16(v.min)
			e.writeInt16(v.max)
			e.writeEmptyTaggedFields()
		}
	)
	enc.writeInt32(0) // throttleTimeMs
	if (taggedFields.length === 0) {
		enc.writeEmptyTaggedFields()
	} else {
		enc.writeTaggedFields(taggedFields)
	}
	return enc.toBuffer()
}

function supportedFeaturesField(): TaggedField {
	const data = new Encoder()
	data.writeCompactArray([{ name: 'metadata.version', min: 1, max: 14 }], (f, e) => {
		e.writeCompactString(f.name)
		e.writeInt16(f.min)
		e.writeInt16(f.max)
		e.writeEmptyTaggedFields()
	})
	return { tag: 0, data: data.toBuffer() }
}

describe('ApiVersions v3 response decoding (KIP-584 tagged fields)', () => {
	it('decodes a v3 response with an empty tagged-fields section', () => {
		const buffer = buildV3Response([])
		const decoder = new Decoder(buffer)

		const response = decodeApiVersionsResponse(decoder, 3)

		expect(response.errorCode).toBe(0)
		expect(response.apiVersions).toHaveLength(2)
		expect(response.apiVersions[1]).toMatchObject({ apiKey: ApiKey.Fetch, minVersion: 0, maxVersion: 16 })
		expect(response.throttleTimeMs).toBe(0)
		expect(response.supportedFeatures).toBeUndefined()
		expect(decoder.remaining()).toBe(0)
	})

	it('decodes SupportedFeatures from tagged field 0', () => {
		const buffer = buildV3Response([supportedFeaturesField()])
		const decoder = new Decoder(buffer)

		const response = decodeApiVersionsResponse(decoder, 3)

		expect(response.supportedFeatures).toEqual([{ name: 'metadata.version', minVersion: 1, maxVersion: 14 }])
		expect(decoder.remaining()).toBe(0)
	})

	it('decodes all KIP-584 tagged fields and ignores unknown tags', () => {
		const epochData = new Encoder()
		epochData.writeInt64(7n)

		const finalizedData = new Encoder()
		finalizedData.writeCompactArray([{ name: 'metadata.version', maxLevel: 14, minLevel: 1 }], (f, e) => {
			e.writeCompactString(f.name)
			e.writeInt16(f.maxLevel)
			e.writeInt16(f.minLevel)
			e.writeEmptyTaggedFields()
		})

		const zkData = new Encoder()
		zkData.writeBoolean(true)

		const unknownData = new Encoder()
		unknownData.writeInt32(42)

		const buffer = buildV3Response([
			supportedFeaturesField(),
			{ tag: 1, data: epochData.toBuffer() },
			{ tag: 2, data: finalizedData.toBuffer() },
			{ tag: 3, data: zkData.toBuffer() },
			{ tag: 99, data: unknownData.toBuffer() },
		])
		const decoder = new Decoder(buffer)

		const response = decodeApiVersionsResponse(decoder, 3)

		expect(response.supportedFeatures).toEqual([{ name: 'metadata.version', minVersion: 1, maxVersion: 14 }])
		expect(response.finalizedFeaturesEpoch).toBe(7n)
		expect(response.finalizedFeatures).toEqual([
			{ name: 'metadata.version', maxVersionLevel: 14, minVersionLevel: 1 },
		])
		expect(response.zkMigrationReady).toBe(true)
		expect(decoder.remaining()).toBe(0)
	})

	it('still decodes a v0 (non-flexible) response', () => {
		const enc = new Encoder()
		enc.writeInt16(0)
		enc.writeArray([{ apiKey: ApiKey.Produce, min: 0, max: 9 }], (v, e) => {
			e.writeInt16(v.apiKey)
			e.writeInt16(v.min)
			e.writeInt16(v.max)
		})
		const decoder = new Decoder(enc.toBuffer())

		const response = decodeApiVersionsResponse(decoder, 0)

		expect(response.apiVersions).toEqual([{ apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 9 }])
		expect(decoder.remaining()).toBe(0)
	})
})
