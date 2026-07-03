import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { encodeRequestHeader, requestHeaderSize } from '@/protocol/messages/headers.js'

describe('api key flexibility table', () => {
	it('OffsetDelete is not flexible at any version (OffsetDeleteRequest.json flexibleVersions=none)', () => {
		expect(isFlexibleVersion(ApiKey.OffsetDelete, 0)).toBe(false)
		expect(isFlexibleVersion(ApiKey.OffsetDelete, 1)).toBe(false)
	})

	it('encodes a non-flexible (v1) request header for OffsetDelete v0', () => {
		const header = { apiKey: ApiKey.OffsetDelete, apiVersion: 0, correlationId: 7, clientId: 'c' }
		const enc = new Encoder()
		encodeRequestHeader(enc, header)

		// apiKey(2) + apiVersion(2) + correlationId(4) + clientId(2 + 1) = 11 bytes.
		// A flexible header would append a tagged-fields byte for 12.
		expect(enc.toBuffer().length).toBe(11)
		expect(requestHeaderSize(header)).toBe(11)
	})

	it('keeps neighbours intact (spot checks)', () => {
		expect(isFlexibleVersion(ApiKey.AlterPartitionReassignments, 0)).toBe(true)
		expect(isFlexibleVersion(ApiKey.ListPartitionReassignments, 0)).toBe(true)
		expect(isFlexibleVersion(ApiKey.DescribeClientQuotas, 0)).toBe(false)
		expect(isFlexibleVersion(ApiKey.DescribeClientQuotas, 1)).toBe(true)
	})
})
