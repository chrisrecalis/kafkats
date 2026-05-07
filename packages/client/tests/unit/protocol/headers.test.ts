import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { decodeResponseHeader } from '@/protocol/messages/headers.js'

describe('decodeResponseHeader', () => {
	it('always uses header v0 for ApiVersions, even at flexible request versions', () => {
		// Per the Kafka protocol: ApiVersions response header is hardcoded v0
		// even when the request version is >= 3 (flexible). The request body
		// uses flexible encoding, but the header does NOT include tagged fields.
		// Without the special case, decodeResponseHeader will try to skip
		// tagged fields and either fail or consume part of the body.
		const encoder = new Encoder()
		encoder.writeInt32(42) // correlationId
		// No tagged-fields byte — this is a v0 header.
		// Now write the body, starting with errorCode int16:
		encoder.writeInt16(0) // ErrorCode.None
		const buffer = encoder.toBuffer()

		const decoder = new Decoder(buffer)
		const header = decodeResponseHeader(decoder, ApiKey.ApiVersions, 3)
		expect(header.correlationId).toBe(42)
		// The next two bytes should still be the errorCode — header decode must not
		// have consumed them as a tagged-fields varint.
		expect(decoder.readInt16()).toBe(0)
		expect(decoder.remaining()).toBe(0)
	})

	it('uses flexible header for non-ApiVersions flexible APIs', () => {
		// Sanity check: Metadata v9 IS flexible and DOES have tagged fields in the header.
		const encoder = new Encoder()
		encoder.writeInt32(7)
		encoder.writeUVarInt(0) // empty tagged fields (varint zero)
		const buffer = encoder.toBuffer()

		const decoder = new Decoder(buffer)
		const header = decodeResponseHeader(decoder, ApiKey.Metadata, 9)
		expect(header.correlationId).toBe(7)
		expect(decoder.remaining()).toBe(0)
	})
})
