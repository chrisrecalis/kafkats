/**
 * Kafka request/response header encoding and decoding
 *
 * Request headers:
 * - v0: apiKey(int16) + apiVersion(int16) + correlationId(int32)
 * - v1: v0 + clientId(nullable string with INT16 length prefix)
 * - v2: v1 + tagged fields (flexible versions)
 *
 * IMPORTANT: The clientId field ALWAYS uses INT16 length prefix (not compact
 * encoding), even in header v2, so that older brokers can still parse the
 * request header. Only the tagged fields at the end are new in v2.
 *
 * Response headers:
 * - v0: correlationId(int32)
 * - v1: v0 + tagged fields (flexible versions)
 */

import type { IEncoder, IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Request header structure
 */
export interface RequestHeader {
	apiKey: ApiKey
	apiVersion: number
	correlationId: number
	clientId: string | null
}

/**
 * Response header structure
 */
export interface ResponseHeader {
	correlationId: number
}

/**
 * Encode a request header
 *
 * Automatically selects header v1 or v2 based on whether the API version
 * uses flexible encoding.
 *
 * @param encoder - The encoder to write to
 * @param header - The header to encode
 */
export function encodeRequestHeader(encoder: IEncoder, header: RequestHeader): void {
	const flexible = isFlexibleVersion(header.apiKey, header.apiVersion)

	encoder.writeInt16(header.apiKey)
	encoder.writeInt16(header.apiVersion)
	encoder.writeInt32(header.correlationId)

	// ClientId ALWAYS uses INT16 length prefix (not compact encoding),
	// even for flexible versions, so older brokers can parse the header
	encoder.writeNullableString(header.clientId)

	if (flexible) {
		// Header v2 adds tagged fields at the end
		encoder.writeEmptyTaggedFields()
	}
}

/**
 * Decode a response header
 *
 * The header version is determined by whether the request used flexible encoding.
 *
 * @param decoder - The decoder to read from
 * @param apiKey - The API key that was requested
 * @param apiVersion - The API version that was requested
 * @returns The decoded response header
 */
export function decodeResponseHeader(decoder: IDecoder, apiKey: ApiKey, apiVersion: number): ResponseHeader {
	const correlationId = decoder.readInt32()

	const flexible = isFlexibleVersion(apiKey, apiVersion)
	if (flexible) {
		// Header v1: includes tagged fields
		decoder.skipTaggedFields()
	}
	// Header v0: just correlation ID (handled above)

	return { correlationId }
}

/**
 * Calculate the size of a request header
 *
 * @param header - The header to calculate size for
 * @returns The size in bytes
 */
export function requestHeaderSize(header: RequestHeader): number {
	const flexible = isFlexibleVersion(header.apiKey, header.apiVersion)

	// apiKey(2) + apiVersion(2) + correlationId(4) = 8 bytes base
	let size = 8

	// ClientId: INT16 length + bytes (always INT16, even for flexible)
	size += 2
	if (header.clientId !== null) {
		size += Buffer.byteLength(header.clientId, 'utf-8')
	}

	if (flexible) {
		// Header v2 adds empty tagged fields (just count = 0)
		size += 1
	}

	return size
}
