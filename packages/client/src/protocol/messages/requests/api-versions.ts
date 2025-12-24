/**
 * ApiVersions Request (API Key 18)
 *
 * Used to discover which API versions the broker supports.
 * This is typically the first request sent to a broker.
 *
 * Version 3+ uses flexible encoding (compact types + tagged fields).
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * ApiVersions request data
 */
export interface ApiVersionsRequest {
	/** Client software name (v3+) */
	clientSoftwareName?: string
	/** Client software version (v3+) */
	clientSoftwareVersion?: string
}

/**
 * Supported API versions for ApiVersions request
 */
export const API_VERSIONS_VERSIONS = {
	min: 0,
	max: 3,
}

/**
 * Encode an ApiVersions request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeApiVersionsRequest(encoder: IEncoder, version: number, request: ApiVersionsRequest = {}): void {
	if (version < API_VERSIONS_VERSIONS.min || version > API_VERSIONS_VERSIONS.max) {
		throw new Error(`Unsupported ApiVersions version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ApiVersions, version)

	if (version >= 3) {
		// v3+: Include client software info
		if (flexible) {
			encoder.writeCompactString(request.clientSoftwareName ?? '')
			encoder.writeCompactString(request.clientSoftwareVersion ?? '')
			encoder.writeEmptyTaggedFields()
		}
	}
	// v0-v2: Empty request body (nothing to encode)
}

/**
 * Get the default API version to use for ApiVersions requests
 *
 * Note: ApiVersions is special - we typically start with v0 or v1 for initial
 * handshake since we don't know what the broker supports yet.
 */
export function getDefaultApiVersionsVersion(): number {
	return 3 // Use v3 for flexible version support
}
