/**
 * ApiVersions Response (API Key 18)
 *
 * Returns the list of API versions supported by the broker.
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion, type ApiVersionRange } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Supported features info (v3+)
 */
export interface SupportedFeature {
	name: string
	minVersion: number
	maxVersion: number
}

/**
 * Finalized features info (v3+)
 */
export interface FinalizedFeature {
	name: string
	maxVersionLevel: number
	minVersionLevel: number
}

/**
 * ApiVersions response data
 */
export interface ApiVersionsResponse {
	/** Error code (0 = success) */
	errorCode: ErrorCode
	/** List of supported API versions */
	apiVersions: ApiVersionRange[]
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Supported features (v3+) */
	supportedFeatures?: SupportedFeature[]
	/** Finalized features epoch (v3+) */
	finalizedFeaturesEpoch?: bigint
	/** Finalized features (v3+) */
	finalizedFeatures?: FinalizedFeature[]
	/** Zk migration ready (v3+) */
	zkMigrationReady?: boolean
}

/**
 * Decode an ApiVersions response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeApiVersionsResponse(decoder: IDecoder, version: number): ApiVersionsResponse {
	const flexible = isFlexibleVersion(ApiKey.ApiVersions, version)

	const errorCode = decoder.readInt16() as ErrorCode

	// API versions array
	let apiVersions: ApiVersionRange[]
	if (flexible) {
		apiVersions = decoder.readCompactArray(d => ({
			apiKey: d.readInt16() as ApiKey,
			minVersion: d.readInt16(),
			maxVersion: d.readInt16(),
			_tagged: d.skipTaggedFields(),
		}))
	} else {
		apiVersions = decoder.readArray(d => ({
			apiKey: d.readInt16() as ApiKey,
			minVersion: d.readInt16(),
			maxVersion: d.readInt16(),
		}))
	}

	// Throttle time (v1+)
	let throttleTimeMs = 0
	if (version >= 1) {
		throttleTimeMs = decoder.readInt32()
	}

	// v3+ features
	let supportedFeatures: SupportedFeature[] | undefined
	let finalizedFeaturesEpoch: bigint | undefined
	let finalizedFeatures: FinalizedFeature[] | undefined
	let zkMigrationReady: boolean | undefined

	if (version >= 3 && flexible) {
		// Supported features
		supportedFeatures = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const minVersion = d.readInt16()
			const maxVersion = d.readInt16()
			d.skipTaggedFields()
			return { name, minVersion, maxVersion }
		})

		// Finalized features epoch
		finalizedFeaturesEpoch = decoder.readInt64()

		// Finalized features
		finalizedFeatures = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const maxVersionLevel = d.readInt16()
			const minVersionLevel = d.readInt16()
			d.skipTaggedFields()
			return { name, maxVersionLevel, minVersionLevel }
		})

		// ZK migration ready (added in later versions, may be in tagged fields)
		// Skip top-level tagged fields which may contain zkMigrationReady
		decoder.skipTaggedFields()
	}

	return {
		errorCode,
		apiVersions,
		throttleTimeMs,
		supportedFeatures,
		finalizedFeaturesEpoch,
		finalizedFeatures,
		zkMigrationReady,
	}
}

/**
 * Find the maximum supported version for an API
 *
 * @param response - The ApiVersions response
 * @param apiKey - The API key to look up
 * @returns The maximum supported version, or undefined if not supported
 */
export function findMaxVersion(response: ApiVersionsResponse, apiKey: ApiKey): number | undefined {
	const version = response.apiVersions.find(v => v.apiKey === apiKey)
	return version?.maxVersion
}

/**
 * Find the best version to use for an API (within a preferred range)
 *
 * @param response - The ApiVersions response
 * @param apiKey - The API key to look up
 * @param preferredMin - Minimum preferred version (optional)
 * @param preferredMax - Maximum preferred version (optional)
 * @returns The best version to use, or undefined if not supported
 */
export function findBestVersion(
	response: ApiVersionsResponse,
	apiKey: ApiKey,
	preferredMin?: number,
	preferredMax?: number
): number | undefined {
	const version = response.apiVersions.find(v => v.apiKey === apiKey)
	if (!version) {
		return undefined
	}

	const min = preferredMin !== undefined ? Math.max(preferredMin, version.minVersion) : version.minVersion
	const max = preferredMax !== undefined ? Math.min(preferredMax, version.maxVersion) : version.maxVersion

	if (min > max) {
		return undefined // No overlapping version range
	}

	return max // Use the highest version in the overlapping range
}
