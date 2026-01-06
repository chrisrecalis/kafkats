/**
 * CreateAcls Response (API Key 30)
 *
 * Returns the result of creating ACL bindings.
 *
 * Supports v2-v3 (flexible encoding only):
 * - v2: First flexible version
 * - v3: Added support for resource pattern types
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Result for a single ACL creation
 */
export interface CreateAclsResult {
	/** Error code for this creation (None if successful) */
	errorCode: ErrorCode
	/** Error message (null if no error) */
	errorMessage: string | null
}

/**
 * CreateAcls response data
 */
export interface CreateAclsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each ACL creation */
	results: CreateAclsResult[]
}

/**
 * Decode a CreateAcls response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeCreateAclsResponse(decoder: IDecoder, version: number): CreateAclsResponse {
	const flexible = isFlexibleVersion(ApiKey.CreateAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible CreateAcls versions (< 2) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Results (compact array)
	const results = decoder.readCompactArray(d => {
		// Error code (INT16)
		const errorCode = d.readInt16() as ErrorCode

		// Error message (compact nullable string)
		const errorMessage = d.readCompactNullableString()

		// Result-level tagged fields
		d.skipTaggedFields()

		return {
			errorCode,
			errorMessage,
		}
	})

	// Response-level tagged fields
	decoder.skipTaggedFields()

	return {
		throttleTimeMs,
		results,
	}
}

/**
 * Check if a CreateAcls response has any errors
 *
 * @param response - The CreateAcls response
 * @returns true if any creation failed
 */
export function hasCreateAclsErrors(response: CreateAclsResponse): boolean {
	return response.results.some(result => result.errorCode !== ErrorCode.None)
}

/**
 * Get all errors from a CreateAcls response
 *
 * @param response - The CreateAcls response
 * @returns Array of { index, errorCode, errorMessage }
 */
export function getCreateAclsErrors(
	response: CreateAclsResponse
): Array<{ index: number; errorCode: ErrorCode; errorMessage: string | null }> {
	const errors: Array<{ index: number; errorCode: ErrorCode; errorMessage: string | null }> = []

	for (let i = 0; i < response.results.length; i++) {
		const result = response.results[i]!
		if (result.errorCode !== ErrorCode.None) {
			errors.push({
				index: i,
				errorCode: result.errorCode,
				errorMessage: result.errorMessage,
			})
		}
	}

	return errors
}
