/**
 * DeleteGroups Response (API Key 42)
 *
 * Returns the result of a delete groups request including any errors.
 *
 * Supports v2 (flexible encoding only):
 * - v2: First flexible version
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Group deletion result
 */
export interface DeleteGroupsGroupResponse {
	/** The group ID */
	groupId: string
	/** Error code for this group */
	errorCode: ErrorCode
}

/**
 * DeleteGroups response data
 */
export interface DeleteGroupsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each group */
	results: DeleteGroupsGroupResponse[]
}

/**
 * Decode a DeleteGroups response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDeleteGroupsResponse(decoder: IDecoder, version: number): DeleteGroupsResponse {
	const flexible = isFlexibleVersion(ApiKey.DeleteGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteGroups versions (< 2) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Results array (compact array)
	const results = decoder.readCompactArray(d => {
		const groupId = d.readCompactString()
		const errorCode = d.readInt16() as ErrorCode

		d.skipTaggedFields()

		return {
			groupId,
			errorCode,
		}
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, results }
}

/**
 * Check if a DeleteGroups response has any errors
 *
 * @param response - The DeleteGroups response
 * @returns true if any group has an error
 */
export function hasDeleteGroupsErrors(response: DeleteGroupsResponse): boolean {
	return response.results.some(group => group.errorCode !== ErrorCode.None)
}

/**
 * Get all errors from a DeleteGroups response
 *
 * @param response - The DeleteGroups response
 * @returns Array of { groupId, errorCode }
 */
export function getDeleteGroupsErrors(response: DeleteGroupsResponse): Array<{
	groupId: string
	errorCode: ErrorCode
}> {
	const errors: Array<{
		groupId: string
		errorCode: ErrorCode
	}> = []

	for (const group of response.results) {
		if (group.errorCode !== ErrorCode.None) {
			errors.push({
				groupId: group.groupId,
				errorCode: group.errorCode,
			})
		}
	}

	return errors
}
