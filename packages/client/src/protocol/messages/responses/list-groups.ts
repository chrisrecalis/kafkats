/**
 * ListGroups Response (API Key 16)
 *
 * Returns the list of consumer groups from a broker.
 *
 * Supports v3-v4 (flexible encoding only):
 * - v3: First flexible version
 * - v4: Added group state field
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Group listing entry
 */
export interface ListGroupsGroup {
	/** The group ID */
	groupId: string
	/** The protocol type (e.g., "consumer") */
	protocolType: string
	/** The group state (v4+) */
	groupState: string
}

/**
 * ListGroups response data
 */
export interface ListGroupsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Error code for the entire request */
	errorCode: ErrorCode
	/** List of groups on this broker */
	groups: ListGroupsGroup[]
}

/**
 * Decode a ListGroups response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeListGroupsResponse(decoder: IDecoder, version: number): ListGroupsResponse {
	const flexible = isFlexibleVersion(ApiKey.ListGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible ListGroups versions (< 3) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Error code
	const errorCode = decoder.readInt16() as ErrorCode

	// Groups array (compact array)
	const groups = decoder.readCompactArray(d => {
		const groupId = d.readCompactString()
		const protocolType = d.readCompactString()

		// Group state (v4+)
		let groupState = ''
		if (version >= 4) {
			groupState = d.readCompactString()
		}

		d.skipTaggedFields()

		return {
			groupId,
			protocolType,
			groupState,
		}
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, errorCode, groups }
}

/**
 * Check if a ListGroups response has an error
 *
 * @param response - The ListGroups response
 * @returns true if there's an error
 */
export function hasListGroupsError(response: ListGroupsResponse): boolean {
	return response.errorCode !== ErrorCode.None
}
