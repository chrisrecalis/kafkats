/**
 * DescribeGroups Response (API Key 15)
 *
 * Returns detailed information about consumer groups including members.
 *
 * Supports v5 (flexible encoding only):
 * - v3: Added authorizedOperations
 * - v4: Added groupInstanceId
 * - v5: First flexible version
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Member information within a group
 */
export interface DescribeGroupsMember {
	/** The member ID assigned by the group coordinator */
	memberId: string
	/** The unique identifier of the consumer instance (v4+) */
	groupInstanceId: string | null
	/** The client ID used by the member */
	clientId: string
	/** The host of the member */
	clientHost: string
	/** Member metadata (subscription info, depends on protocol type) */
	memberMetadata: Buffer
	/** Member assignment (partition assignments, depends on protocol type) */
	memberAssignment: Buffer
}

/**
 * Group description result
 */
export interface DescribeGroupsGroup {
	/** Error code for this group */
	errorCode: ErrorCode
	/** The group ID */
	groupId: string
	/** The group state (e.g., "Stable", "Empty", "Dead") */
	groupState: string
	/** The protocol type (e.g., "consumer") */
	protocolType: string
	/** The protocol data (e.g., partition assignment strategy name) */
	protocolData: string
	/** Members of the group */
	members: DescribeGroupsMember[]
	/** Authorized operations bitmask (v3+) */
	authorizedOperations: number
}

/**
 * DescribeGroups response data
 */
export interface DescribeGroupsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each group */
	groups: DescribeGroupsGroup[]
}

/**
 * Decode a DescribeGroups response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDescribeGroupsResponse(decoder: IDecoder, version: number): DescribeGroupsResponse {
	const flexible = isFlexibleVersion(ApiKey.DescribeGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible DescribeGroups versions (< 5) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Groups array (compact array)
	const groups = decoder.readCompactArray(d => {
		const errorCode = d.readInt16() as ErrorCode
		const groupId = d.readCompactString()
		const groupState = d.readCompactString()
		const protocolType = d.readCompactString()
		const protocolData = d.readCompactString()

		// Members array
		const members = d.readCompactArray(md => {
			const memberId = md.readCompactString()
			const groupInstanceId = md.readCompactNullableString()
			const clientId = md.readCompactString()
			const clientHost = md.readCompactString()
			const memberMetadata = md.readCompactBytes()
			const memberAssignment = md.readCompactBytes()

			md.skipTaggedFields()

			return {
				memberId,
				groupInstanceId,
				clientId,
				clientHost,
				memberMetadata,
				memberAssignment,
			}
		})

		// Authorized operations (v3+)
		const authorizedOperations = d.readInt32()

		d.skipTaggedFields()

		return {
			errorCode,
			groupId,
			groupState,
			protocolType,
			protocolData,
			members,
			authorizedOperations,
		}
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, groups }
}

/**
 * Check if a DescribeGroups response has any errors
 *
 * @param response - The DescribeGroups response
 * @returns true if any group has an error
 */
export function hasDescribeGroupsErrors(response: DescribeGroupsResponse): boolean {
	return response.groups.some(group => group.errorCode !== ErrorCode.None)
}

/**
 * Get all errors from a DescribeGroups response
 *
 * @param response - The DescribeGroups response
 * @returns Array of { groupId, errorCode }
 */
export function getDescribeGroupsErrors(response: DescribeGroupsResponse): Array<{
	groupId: string
	errorCode: ErrorCode
}> {
	const errors: Array<{
		groupId: string
		errorCode: ErrorCode
	}> = []

	for (const group of response.groups) {
		if (group.errorCode !== ErrorCode.None) {
			errors.push({
				groupId: group.groupId,
				errorCode: group.errorCode,
			})
		}
	}

	return errors
}
