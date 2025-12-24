/**
 * DescribeGroups Request (API Key 15)
 *
 * Used to describe consumer groups with detailed member information.
 *
 * Supports v5 (flexible encoding only):
 * - v3: Added includeAuthorizedOperations
 * - v4: Added groupInstanceId in member response
 * - v5: First flexible version
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * DescribeGroups request data
 */
export interface DescribeGroupsRequest {
	/** Group IDs to describe */
	groups: string[]
	/** Whether to include authorized operations (v3+) */
	includeAuthorizedOperations?: boolean
}

/**
 * Supported API versions for DescribeGroups request
 *
 * - v5: flexible encoding with compact strings and tagged fields
 */
export const DESCRIBE_GROUPS_VERSIONS = {
	min: 5,
	max: 5,
}

/**
 * Encode a DescribeGroups request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDescribeGroupsRequest(encoder: IEncoder, version: number, request: DescribeGroupsRequest): void {
	if (version < DESCRIBE_GROUPS_VERSIONS.min || version > DESCRIBE_GROUPS_VERSIONS.max) {
		throw new Error(`Unsupported DescribeGroups version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.DescribeGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible DescribeGroups versions (< 5) not supported')
	}

	// Groups array (compact array of strings)
	encoder.writeCompactArray(request.groups, (groupId, enc) => {
		enc.writeCompactString(groupId)
	})

	// Include authorized operations (v3+)
	encoder.writeBoolean(request.includeAuthorizedOperations ?? false)

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a DescribeGroups request
 *
 * @param groupIds - Group IDs to describe
 * @param options - Optional configuration
 * @returns A DescribeGroupsRequest
 */
export function createDescribeGroupsRequest(
	groupIds: string[],
	options?: { includeAuthorizedOperations?: boolean }
): DescribeGroupsRequest {
	return {
		groups: groupIds,
		includeAuthorizedOperations: options?.includeAuthorizedOperations ?? false,
	}
}
