/**
 * DeleteGroups Request (API Key 42)
 *
 * Used to delete consumer groups from the cluster.
 * Groups must be empty (no active members) to be deleted.
 *
 * Supports v2 (flexible encoding only):
 * - v2: First flexible version
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * DeleteGroups request data
 */
export interface DeleteGroupsRequest {
	/** Group IDs to delete */
	groupsNames: string[]
}

/**
 * Supported API versions for DeleteGroups request
 *
 * - v2: flexible encoding with compact strings and tagged fields
 */
export const DELETE_GROUPS_VERSIONS = {
	min: 2,
	max: 2,
}

/**
 * Encode a DeleteGroups request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDeleteGroupsRequest(encoder: IEncoder, version: number, request: DeleteGroupsRequest): void {
	if (version < DELETE_GROUPS_VERSIONS.min || version > DELETE_GROUPS_VERSIONS.max) {
		throw new Error(`Unsupported DeleteGroups version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.DeleteGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteGroups versions (< 2) not supported')
	}

	// Groups array (compact array of strings)
	encoder.writeCompactArray(request.groupsNames, (groupId, enc) => {
		enc.writeCompactString(groupId)
	})

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a DeleteGroups request
 *
 * @param groupIds - Group IDs to delete
 * @returns A DeleteGroupsRequest
 */
export function createDeleteGroupsRequest(groupIds: string[]): DeleteGroupsRequest {
	return {
		groupsNames: groupIds,
	}
}
