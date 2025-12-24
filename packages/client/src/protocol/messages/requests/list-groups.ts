/**
 * ListGroups Request (API Key 16)
 *
 * Used to list all consumer groups in the cluster.
 *
 * Supports v3-v4 (flexible encoding only):
 * - v3: First flexible version
 * - v4: Added states filter and group state in response
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * ListGroups request data
 */
export interface ListGroupsRequest {
	/** Filter groups by state (v4+). Empty array means all states. */
	statesFilter?: string[]
}

/**
 * Supported API versions for ListGroups request
 *
 * - v3-v4: flexible encoding with compact strings and tagged fields
 */
export const LIST_GROUPS_VERSIONS = {
	min: 3,
	max: 4,
}

/**
 * Encode a ListGroups request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeListGroupsRequest(encoder: IEncoder, version: number, request: ListGroupsRequest): void {
	if (version < LIST_GROUPS_VERSIONS.min || version > LIST_GROUPS_VERSIONS.max) {
		throw new Error(`Unsupported ListGroups version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ListGroups, version)

	if (!flexible) {
		throw new Error('Non-flexible ListGroups versions (< 3) not supported')
	}

	// States filter (v4+)
	if (version >= 4) {
		const states = request.statesFilter ?? []
		encoder.writeCompactArray(states, (state, enc) => {
			enc.writeCompactString(state)
		})
	}

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a ListGroups request
 *
 * @param options - Optional filter configuration
 * @returns A ListGroupsRequest
 */
export function createListGroupsRequest(options?: { statesFilter?: string[] }): ListGroupsRequest {
	return {
		statesFilter: options?.statesFilter,
	}
}
