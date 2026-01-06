/**
 * DeleteAcls Request (API Key 31)
 *
 * Used to delete ACLs from the Kafka cluster that match specified filters.
 *
 * Supports v2-v3 (flexible encoding only):
 * - v2: First flexible version
 * - v3: Added support for resource pattern types
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import {
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'

/**
 * ACL deletion filter
 */
export interface DeleteAclsFilter {
	/** Resource type filter */
	resourceTypeFilter: AclResourceType
	/** Resource name filter (null matches any) */
	resourceNameFilter: string | null
	/** Resource pattern type filter */
	patternTypeFilter: AclResourcePatternType
	/** Principal filter (null matches any) */
	principalFilter: string | null
	/** Host filter (null matches any) */
	hostFilter: string | null
	/** Operation filter */
	operation: AclOperation
	/** Permission type filter */
	permissionType: AclPermissionType
}

/**
 * DeleteAcls request data
 */
export interface DeleteAclsRequest {
	/** Filters for ACLs to delete */
	filters: DeleteAclsFilter[]
}

/**
 * Supported API versions for DeleteAcls request
 *
 * - v2-v3: flexible encoding with compact strings and tagged fields
 */
export const DELETE_ACLS_VERSIONS = {
	min: 2,
	max: 3,
}

/**
 * Encode a DeleteAcls request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDeleteAclsRequest(encoder: IEncoder, version: number, request: DeleteAclsRequest): void {
	if (version < DELETE_ACLS_VERSIONS.min || version > DELETE_ACLS_VERSIONS.max) {
		throw new Error(`Unsupported DeleteAcls version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.DeleteAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteAcls versions (< 2) not supported')
	}

	// Filters array (compact array)
	encoder.writeCompactArray(request.filters, (filter, enc) => {
		// Resource type filter (INT8)
		enc.writeInt8(filter.resourceTypeFilter)

		// Resource name filter (compact nullable string)
		enc.writeCompactNullableString(filter.resourceNameFilter)

		// Pattern type filter (INT8)
		enc.writeInt8(filter.patternTypeFilter)

		// Principal filter (compact nullable string)
		enc.writeCompactNullableString(filter.principalFilter)

		// Host filter (compact nullable string)
		enc.writeCompactNullableString(filter.hostFilter)

		// Operation (INT8)
		enc.writeInt8(filter.operation)

		// Permission type (INT8)
		enc.writeInt8(filter.permissionType)

		// Entry-level tagged fields
		enc.writeEmptyTaggedFields()
	})

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a DeleteAcls request
 *
 * @param filters - Filters for ACLs to delete
 * @returns A DeleteAclsRequest
 */
export function createDeleteAclsRequest(filters: DeleteAclsFilter[]): DeleteAclsRequest {
	return {
		filters,
	}
}
