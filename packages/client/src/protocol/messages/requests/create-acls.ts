/**
 * CreateAcls Request (API Key 30)
 *
 * Used to create ACL bindings in the Kafka cluster.
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
 * ACL creation entry
 */
export interface AclCreation {
	/** Resource type */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type */
	resourcePatternType: AclResourcePatternType
	/** Principal (e.g., "User:alice") */
	principal: string
	/** Host (use "*" for all hosts) */
	host: string
	/** Operation to allow or deny */
	operation: AclOperation
	/** Permission type (ALLOW or DENY) */
	permissionType: AclPermissionType
}

/**
 * CreateAcls request data
 */
export interface CreateAclsRequest {
	/** ACL entries to create */
	creations: AclCreation[]
}

/**
 * Supported API versions for CreateAcls request
 *
 * - v2-v3: flexible encoding with compact strings and tagged fields
 */
export const CREATE_ACLS_VERSIONS = {
	min: 2,
	max: 3,
}

/**
 * Encode a CreateAcls request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeCreateAclsRequest(encoder: IEncoder, version: number, request: CreateAclsRequest): void {
	if (version < CREATE_ACLS_VERSIONS.min || version > CREATE_ACLS_VERSIONS.max) {
		throw new Error(`Unsupported CreateAcls version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.CreateAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible CreateAcls versions (< 2) not supported')
	}

	// Creations array (compact array)
	encoder.writeCompactArray(request.creations, (creation, enc) => {
		// Resource type (INT8)
		enc.writeInt8(creation.resourceType)

		// Resource name (compact string)
		enc.writeCompactString(creation.resourceName)

		// Resource pattern type (INT8)
		enc.writeInt8(creation.resourcePatternType)

		// Principal (compact string)
		enc.writeCompactString(creation.principal)

		// Host (compact string)
		enc.writeCompactString(creation.host)

		// Operation (INT8)
		enc.writeInt8(creation.operation)

		// Permission type (INT8)
		enc.writeInt8(creation.permissionType)

		// Entry-level tagged fields
		enc.writeEmptyTaggedFields()
	})

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a CreateAcls request
 *
 * @param acls - ACL entries to create
 * @returns A CreateAclsRequest
 */
export function createCreateAclsRequest(acls: AclCreation[]): CreateAclsRequest {
	return {
		creations: acls,
	}
}
