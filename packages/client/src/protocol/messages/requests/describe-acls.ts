/**
 * DescribeAcls Request (API Key 29)
 *
 * Used to query ACLs from the Kafka cluster.
 *
 * Supports v2-v3 (flexible encoding only):
 * - v2: First flexible version
 * - v3: Added support for describing ACLs by resource pattern type
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * ACL resource types as defined in Kafka protocol
 */
export enum AclResourceType {
	/** Unknown resource type */
	UNKNOWN = 0,
	/** Match any resource type (for filters) */
	ANY = 1,
	/** Topic resource */
	TOPIC = 2,
	/** Consumer group resource */
	GROUP = 3,
	/** Cluster resource */
	CLUSTER = 4,
	/** Transactional ID resource */
	TRANSACTIONAL_ID = 5,
	/** Delegation token resource */
	DELEGATION_TOKEN = 6,
	/** User resource */
	USER = 7,
}

/**
 * ACL resource pattern types as defined in Kafka protocol
 */
export enum AclResourcePatternType {
	/** Unknown pattern type */
	UNKNOWN = 0,
	/** Match any pattern type (for filters) */
	ANY = 1,
	/** Match patterns (for filters, returns both LITERAL and PREFIXED) */
	MATCH = 2,
	/** Literal resource name match */
	LITERAL = 3,
	/** Prefixed resource name match */
	PREFIXED = 4,
}

/**
 * ACL operations as defined in Kafka protocol
 */
export enum AclOperation {
	/** Unknown operation */
	UNKNOWN = 0,
	/** Match any operation (for filters) */
	ANY = 1,
	/** All operations */
	ALL = 2,
	/** Read operation */
	READ = 3,
	/** Write operation */
	WRITE = 4,
	/** Create operation */
	CREATE = 5,
	/** Delete operation */
	DELETE = 6,
	/** Alter operation */
	ALTER = 7,
	/** Describe operation */
	DESCRIBE = 8,
	/** Cluster action operation */
	CLUSTER_ACTION = 9,
	/** Describe configs operation */
	DESCRIBE_CONFIGS = 10,
	/** Alter configs operation */
	ALTER_CONFIGS = 11,
	/** Idempotent write operation */
	IDEMPOTENT_WRITE = 12,
}

/**
 * ACL permission types as defined in Kafka protocol
 */
export enum AclPermissionType {
	/** Unknown permission type */
	UNKNOWN = 0,
	/** Match any permission type (for filters) */
	ANY = 1,
	/** Deny permission */
	DENY = 2,
	/** Allow permission */
	ALLOW = 3,
}

/**
 * DescribeAcls request data
 */
export interface DescribeAclsRequest {
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
 * Supported API versions for DescribeAcls request
 *
 * - v2-v3: flexible encoding with compact strings and tagged fields
 */
export const DESCRIBE_ACLS_VERSIONS = {
	min: 2,
	max: 3,
}

/**
 * Encode a DescribeAcls request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDescribeAclsRequest(encoder: IEncoder, version: number, request: DescribeAclsRequest): void {
	if (version < DESCRIBE_ACLS_VERSIONS.min || version > DESCRIBE_ACLS_VERSIONS.max) {
		throw new Error(`Unsupported DescribeAcls version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.DescribeAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible DescribeAcls versions (< 2) not supported')
	}

	// Resource type filter (INT8)
	encoder.writeInt8(request.resourceTypeFilter)

	// Resource name filter (compact nullable string)
	encoder.writeCompactNullableString(request.resourceNameFilter)

	// Pattern type filter (INT8)
	encoder.writeInt8(request.patternTypeFilter)

	// Principal filter (compact nullable string)
	encoder.writeCompactNullableString(request.principalFilter)

	// Host filter (compact nullable string)
	encoder.writeCompactNullableString(request.hostFilter)

	// Operation (INT8)
	encoder.writeInt8(request.operation)

	// Permission type (INT8)
	encoder.writeInt8(request.permissionType)

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Helper to create a DescribeAcls request that matches all ACLs
 *
 * @returns A DescribeAclsRequest that matches everything
 */
export function createDescribeAllAclsRequest(): DescribeAclsRequest {
	return {
		resourceTypeFilter: AclResourceType.ANY,
		resourceNameFilter: null,
		patternTypeFilter: AclResourcePatternType.ANY,
		principalFilter: null,
		hostFilter: null,
		operation: AclOperation.ANY,
		permissionType: AclPermissionType.ANY,
	}
}

/**
 * Helper to create a DescribeAcls request for a specific resource
 *
 * @param resourceType - The resource type to filter by
 * @param resourceName - The resource name to filter by (null for any)
 * @param options - Additional filter options
 * @returns A DescribeAclsRequest
 */
export function createDescribeAclsRequest(
	resourceType: AclResourceType,
	resourceName: string | null,
	options?: {
		patternType?: AclResourcePatternType
		principal?: string | null
		host?: string | null
		operation?: AclOperation
		permissionType?: AclPermissionType
	}
): DescribeAclsRequest {
	return {
		resourceTypeFilter: resourceType,
		resourceNameFilter: resourceName,
		patternTypeFilter: options?.patternType ?? AclResourcePatternType.ANY,
		principalFilter: options?.principal ?? null,
		hostFilter: options?.host ?? null,
		operation: options?.operation ?? AclOperation.ANY,
		permissionType: options?.permissionType ?? AclPermissionType.ANY,
	}
}
