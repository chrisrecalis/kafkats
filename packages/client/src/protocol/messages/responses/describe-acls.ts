/**
 * DescribeAcls Response (API Key 29)
 *
 * Returns ACLs matching the filter criteria.
 *
 * Supports v2-v3 (flexible encoding only):
 * - v2: First flexible version
 * - v3: Added support for resource pattern types
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'

/**
 * ACL entry in the response
 */
export interface DescribeAclsAcl {
	/** Principal (e.g., "User:alice") */
	principal: string
	/** Host */
	host: string
	/** Operation */
	operation: AclOperation
	/** Permission type */
	permissionType: AclPermissionType
}

/**
 * Resource with its ACLs
 */
export interface DescribeAclsResource {
	/** Resource type */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type */
	patternType: AclResourcePatternType
	/** ACLs for this resource */
	acls: DescribeAclsAcl[]
}

/**
 * DescribeAcls response data
 */
export interface DescribeAclsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Error code (None if successful) */
	errorCode: ErrorCode
	/** Error message (null if no error) */
	errorMessage: string | null
	/** Resources with their ACLs */
	resources: DescribeAclsResource[]
}

/**
 * Decode a DescribeAcls response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDescribeAclsResponse(decoder: IDecoder, version: number): DescribeAclsResponse {
	const flexible = isFlexibleVersion(ApiKey.DescribeAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible DescribeAcls versions (< 2) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Error code
	const errorCode = decoder.readInt16() as ErrorCode

	// Error message (compact nullable string)
	const errorMessage = decoder.readCompactNullableString()

	// Resources (compact array)
	const resources = decoder.readCompactArray(d => {
		// Resource type (INT8)
		const resourceType = d.readInt8() as AclResourceType

		// Resource name (compact string)
		const resourceName = d.readCompactString()

		// Pattern type (INT8)
		const patternType = d.readInt8() as AclResourcePatternType

		// ACLs (compact array)
		const acls = d.readCompactArray(ad => {
			// Principal (compact string)
			const principal = ad.readCompactString()

			// Host (compact string)
			const host = ad.readCompactString()

			// Operation (INT8)
			const operation = ad.readInt8() as AclOperation

			// Permission type (INT8)
			const permissionType = ad.readInt8() as AclPermissionType

			// ACL-level tagged fields
			ad.skipTaggedFields()

			return {
				principal,
				host,
				operation,
				permissionType,
			}
		})

		// Resource-level tagged fields
		d.skipTaggedFields()

		return {
			resourceType,
			resourceName,
			patternType,
			acls,
		}
	})

	// Response-level tagged fields
	decoder.skipTaggedFields()

	return {
		throttleTimeMs,
		errorCode,
		errorMessage,
		resources,
	}
}

/**
 * Check if a DescribeAcls response has an error
 *
 * @param response - The DescribeAcls response
 * @returns true if there is an error
 */
export function hasDescribeAclsError(response: DescribeAclsResponse): boolean {
	return response.errorCode !== ErrorCode.None
}
