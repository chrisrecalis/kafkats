/**
 * DeleteAcls Response (API Key 31)
 *
 * Returns the result of deleting ACLs.
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
 * A matching ACL that was deleted
 */
export interface DeleteAclsMatchingAcl {
	/** Error code for this ACL deletion (None if successful) */
	errorCode: ErrorCode
	/** Error message (null if no error) */
	errorMessage: string | null
	/** Resource type */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type */
	patternType: AclResourcePatternType
	/** Principal */
	principal: string
	/** Host */
	host: string
	/** Operation */
	operation: AclOperation
	/** Permission type */
	permissionType: AclPermissionType
}

/**
 * Result for a single deletion filter
 */
export interface DeleteAclsFilterResult {
	/** Error code for this filter (None if successful) */
	errorCode: ErrorCode
	/** Error message (null if no error) */
	errorMessage: string | null
	/** ACLs that matched and were deleted */
	matchingAcls: DeleteAclsMatchingAcl[]
}

/**
 * DeleteAcls response data
 */
export interface DeleteAclsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each deletion filter */
	filterResults: DeleteAclsFilterResult[]
}

/**
 * Decode a DeleteAcls response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDeleteAclsResponse(decoder: IDecoder, version: number): DeleteAclsResponse {
	const flexible = isFlexibleVersion(ApiKey.DeleteAcls, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteAcls versions (< 2) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Filter results (compact array)
	const filterResults = decoder.readCompactArray(d => {
		// Error code (INT16)
		const errorCode = d.readInt16() as ErrorCode

		// Error message (compact nullable string)
		const errorMessage = d.readCompactNullableString()

		// Matching ACLs (compact array)
		const matchingAcls = d.readCompactArray(md => {
			// Error code (INT16)
			const aclErrorCode = md.readInt16() as ErrorCode

			// Error message (compact nullable string)
			const aclErrorMessage = md.readCompactNullableString()

			// Resource type (INT8)
			const resourceType = md.readInt8() as AclResourceType

			// Resource name (compact string)
			const resourceName = md.readCompactString()

			// Pattern type (INT8)
			const patternType = md.readInt8() as AclResourcePatternType

			// Principal (compact string)
			const principal = md.readCompactString()

			// Host (compact string)
			const host = md.readCompactString()

			// Operation (INT8)
			const operation = md.readInt8() as AclOperation

			// Permission type (INT8)
			const permissionType = md.readInt8() as AclPermissionType

			// ACL-level tagged fields
			md.skipTaggedFields()

			return {
				errorCode: aclErrorCode,
				errorMessage: aclErrorMessage,
				resourceType,
				resourceName,
				patternType,
				principal,
				host,
				operation,
				permissionType,
			}
		})

		// Filter result-level tagged fields
		d.skipTaggedFields()

		return {
			errorCode,
			errorMessage,
			matchingAcls,
		}
	})

	// Response-level tagged fields
	decoder.skipTaggedFields()

	return {
		throttleTimeMs,
		filterResults,
	}
}

/**
 * Check if a DeleteAcls response has any errors
 *
 * @param response - The DeleteAcls response
 * @returns true if any filter or matching ACL has an error
 */
export function hasDeleteAclsErrors(response: DeleteAclsResponse): boolean {
	return response.filterResults.some(
		fr => fr.errorCode !== ErrorCode.None || fr.matchingAcls.some(ma => ma.errorCode !== ErrorCode.None)
	)
}

/**
 * Get all errors from a DeleteAcls response
 *
 * @param response - The DeleteAcls response
 * @returns Array of errors with filter index and optional ACL index
 */
export function getDeleteAclsErrors(
	response: DeleteAclsResponse
): Array<{ filterIndex: number; aclIndex?: number; errorCode: ErrorCode; errorMessage: string | null }> {
	const errors: Array<{ filterIndex: number; aclIndex?: number; errorCode: ErrorCode; errorMessage: string | null }> =
		[]

	for (let fi = 0; fi < response.filterResults.length; fi++) {
		const fr = response.filterResults[fi]!

		// Filter-level error
		if (fr.errorCode !== ErrorCode.None) {
			errors.push({
				filterIndex: fi,
				errorCode: fr.errorCode,
				errorMessage: fr.errorMessage,
			})
		}

		// ACL-level errors
		for (let ai = 0; ai < fr.matchingAcls.length; ai++) {
			const ma = fr.matchingAcls[ai]!
			if (ma.errorCode !== ErrorCode.None) {
				errors.push({
					filterIndex: fi,
					aclIndex: ai,
					errorCode: ma.errorCode,
					errorMessage: ma.errorMessage,
				})
			}
		}
	}

	return errors
}
