import { describe, expect, it } from 'vitest'

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

import {
	encodeDescribeAclsRequest,
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'
import { encodeCreateAclsRequest } from '@/protocol/messages/requests/create-acls.js'
import { encodeDeleteAclsRequest } from '@/protocol/messages/requests/delete-acls.js'
import { decodeDescribeAclsResponse } from '@/protocol/messages/responses/describe-acls.js'
import { decodeCreateAclsResponse } from '@/protocol/messages/responses/create-acls.js'
import { decodeDeleteAclsResponse } from '@/protocol/messages/responses/delete-acls.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('DescribeAcls request codec', () => {
	it('encodes DescribeAclsRequest v2 with all filter fields', () => {
		const enc = new Encoder()
		encodeDescribeAclsRequest(enc, 2, {
			resourceTypeFilter: AclResourceType.TOPIC,
			resourceNameFilter: 'my-topic',
			patternTypeFilter: AclResourcePatternType.LITERAL,
			principalFilter: 'User:alice',
			hostFilter: '*',
			operation: AclOperation.READ,
			permissionType: AclPermissionType.ALLOW,
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(AclResourceType.TOPIC) // resourceTypeFilter
		expect(dec.readCompactNullableString()).toBe('my-topic') // resourceNameFilter
		expect(dec.readInt8()).toBe(AclResourcePatternType.LITERAL) // patternTypeFilter
		expect(dec.readCompactNullableString()).toBe('User:alice') // principalFilter
		expect(dec.readCompactNullableString()).toBe('*') // hostFilter
		expect(dec.readInt8()).toBe(AclOperation.READ) // operation
		expect(dec.readInt8()).toBe(AclPermissionType.ALLOW) // permissionType
		expect(dec.readUVarInt()).toBe(0) // tagged fields
		expect(dec.remaining()).toBe(0)
	})

	it('encodes DescribeAclsRequest v3 with null filters for wildcard matching', () => {
		const enc = new Encoder()
		encodeDescribeAclsRequest(enc, 3, {
			resourceTypeFilter: AclResourceType.ANY,
			resourceNameFilter: null,
			patternTypeFilter: AclResourcePatternType.ANY,
			principalFilter: null,
			hostFilter: null,
			operation: AclOperation.ANY,
			permissionType: AclPermissionType.ANY,
		})

		const dec = new Decoder(enc.toBuffer())
		expect(dec.readInt8()).toBe(AclResourceType.ANY)
		expect(dec.readCompactNullableString()).toBeNull()
		expect(dec.readInt8()).toBe(AclResourcePatternType.ANY)
		expect(dec.readCompactNullableString()).toBeNull()
		expect(dec.readCompactNullableString()).toBeNull()
		expect(dec.readInt8()).toBe(AclOperation.ANY)
		expect(dec.readInt8()).toBe(AclPermissionType.ANY)
		expect(dec.readUVarInt()).toBe(0)
		expect(dec.remaining()).toBe(0)
	})
})

describe('DescribeAcls response codec', () => {
	it('decodes DescribeAclsResponse v2 with resources', () => {
		const enc = new Encoder()
		// throttleTimeMs
		enc.writeInt32(0)
		// errorCode
		enc.writeInt16(ErrorCode.None)
		// errorMessage (null)
		enc.writeCompactNullableString(null)
		// resources array (1 resource)
		enc.writeCompactArray(
			[
				{
					resourceType: AclResourceType.TOPIC,
					resourceName: 'my-topic',
					patternType: AclResourcePatternType.LITERAL,
					acls: [
						{
							principal: 'User:alice',
							host: '*',
							operation: AclOperation.READ,
							permissionType: AclPermissionType.ALLOW,
						},
					],
				},
			],
			(resource, renc) => {
				renc.writeInt8(resource.resourceType)
				renc.writeCompactString(resource.resourceName)
				renc.writeInt8(resource.patternType)
				renc.writeCompactArray(resource.acls, (acl, aenc) => {
					aenc.writeCompactString(acl.principal)
					aenc.writeCompactString(acl.host)
					aenc.writeInt8(acl.operation)
					aenc.writeInt8(acl.permissionType)
					aenc.writeEmptyTaggedFields()
				})
				renc.writeEmptyTaggedFields()
			}
		)
		enc.writeEmptyTaggedFields()

		const response = decodeDescribeAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.throttleTimeMs).toBe(0)
		expect(response.errorCode).toBe(ErrorCode.None)
		expect(response.errorMessage).toBeNull()
		expect(response.resources).toHaveLength(1)
		expect(response.resources[0]).toEqual({
			resourceType: AclResourceType.TOPIC,
			resourceName: 'my-topic',
			patternType: AclResourcePatternType.LITERAL,
			acls: [
				{
					principal: 'User:alice',
					host: '*',
					operation: AclOperation.READ,
					permissionType: AclPermissionType.ALLOW,
				},
			],
		})
	})

	it('decodes DescribeAclsResponse v2 with error', () => {
		const enc = new Encoder()
		enc.writeInt32(100) // throttleTimeMs
		enc.writeInt16(ErrorCode.ClusterAuthorizationFailed)
		enc.writeCompactNullableString('Not authorized')
		enc.writeCompactArray([], () => {}) // empty resources
		enc.writeEmptyTaggedFields()

		const response = decodeDescribeAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.throttleTimeMs).toBe(100)
		expect(response.errorCode).toBe(ErrorCode.ClusterAuthorizationFailed)
		expect(response.errorMessage).toBe('Not authorized')
		expect(response.resources).toHaveLength(0)
	})
})

describe('CreateAcls request codec', () => {
	it('encodes CreateAclsRequest v2 with multiple ACLs', () => {
		const enc = new Encoder()
		encodeCreateAclsRequest(enc, 2, {
			creations: [
				{
					resourceType: AclResourceType.TOPIC,
					resourceName: 'topic1',
					resourcePatternType: AclResourcePatternType.LITERAL,
					principal: 'User:alice',
					host: '*',
					operation: AclOperation.READ,
					permissionType: AclPermissionType.ALLOW,
				},
				{
					resourceType: AclResourceType.GROUP,
					resourceName: 'group1',
					resourcePatternType: AclResourcePatternType.LITERAL,
					principal: 'User:bob',
					host: '192.168.1.1',
					operation: AclOperation.ALL,
					permissionType: AclPermissionType.ALLOW,
				},
			],
		})

		const dec = new Decoder(enc.toBuffer())

		// Read creations array
		const creations = dec.readCompactArray(d => {
			const resourceType = d.readInt8()
			const resourceName = d.readCompactString()
			const resourcePatternType = d.readInt8()
			const principal = d.readCompactString()
			const host = d.readCompactString()
			const operation = d.readInt8()
			const permissionType = d.readInt8()
			d.skipTaggedFields()
			return { resourceType, resourceName, resourcePatternType, principal, host, operation, permissionType }
		})

		expect(creations).toHaveLength(2)
		expect(creations[0]).toEqual({
			resourceType: AclResourceType.TOPIC,
			resourceName: 'topic1',
			resourcePatternType: AclResourcePatternType.LITERAL,
			principal: 'User:alice',
			host: '*',
			operation: AclOperation.READ,
			permissionType: AclPermissionType.ALLOW,
		})
		expect(creations[1]).toEqual({
			resourceType: AclResourceType.GROUP,
			resourceName: 'group1',
			resourcePatternType: AclResourcePatternType.LITERAL,
			principal: 'User:bob',
			host: '192.168.1.1',
			operation: AclOperation.ALL,
			permissionType: AclPermissionType.ALLOW,
		})

		expect(dec.readUVarInt()).toBe(0) // tagged fields
		expect(dec.remaining()).toBe(0)
	})
})

describe('CreateAcls response codec', () => {
	it('decodes CreateAclsResponse v2 with success results', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeCompactArray(
			[
				{ errorCode: ErrorCode.None, errorMessage: null },
				{ errorCode: ErrorCode.None, errorMessage: null },
			],
			(result, renc) => {
				renc.writeInt16(result.errorCode)
				renc.writeCompactNullableString(result.errorMessage)
				renc.writeEmptyTaggedFields()
			}
		)
		enc.writeEmptyTaggedFields()

		const response = decodeCreateAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.throttleTimeMs).toBe(0)
		expect(response.results).toHaveLength(2)
		expect(response.results[0]).toEqual({ errorCode: ErrorCode.None, errorMessage: null })
		expect(response.results[1]).toEqual({ errorCode: ErrorCode.None, errorMessage: null })
	})

	it('decodes CreateAclsResponse v2 with mixed results', () => {
		const enc = new Encoder()
		enc.writeInt32(50)
		enc.writeCompactArray(
			[
				{ errorCode: ErrorCode.None, errorMessage: null },
				{ errorCode: ErrorCode.InvalidRequest, errorMessage: 'Invalid principal' },
			],
			(result, renc) => {
				renc.writeInt16(result.errorCode)
				renc.writeCompactNullableString(result.errorMessage)
				renc.writeEmptyTaggedFields()
			}
		)
		enc.writeEmptyTaggedFields()

		const response = decodeCreateAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.throttleTimeMs).toBe(50)
		expect(response.results).toHaveLength(2)
		expect(response.results[0]!.errorCode).toBe(ErrorCode.None)
		expect(response.results[1]!.errorCode).toBe(ErrorCode.InvalidRequest)
		expect(response.results[1]!.errorMessage).toBe('Invalid principal')
	})
})

describe('DeleteAcls request codec', () => {
	it('encodes DeleteAclsRequest v2 with filter', () => {
		const enc = new Encoder()
		encodeDeleteAclsRequest(enc, 2, {
			filters: [
				{
					resourceTypeFilter: AclResourceType.TOPIC,
					resourceNameFilter: 'my-topic',
					patternTypeFilter: AclResourcePatternType.LITERAL,
					principalFilter: 'User:alice',
					hostFilter: null,
					operation: AclOperation.ANY,
					permissionType: AclPermissionType.ANY,
				},
			],
		})

		const dec = new Decoder(enc.toBuffer())

		const filters = dec.readCompactArray(d => {
			const resourceTypeFilter = d.readInt8()
			const resourceNameFilter = d.readCompactNullableString()
			const patternTypeFilter = d.readInt8()
			const principalFilter = d.readCompactNullableString()
			const hostFilter = d.readCompactNullableString()
			const operation = d.readInt8()
			const permissionType = d.readInt8()
			d.skipTaggedFields()
			return {
				resourceTypeFilter,
				resourceNameFilter,
				patternTypeFilter,
				principalFilter,
				hostFilter,
				operation,
				permissionType,
			}
		})

		expect(filters).toHaveLength(1)
		expect(filters[0]).toEqual({
			resourceTypeFilter: AclResourceType.TOPIC,
			resourceNameFilter: 'my-topic',
			patternTypeFilter: AclResourcePatternType.LITERAL,
			principalFilter: 'User:alice',
			hostFilter: null,
			operation: AclOperation.ANY,
			permissionType: AclPermissionType.ANY,
		})

		expect(dec.readUVarInt()).toBe(0)
		expect(dec.remaining()).toBe(0)
	})
})

describe('DeleteAcls response codec', () => {
	it('decodes DeleteAclsResponse v2 with matching ACLs', () => {
		const enc = new Encoder()
		enc.writeInt32(0) // throttleTimeMs
		enc.writeCompactArray(
			[
				{
					errorCode: ErrorCode.None,
					errorMessage: null,
					matchingAcls: [
						{
							errorCode: ErrorCode.None,
							errorMessage: null,
							resourceType: AclResourceType.TOPIC,
							resourceName: 'my-topic',
							patternType: AclResourcePatternType.LITERAL,
							principal: 'User:alice',
							host: '*',
							operation: AclOperation.READ,
							permissionType: AclPermissionType.ALLOW,
						},
					],
				},
			],
			(fr, frenc) => {
				frenc.writeInt16(fr.errorCode)
				frenc.writeCompactNullableString(fr.errorMessage)
				frenc.writeCompactArray(fr.matchingAcls, (ma, maenc) => {
					maenc.writeInt16(ma.errorCode)
					maenc.writeCompactNullableString(ma.errorMessage)
					maenc.writeInt8(ma.resourceType)
					maenc.writeCompactString(ma.resourceName)
					maenc.writeInt8(ma.patternType)
					maenc.writeCompactString(ma.principal)
					maenc.writeCompactString(ma.host)
					maenc.writeInt8(ma.operation)
					maenc.writeInt8(ma.permissionType)
					maenc.writeEmptyTaggedFields()
				})
				frenc.writeEmptyTaggedFields()
			}
		)
		enc.writeEmptyTaggedFields()

		const response = decodeDeleteAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.throttleTimeMs).toBe(0)
		expect(response.filterResults).toHaveLength(1)
		expect(response.filterResults[0]!.errorCode).toBe(ErrorCode.None)
		expect(response.filterResults[0]!.matchingAcls).toHaveLength(1)
		expect(response.filterResults[0]!.matchingAcls[0]).toEqual({
			errorCode: ErrorCode.None,
			errorMessage: null,
			resourceType: AclResourceType.TOPIC,
			resourceName: 'my-topic',
			patternType: AclResourcePatternType.LITERAL,
			principal: 'User:alice',
			host: '*',
			operation: AclOperation.READ,
			permissionType: AclPermissionType.ALLOW,
		})
	})

	it('decodes DeleteAclsResponse v2 with empty matching ACLs', () => {
		const enc = new Encoder()
		enc.writeInt32(0)
		enc.writeCompactArray([{ errorCode: ErrorCode.None, errorMessage: null, matchingAcls: [] }], (fr, frenc) => {
			frenc.writeInt16(fr.errorCode)
			frenc.writeCompactNullableString(fr.errorMessage)
			frenc.writeCompactArray(fr.matchingAcls, () => {})
			frenc.writeEmptyTaggedFields()
		})
		enc.writeEmptyTaggedFields()

		const response = decodeDeleteAclsResponse(new Decoder(enc.toBuffer()), 2)

		expect(response.filterResults).toHaveLength(1)
		expect(response.filterResults[0]!.matchingAcls).toHaveLength(0)
	})
})
