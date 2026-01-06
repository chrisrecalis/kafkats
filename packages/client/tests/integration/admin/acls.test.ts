import { describe, expect, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Admin - ACLs (integration)', () => {
	it('creates and describes ACLs', async () => {
		const client = createClient('admin-create-acls')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('acl-topic')
		const principal = 'User:test-user'

		// Create an ACL
		const createResults = await admin.createAcls([
			{
				resourceType: AclResourceType.TOPIC,
				resourceName: topicName,
				resourcePatternType: AclResourcePatternType.LITERAL,
				principal,
				host: '*',
				operation: AclOperation.READ,
				permissionType: AclPermissionType.ALLOW,
			},
		])

		expect(createResults).toHaveLength(1)
		expect(createResults[0]!.errorCode).toBe(ErrorCode.None)

		// Describe the ACL
		await vi.waitFor(
			async () => {
				const describeResult = await admin.describeAcls({
					resourceTypeFilter: AclResourceType.TOPIC,
					resourceNameFilter: topicName,
					patternTypeFilter: AclResourcePatternType.LITERAL,
					principalFilter: principal,
					hostFilter: null,
					operation: AclOperation.ANY,
					permissionType: AclPermissionType.ANY,
				})

				expect(describeResult.errorCode).toBe(ErrorCode.None)
				expect(describeResult.resources).toHaveLength(1)

				const resource = describeResult.resources[0]!
				expect(resource.resourceType).toBe(AclResourceType.TOPIC)
				expect(resource.resourceName).toBe(topicName)
				expect(resource.patternType).toBe(AclResourcePatternType.LITERAL)
				expect(resource.acls).toHaveLength(1)

				const acl = resource.acls[0]!
				expect(acl.principal).toBe(principal)
				expect(acl.host).toBe('*')
				expect(acl.operation).toBe(AclOperation.READ)
				expect(acl.permissionType).toBe(AclPermissionType.ALLOW)
			},
			{ timeout: 30_000 }
		)

		// Cleanup: delete the ACL
		await admin.deleteAcls([
			{
				resourceTypeFilter: AclResourceType.TOPIC,
				resourceNameFilter: topicName,
				patternTypeFilter: AclResourcePatternType.LITERAL,
				principalFilter: principal,
				hostFilter: '*',
				operation: AclOperation.READ,
				permissionType: AclPermissionType.ALLOW,
			},
		])

		await client.disconnect()
	})

	it('deletes ACLs and returns matching ACLs', async () => {
		const client = createClient('admin-delete-acls')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('acl-delete-topic')
		const principal = 'User:delete-test-user'

		// Create multiple ACLs
		const createResults = await admin.createAcls([
			{
				resourceType: AclResourceType.TOPIC,
				resourceName: topicName,
				resourcePatternType: AclResourcePatternType.LITERAL,
				principal,
				host: '*',
				operation: AclOperation.READ,
				permissionType: AclPermissionType.ALLOW,
			},
			{
				resourceType: AclResourceType.TOPIC,
				resourceName: topicName,
				resourcePatternType: AclResourcePatternType.LITERAL,
				principal,
				host: '*',
				operation: AclOperation.WRITE,
				permissionType: AclPermissionType.ALLOW,
			},
		])

		expect(createResults).toHaveLength(2)
		expect(createResults.every(r => r.errorCode === ErrorCode.None)).toBe(true)

		// Delete all ACLs for the principal on the topic
		const deleteResults = await admin.deleteAcls([
			{
				resourceTypeFilter: AclResourceType.TOPIC,
				resourceNameFilter: topicName,
				patternTypeFilter: AclResourcePatternType.LITERAL,
				principalFilter: principal,
				hostFilter: null,
				operation: AclOperation.ANY,
				permissionType: AclPermissionType.ANY,
			},
		])

		expect(deleteResults).toHaveLength(1)
		expect(deleteResults[0]!.errorCode).toBe(ErrorCode.None)
		expect(deleteResults[0]!.matchingAcls).toHaveLength(2)

		// Verify ACLs are deleted
		const describeResult = await admin.describeAcls({
			resourceTypeFilter: AclResourceType.TOPIC,
			resourceNameFilter: topicName,
			patternTypeFilter: AclResourcePatternType.LITERAL,
			principalFilter: principal,
			hostFilter: null,
			operation: AclOperation.ANY,
			permissionType: AclPermissionType.ANY,
		})

		expect(describeResult.errorCode).toBe(ErrorCode.None)
		expect(describeResult.resources).toHaveLength(0)

		await client.disconnect()
	})

	it('describes all ACLs with ANY filter', async () => {
		const client = createClient('admin-describe-all-acls')
		await client.connect()

		const admin = client.admin()

		// Describe all ACLs (may return any ACLs in the cluster)
		const describeResult = await admin.describeAcls({
			resourceTypeFilter: AclResourceType.ANY,
			resourceNameFilter: null,
			patternTypeFilter: AclResourcePatternType.ANY,
			principalFilter: null,
			hostFilter: null,
			operation: AclOperation.ANY,
			permissionType: AclPermissionType.ANY,
		})

		expect(describeResult.errorCode).toBe(ErrorCode.None)
		// resources may be empty if no ACLs exist in the cluster
		expect(Array.isArray(describeResult.resources)).toBe(true)

		await client.disconnect()
	})

	it('creates ACL with PREFIXED pattern type', async () => {
		const client = createClient('admin-prefixed-acl')
		await client.connect()

		const admin = client.admin()
		const topicPrefix = uniqueName('acl-prefix-')
		const principal = 'User:prefix-test-user'

		// Create a prefixed ACL
		const createResults = await admin.createAcls([
			{
				resourceType: AclResourceType.TOPIC,
				resourceName: topicPrefix,
				resourcePatternType: AclResourcePatternType.PREFIXED,
				principal,
				host: '*',
				operation: AclOperation.DESCRIBE,
				permissionType: AclPermissionType.ALLOW,
			},
		])

		expect(createResults).toHaveLength(1)
		expect(createResults[0]!.errorCode).toBe(ErrorCode.None)

		// Describe the prefixed ACL
		await vi.waitFor(
			async () => {
				const describeResult = await admin.describeAcls({
					resourceTypeFilter: AclResourceType.TOPIC,
					resourceNameFilter: topicPrefix,
					patternTypeFilter: AclResourcePatternType.PREFIXED,
					principalFilter: principal,
					hostFilter: null,
					operation: AclOperation.ANY,
					permissionType: AclPermissionType.ANY,
				})

				expect(describeResult.errorCode).toBe(ErrorCode.None)
				expect(describeResult.resources).toHaveLength(1)

				const resource = describeResult.resources[0]!
				expect(resource.patternType).toBe(AclResourcePatternType.PREFIXED)
			},
			{ timeout: 30_000 }
		)

		// Cleanup
		await admin.deleteAcls([
			{
				resourceTypeFilter: AclResourceType.TOPIC,
				resourceNameFilter: topicPrefix,
				patternTypeFilter: AclResourcePatternType.PREFIXED,
				principalFilter: principal,
				hostFilter: null,
				operation: AclOperation.ANY,
				permissionType: AclPermissionType.ANY,
			},
		])

		await client.disconnect()
	})

	it('creates ACL for consumer group', async () => {
		const client = createClient('admin-group-acl')
		await client.connect()

		const admin = client.admin()
		const groupName = uniqueName('acl-group')
		const principal = 'User:group-test-user'

		// Create an ACL for a consumer group
		const createResults = await admin.createAcls([
			{
				resourceType: AclResourceType.GROUP,
				resourceName: groupName,
				resourcePatternType: AclResourcePatternType.LITERAL,
				principal,
				host: '*',
				operation: AclOperation.READ,
				permissionType: AclPermissionType.ALLOW,
			},
		])

		expect(createResults).toHaveLength(1)
		expect(createResults[0]!.errorCode).toBe(ErrorCode.None)

		// Describe the ACL
		await vi.waitFor(
			async () => {
				const describeResult = await admin.describeAcls({
					resourceTypeFilter: AclResourceType.GROUP,
					resourceNameFilter: groupName,
					patternTypeFilter: AclResourcePatternType.LITERAL,
					principalFilter: principal,
					hostFilter: null,
					operation: AclOperation.ANY,
					permissionType: AclPermissionType.ANY,
				})

				expect(describeResult.errorCode).toBe(ErrorCode.None)
				expect(describeResult.resources).toHaveLength(1)
				expect(describeResult.resources[0]!.resourceType).toBe(AclResourceType.GROUP)
			},
			{ timeout: 30_000 }
		)

		// Cleanup
		await admin.deleteAcls([
			{
				resourceTypeFilter: AclResourceType.GROUP,
				resourceNameFilter: groupName,
				patternTypeFilter: AclResourcePatternType.LITERAL,
				principalFilter: principal,
				hostFilter: null,
				operation: AclOperation.ANY,
				permissionType: AclPermissionType.ANY,
			},
		])

		await client.disconnect()
	})

	it('handles delete with no matching ACLs', async () => {
		const client = createClient('admin-delete-no-match')
		await client.connect()

		const admin = client.admin()
		const nonExistentTopic = uniqueName('acl-nonexistent')

		// Try to delete ACLs that don't exist
		const deleteResults = await admin.deleteAcls([
			{
				resourceTypeFilter: AclResourceType.TOPIC,
				resourceNameFilter: nonExistentTopic,
				patternTypeFilter: AclResourcePatternType.LITERAL,
				principalFilter: 'User:nonexistent',
				hostFilter: null,
				operation: AclOperation.ANY,
				permissionType: AclPermissionType.ANY,
			},
		])

		expect(deleteResults).toHaveLength(1)
		expect(deleteResults[0]!.errorCode).toBe(ErrorCode.None)
		expect(deleteResults[0]!.matchingAcls).toHaveLength(0)

		await client.disconnect()
	})
})
