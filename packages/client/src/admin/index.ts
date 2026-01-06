/**
 * Admin module exports
 */

export { Admin } from './admin.js'
export type {
	AdminConfig,
	TopicDescription,
	PartitionInfo,
	ConsumerGroupListing,
	ConsumerGroupDescription,
	MemberDescription,
	TopicPartition,
	ClusterDescription,
	BrokerDescription,
	CreateTopicsResult,
	DeleteTopicsResult,
	DeleteGroupsResult,
	// ACL types
	AclBinding,
	AclBindingFilter,
	AclEntry,
	AclResource,
	DescribeAclsResult,
	CreateAclResult,
	DeletedAcl,
	DeleteAclsFilterResult,
} from './types.js'

// Re-export ACL enums
export { AclResourceType, AclResourcePatternType, AclOperation, AclPermissionType } from './types.js'
