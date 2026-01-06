/**
 * Admin client types
 */

import type { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Admin client configuration
 */
export interface AdminConfig {
	/** Timeout for admin operations in milliseconds (default: 30000) */
	requestTimeoutMs?: number
}

/**
 * Resolved admin configuration with defaults applied
 */
export interface ResolvedAdminConfig {
	requestTimeoutMs: number
}

/**
 * Partition information
 */
export interface PartitionInfo {
	/** Partition index */
	partitionIndex: number
	/** Leader broker ID */
	leaderId: number
	/** Leader epoch */
	leaderEpoch: number
	/** Replica broker IDs */
	replicas: number[]
	/** In-sync replica broker IDs */
	isr: number[]
	/** Offline replica broker IDs */
	offlineReplicas: number[]
}

/**
 * Topic description with metadata
 */
export interface TopicDescription {
	/** Topic name */
	name: string
	/** Topic UUID */
	topicId: string
	/** Whether this is an internal Kafka topic */
	isInternal: boolean
	/** Partition information */
	partitions: PartitionInfo[]
}

/**
 * Consumer group listing entry
 */
export interface ConsumerGroupListing {
	/** Group ID */
	groupId: string
	/** Protocol type (e.g., "consumer") */
	protocolType: string
	/** Group state */
	state: string
}

/**
 * Topic-partition assignment
 */
export interface TopicPartition {
	/** Topic name */
	topic: string
	/** Partition index */
	partition: number
}

/**
 * Consumer group member description
 */
export interface MemberDescription {
	/** Member ID assigned by coordinator */
	memberId: string
	/** Group instance ID for static membership */
	groupInstanceId: string | null
	/** Client ID */
	clientId: string
	/** Client host */
	clientHost: string
	/** Assigned partitions */
	assignment: TopicPartition[]
}

/**
 * Consumer group description
 */
export interface ConsumerGroupDescription {
	/** Group ID */
	groupId: string
	/** Group state (e.g., "Stable", "Empty", "Dead") */
	state: string
	/** Protocol type (e.g., "consumer") */
	protocolType: string
	/** Protocol name (e.g., assignor name) */
	protocol: string
	/** Group members */
	members: MemberDescription[]
	/** Error code if group lookup failed */
	errorCode: ErrorCode
}

/**
 * Broker description
 */
export interface BrokerDescription {
	/** Broker node ID */
	nodeId: number
	/** Broker host */
	host: string
	/** Broker port */
	port: number
	/** Broker rack */
	rack: string | null
}

/**
 * Cluster description
 */
export interface ClusterDescription {
	/** Cluster ID */
	clusterId: string | null
	/** Controller broker ID */
	controllerId: number
	/** Broker information */
	brokers: BrokerDescription[]
}

/**
 * Result of a delete topics operation
 */
export interface DeleteTopicsResult {
	/** Topic name */
	name: string | null
	/** Error code */
	errorCode: ErrorCode
	/** Error message */
	errorMessage: string | null
}

/**
 * Result of a delete groups operation
 */
export interface DeleteGroupsResult {
	/** Group ID */
	groupId: string
	/** Error code */
	errorCode: ErrorCode
}

/**
 * Result of a create topics operation
 */
export interface CreateTopicsResult {
	/** Topic name */
	name: string
	/** Topic ID */
	topicId: string
	/** Error code */
	errorCode: ErrorCode
	/** Error message */
	errorMessage: string | null
	/** Number of partitions */
	numPartitions: number
	/** Replication factor */
	replicationFactor: number
}

// Re-export ACL enums for convenience
export {
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'

import type {
	AclResourceType,
	AclResourcePatternType,
	AclOperation,
	AclPermissionType,
} from '@/protocol/messages/requests/describe-acls.js'

/**
 * ACL binding representing a single access control entry
 */
export interface AclBinding {
	/** Resource type (TOPIC, GROUP, CLUSTER, etc.) */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type (LITERAL, PREFIXED) */
	resourcePatternType: AclResourcePatternType
	/** Principal (e.g., "User:alice") */
	principal: string
	/** Host (use "*" for all hosts) */
	host: string
	/** Operation (READ, WRITE, CREATE, etc.) */
	operation: AclOperation
	/** Permission type (ALLOW or DENY) */
	permissionType: AclPermissionType
}

/**
 * ACL filter for describe and delete operations
 */
export interface AclBindingFilter {
	/** Resource type filter (use ANY to match all) */
	resourceTypeFilter: AclResourceType
	/** Resource name filter (null matches any) */
	resourceNameFilter: string | null
	/** Resource pattern type filter (use ANY to match all) */
	patternTypeFilter: AclResourcePatternType
	/** Principal filter (null matches any) */
	principalFilter: string | null
	/** Host filter (null matches any) */
	hostFilter: string | null
	/** Operation filter (use ANY to match all) */
	operation: AclOperation
	/** Permission type filter (use ANY to match all) */
	permissionType: AclPermissionType
}

/**
 * ACL entry in a resource
 */
export interface AclEntry {
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
 * Resource with its ACLs
 */
export interface AclResource {
	/** Resource type */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type */
	patternType: AclResourcePatternType
	/** ACL entries for this resource */
	acls: AclEntry[]
}

/**
 * Result of a describe ACLs operation
 */
export interface DescribeAclsResult {
	/** Error code (None if successful) */
	errorCode: ErrorCode
	/** Error message */
	errorMessage: string | null
	/** Resources with their ACLs */
	resources: AclResource[]
}

/**
 * Result of a single ACL creation
 */
export interface CreateAclResult {
	/** Error code (None if successful) */
	errorCode: ErrorCode
	/** Error message */
	errorMessage: string | null
}

/**
 * A matching ACL that was deleted
 */
export interface DeletedAcl {
	/** Resource type */
	resourceType: AclResourceType
	/** Resource name */
	resourceName: string
	/** Resource pattern type */
	resourcePatternType: AclResourcePatternType
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
 * Result of a delete ACLs filter
 */
export interface DeleteAclsFilterResult {
	/** Error code (None if successful) */
	errorCode: ErrorCode
	/** Error message */
	errorMessage: string | null
	/** ACLs that matched and were deleted */
	matchingAcls: DeletedAcl[]
}
