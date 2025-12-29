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
