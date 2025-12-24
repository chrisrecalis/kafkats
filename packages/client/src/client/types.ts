/**
 * Client-level types for Kafka client
 */

import type { TlsConfig, SaslConfig } from '@/network/types.js'
import type { ApiKey } from '@/protocol/messages/api-keys.js'
import type { Logger, LogLevel } from '@/logger.js'

/**
 * Kafka client configuration
 */
export interface KafkaClientConfig {
	/** Bootstrap broker addresses (format: "host:port") */
	brokers: string[]

	/** Client identifier sent to brokers */
	clientId: string

	/** Request timeout in milliseconds (default: 30000) */
	requestTimeoutMs?: number

	/** Connection timeout in milliseconds (default: 10000) */
	connectionTimeoutMs?: number

	/** Metadata refresh interval in milliseconds (default: 300000) */
	metadataRefreshIntervalMs?: number

	/** Maximum number of in-flight requests per connection (default: 5) */
	maxInFlightRequests?: number

	/** TLS configuration (optional) */
	tls?: TlsConfig

	/** SASL authentication configuration (optional) */
	sasl?: SaslConfig

	/** Logger instance (optional, defaults to no-op) */
	logger?: Logger

	/** Log level when using default logger (default: 'info') */
	logLevel?: LogLevel
}

/**
 * Broker configuration for internal use
 */
export interface BrokerConfig {
	host: string
	port: number
	nodeId: number
	clientId: string
	connectionTimeoutMs?: number
	requestTimeoutMs?: number
	maxInFlightRequests?: number
	tls?: TlsConfig
	sasl?: SaslConfig
	logger?: Logger
}

/**
 * Cached API version information per broker
 */
export interface BrokerApiVersions {
	nodeId: number
	versions: Map<ApiKey, VersionRange>
	fetchedAt: number
}

/**
 * Simple version range (min/max)
 */
export interface VersionRange {
	min: number
	max: number
}

/**
 * Cluster metadata state
 */
export interface ClusterMetadata {
	/** Cluster ID (may be null for older brokers) */
	clusterId: string | null

	/** Controller broker ID */
	controllerId: number

	/** Broker information by node ID */
	brokers: Map<number, BrokerInfo>

	/** Topic metadata by name */
	topics: Map<string, TopicMetadata>

	/** When metadata was last updated */
	updatedAt: number
}

/**
 * Broker information from metadata
 */
export interface BrokerInfo {
	nodeId: number
	host: string
	port: number
	rack: string | null
}

/**
 * Topic metadata
 */
export interface TopicMetadata {
	name: string
	topicId: string
	isInternal: boolean
	partitions: Map<number, PartitionMetadata>
}

/**
 * Partition metadata
 */
export interface PartitionMetadata {
	partitionIndex: number
	leaderId: number
	leaderEpoch: number
	replicaNodes: number[]
	isrNodes: number[]
	offlineReplicas: number[]
}

/**
 * Coordinator types
 */
export type CoordinatorType = 'GROUP' | 'TRANSACTION'

/**
 * Coordinator cache entry
 */
export interface CoordinatorEntry {
	type: CoordinatorType
	key: string
	nodeId: number
	host: string
	port: number
	fetchedAt: number
}

/**
 * Cluster configuration
 */
export interface ClusterConfig {
	/** Bootstrap broker addresses (format: "host:port") */
	brokers: string[]

	/** Client identifier */
	clientId: string

	/** Connection timeout in milliseconds */
	connectionTimeoutMs?: number

	/** Request timeout in milliseconds */
	requestTimeoutMs?: number

	/** Maximum in-flight requests per connection */
	maxInFlightRequests?: number

	/** TLS configuration */
	tls?: TlsConfig

	/** SASL authentication configuration */
	sasl?: SaslConfig

	/** Metadata refresh interval in milliseconds (default: 300000) */
	metadataRefreshIntervalMs?: number

	/** Logger instance */
	logger?: Logger
}

/**
 * Cluster events
 */
export interface ClusterEvents {
	metadataUpdate: [metadata: ClusterMetadata]
	brokerConnect: [nodeId: number, host: string, port: number]
	brokerDisconnect: [nodeId: number, error?: Error]
	error: [error: Error]
}
