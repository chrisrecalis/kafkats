/**
 * Metadata Response (API Key 3)
 *
 * Returns cluster topology information including brokers, topics, and partitions.
 *
 * Supports v4-v12:
 * - v4-v8: non-flexible encoding (INT32/INT16 length prefixes)
 * - v9+: flexible encoding (UVARINT compact strings, tagged fields)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Broker information
 */
export interface MetadataBroker {
	nodeId: number
	host: string
	port: number
	rack: string | null
}

/**
 * Partition information
 */
export interface MetadataPartition {
	errorCode: ErrorCode
	partitionIndex: number
	leaderId: number
	leaderEpoch: number
	replicaNodes: number[]
	isrNodes: number[]
	offlineReplicas: number[]
}

/**
 * Topic information
 */
export interface MetadataTopic {
	errorCode: ErrorCode
	name: string | null
	topicId: string
	isInternal: boolean
	partitions: MetadataPartition[]
	topicAuthorizedOperations: number
}

/**
 * Metadata response data
 */
export interface MetadataResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** List of brokers in the cluster */
	brokers: MetadataBroker[]
	/** Cluster ID */
	clusterId: string | null
	/** Controller broker ID */
	controllerId: number
	/** List of topics */
	topics: MetadataTopic[]
	/** Cluster authorized operations bitmask (v8+) */
	clusterAuthorizedOperations: number
}

/**
 * Decode a Metadata response
 *
 * Supports v4-v12:
 * - v4-v8: non-flexible encoding
 * - v9+: flexible encoding with compact strings and tagged fields
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeMetadataResponse(decoder: IDecoder, version: number): MetadataResponse {
	const flexible = isFlexibleVersion(ApiKey.Metadata, version)

	// Throttle time (v3+)
	const throttleTimeMs = version >= 3 ? decoder.readInt32() : 0

	// Brokers array
	const brokers: MetadataBroker[] = flexible
		? decoder.readCompactArray(d => {
				const nodeId = d.readInt32()
				const host = d.readCompactString()
				const port = d.readInt32()
				const rack = d.readCompactNullableString()
				d.skipTaggedFields()
				return { nodeId, host, port, rack }
			})
		: decodeBrokersNonFlexible(decoder, version)

	// Cluster ID (v2+)
	const clusterId =
		version >= 2 ? (flexible ? decoder.readCompactNullableString() : decoder.readNullableString()) : null

	// Controller ID (v1+)
	const controllerId = version >= 1 ? decoder.readInt32() : -1

	// Topics array
	const topics: MetadataTopic[] = flexible
		? decoder.readCompactArray(d => decodeTopicFlexible(d, version))
		: decodeTopicsNonFlexible(decoder, version)

	// Cluster authorized operations (v8-v10, removed in v11+)
	const clusterAuthorizedOperations = version >= 8 && version <= 10 ? decoder.readInt32() : 0

	// Skip response-level tagged fields (flexible only)
	if (flexible) {
		decoder.skipTaggedFields()
	}

	return {
		throttleTimeMs,
		brokers,
		clusterId,
		controllerId,
		topics,
		clusterAuthorizedOperations,
	}
}

/**
 * Decode brokers array for non-flexible versions
 */
function decodeBrokersNonFlexible(decoder: IDecoder, version: number): MetadataBroker[] {
	const brokerCount = decoder.readInt32()
	const brokers: MetadataBroker[] = []
	for (let i = 0; i < brokerCount; i++) {
		const nodeId = decoder.readInt32()
		const host = decoder.readString()
		const port = decoder.readInt32()
		const rack = version >= 1 ? decoder.readNullableString() : null
		brokers.push({ nodeId, host, port, rack })
	}
	return brokers
}

/**
 * Decode topics array for non-flexible versions
 */
function decodeTopicsNonFlexible(decoder: IDecoder, version: number): MetadataTopic[] {
	const topicCount = decoder.readInt32()
	const topics: MetadataTopic[] = []
	for (let i = 0; i < topicCount; i++) {
		const errorCode = decoder.readInt16() as ErrorCode
		const name = decoder.readNullableString()
		const topicId = '00000000-0000-0000-0000-000000000000' // Not available in non-flexible versions
		const isInternal = version >= 1 ? decoder.readBoolean() : false
		const partitions = decodePartitionsNonFlexible(decoder, version)
		const topicAuthorizedOperations = version >= 8 ? decoder.readInt32() : 0

		topics.push({
			errorCode,
			name,
			topicId,
			isInternal,
			partitions,
			topicAuthorizedOperations,
		})
	}
	return topics
}

/**
 * Decode a single topic for flexible versions
 */
function decodeTopicFlexible(decoder: IDecoder, version: number): MetadataTopic {
	const errorCode = decoder.readInt16() as ErrorCode
	const name = decoder.readCompactNullableString()

	// Topic ID (v10+): UUID
	const topicId = version >= 10 ? decoder.readUUID() : '00000000-0000-0000-0000-000000000000'

	const isInternal = decoder.readBoolean()

	// Partitions array
	const partitions = decoder.readCompactArray(d => decodePartitionFlexible(d, version))

	// Topic authorized operations (v8+)
	const topicAuthorizedOperations = version >= 8 ? decoder.readInt32() : 0

	// Skip tagged fields for this topic
	decoder.skipTaggedFields()

	return {
		errorCode,
		name,
		topicId,
		isInternal,
		partitions,
		topicAuthorizedOperations,
	}
}

/**
 * Decode partitions array for non-flexible versions
 */
function decodePartitionsNonFlexible(decoder: IDecoder, version: number): MetadataPartition[] {
	const partitionCount = decoder.readInt32()
	const partitions: MetadataPartition[] = []
	for (let j = 0; j < partitionCount; j++) {
		const partitionErrorCode = decoder.readInt16() as ErrorCode
		const partitionIndex = decoder.readInt32()
		const leaderId = decoder.readInt32()
		const leaderEpoch = version >= 7 ? decoder.readInt32() : -1

		// Replica nodes
		const replicaCount = decoder.readInt32()
		const replicaNodes: number[] = []
		for (let k = 0; k < replicaCount; k++) {
			replicaNodes.push(decoder.readInt32())
		}

		// ISR nodes
		const isrCount = decoder.readInt32()
		const isrNodes: number[] = []
		for (let k = 0; k < isrCount; k++) {
			isrNodes.push(decoder.readInt32())
		}

		// Offline replicas (v5+)
		const offlineReplicas: number[] = []
		if (version >= 5) {
			const offlineCount = decoder.readInt32()
			for (let k = 0; k < offlineCount; k++) {
				offlineReplicas.push(decoder.readInt32())
			}
		}

		partitions.push({
			errorCode: partitionErrorCode,
			partitionIndex,
			leaderId,
			leaderEpoch,
			replicaNodes,
			isrNodes,
			offlineReplicas,
		})
	}
	return partitions
}

/**
 * Decode a single partition for flexible versions
 */
function decodePartitionFlexible(decoder: IDecoder, version: number): MetadataPartition {
	const errorCode = decoder.readInt16() as ErrorCode
	const partitionIndex = decoder.readInt32()
	const leaderId = decoder.readInt32()
	const leaderEpoch = version >= 7 ? decoder.readInt32() : -1

	// Replica nodes (compact array of INT32)
	const replicaNodes = decoder.readCompactArray(d => d.readInt32())

	// ISR nodes (compact array of INT32)
	const isrNodes = decoder.readCompactArray(d => d.readInt32())

	// Offline replicas (v5+, compact array)
	const offlineReplicas = version >= 5 ? decoder.readCompactArray(d => d.readInt32()) : []

	// Skip tagged fields for this partition
	decoder.skipTaggedFields()

	return {
		errorCode,
		partitionIndex,
		leaderId,
		leaderEpoch,
		replicaNodes,
		isrNodes,
		offlineReplicas,
	}
}

/**
 * Find a topic by name in the response
 *
 * @param response - The metadata response
 * @param topicName - The topic name to find
 * @returns The topic metadata or undefined
 */
export function findTopic(response: MetadataResponse, topicName: string): MetadataTopic | undefined {
	return response.topics.find(t => t.name === topicName)
}

/**
 * Find a broker by ID in the response
 *
 * @param response - The metadata response
 * @param nodeId - The node ID to find
 * @returns The broker metadata or undefined
 */
export function findBroker(response: MetadataResponse, nodeId: number): MetadataBroker | undefined {
	return response.brokers.find(b => b.nodeId === nodeId)
}

/**
 * Find the leader broker for a partition
 *
 * @param response - The metadata response
 * @param topicName - The topic name
 * @param partitionIndex - The partition index
 * @returns The leader broker or undefined
 */
export function findPartitionLeader(
	response: MetadataResponse,
	topicName: string,
	partitionIndex: number
): MetadataBroker | undefined {
	const topic = findTopic(response, topicName)
	if (!topic) return undefined

	const partition = topic.partitions.find(p => p.partitionIndex === partitionIndex)
	if (!partition) return undefined

	return findBroker(response, partition.leaderId)
}
