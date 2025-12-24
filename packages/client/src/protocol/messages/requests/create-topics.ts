/**
 * CreateTopics Request (API Key 19)
 *
 * Used to create topics on the Kafka cluster.
 *
 * Supports v5-v7 (flexible encoding only):
 * - v5: Added validateOnly field
 * - v6: Added topic configs response
 * - v7: Added topicId in response
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Partition assignment for topic creation
 */
export interface CreateTopicsRequestAssignment {
	/** Partition index */
	partitionIndex: number
	/** Broker IDs to assign as replicas */
	brokerIds: number[]
}

/**
 * Configuration entry for topic creation
 */
export interface CreateTopicsRequestConfig {
	/** Configuration key name */
	name: string
	/** Configuration value (null for default) */
	value: string | null
}

/**
 * Topic to create
 */
export interface CreateTopicsRequestTopic {
	/** Topic name */
	name: string
	/** Number of partitions (-1 for broker default) */
	numPartitions: number
	/** Replication factor (-1 for broker default) */
	replicationFactor: number
	/** Manual partition-to-broker assignments (empty for automatic) */
	assignments: CreateTopicsRequestAssignment[]
	/** Topic configuration overrides */
	configs: CreateTopicsRequestConfig[]
}

/**
 * CreateTopics request data
 */
export interface CreateTopicsRequest {
	/** Topics to create */
	topics: CreateTopicsRequestTopic[]
	/** Timeout in milliseconds */
	timeoutMs: number
	/** If true, only validate the request without creating topics (v1+) */
	validateOnly?: boolean
}

/**
 * Supported API versions for CreateTopics request
 *
 * - v5-v7: flexible encoding with compact strings and tagged fields
 */
export const CREATE_TOPICS_VERSIONS = {
	min: 5,
	max: 7,
}

/**
 * Encode a CreateTopics request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeCreateTopicsRequest(encoder: IEncoder, version: number, request: CreateTopicsRequest): void {
	if (version < CREATE_TOPICS_VERSIONS.min || version > CREATE_TOPICS_VERSIONS.max) {
		throw new Error(`Unsupported CreateTopics version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.CreateTopics, version)

	if (!flexible) {
		throw new Error('Non-flexible CreateTopics versions (< 5) not supported')
	}

	// Topics array (compact array)
	encoder.writeCompactArray(request.topics, (topic, enc) => {
		// Topic name (compact string)
		enc.writeCompactString(topic.name)

		// Number of partitions
		enc.writeInt32(topic.numPartitions)

		// Replication factor
		enc.writeInt16(topic.replicationFactor)

		// Replica assignments (compact array)
		enc.writeCompactArray(topic.assignments, (assignment, aenc) => {
			aenc.writeInt32(assignment.partitionIndex)
			// Broker IDs (compact array of INT32)
			aenc.writeCompactArray(assignment.brokerIds, (brokerId, benc) => {
				benc.writeInt32(brokerId)
			})
			aenc.writeEmptyTaggedFields()
		})

		// Configs (compact array)
		enc.writeCompactArray(topic.configs, (config, cenc) => {
			cenc.writeCompactString(config.name)
			cenc.writeCompactNullableString(config.value)
			cenc.writeEmptyTaggedFields()
		})

		enc.writeEmptyTaggedFields()
	})

	// Timeout in milliseconds
	encoder.writeInt32(request.timeoutMs)

	// Validate only (v1+, always present in v5+)
	encoder.writeBoolean(request.validateOnly ?? false)

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Default CreateTopics request configuration
 */
export const CREATE_TOPICS_DEFAULTS = {
	timeoutMs: 30000,
	validateOnly: false,
} as const

/**
 * Helper to create a simple CreateTopics request
 *
 * @param topics - Topics to create with optional configuration
 * @returns A CreateTopicsRequest
 */
export function createCreateTopicsRequest(
	topics: Array<{
		name: string
		numPartitions?: number
		replicationFactor?: number
		configs?: Record<string, string>
	}>
): CreateTopicsRequest {
	return {
		topics: topics.map(topic => ({
			name: topic.name,
			numPartitions: topic.numPartitions ?? 1,
			replicationFactor: topic.replicationFactor ?? 1,
			assignments: [],
			configs: topic.configs ? Object.entries(topic.configs).map(([name, value]) => ({ name, value })) : [],
		})),
		timeoutMs: CREATE_TOPICS_DEFAULTS.timeoutMs,
		validateOnly: false,
	}
}
