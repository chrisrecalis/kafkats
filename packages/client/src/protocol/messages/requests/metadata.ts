/**
 * Metadata Request (API Key 3)
 *
 * Used to discover cluster topology (brokers, topics, partitions).
 *
 * Supports v4-v12:
 * - v4-v8: non-flexible encoding (INT32/INT16 length prefixes)
 * - v9+: flexible encoding (UVARINT compact strings, tagged fields)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Topic to request metadata for (non-flexible versions v4-v8)
 */
export interface MetadataRequestTopic {
	/** Topic name (required for non-flexible versions) */
	name: string
}

/**
 * Topic to request metadata for (flexible versions v9+)
 */
export interface MetadataRequestTopicFlexible {
	/** Topic ID (v10+, null to use name) */
	topicId?: string | null
	/** Topic name (null to use topicId in v10+) */
	name: string | null
}

/**
 * Metadata request data
 */
export interface MetadataRequest {
	/** Topics to fetch metadata for (null = all topics) */
	topics: (MetadataRequestTopic | MetadataRequestTopicFlexible)[] | null
	/** Allow auto topic creation (v4+) */
	allowAutoTopicCreation?: boolean
	/** Include cluster authorized operations (v8-v10, removed in v11+) */
	includeClusterAuthorizedOperations?: boolean
	/** Include topic authorized operations (v8+) */
	includeTopicAuthorizedOperations?: boolean
}

/**
 * Supported API versions for Metadata request
 *
 * - v4-v8: non-flexible encoding
 * - v9-v12: flexible encoding with compact strings and tagged fields
 */
export const METADATA_VERSIONS = {
	min: 4,
	max: 12,
}

/**
 * Encode a Metadata request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeMetadataRequest(encoder: IEncoder, version: number, request: MetadataRequest): void {
	if (version < METADATA_VERSIONS.min || version > METADATA_VERSIONS.max) {
		throw new Error(`Unsupported Metadata version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.Metadata, version)

	if (flexible) {
		// Flexible encoding (v9+): COMPACT_ARRAY with UVARINT length
		if (request.topics === null) {
			encoder.writeUVarInt(0) // compact null array (0 means null)
		} else {
			encoder.writeUVarInt(request.topics.length + 1) // compact array length + 1
			for (const topic of request.topics) {
				// Topic ID (v10+): UUID, null UUID (all zeros) for name-based lookup
				if (version >= 10) {
					const topicId = (topic as MetadataRequestTopicFlexible).topicId
					encoder.writeUUID(topicId ?? '00000000-0000-0000-0000-000000000000')
				}
				// Topic name: COMPACT_NULLABLE_STRING
				encoder.writeCompactNullableString(topic.name)
				// Tagged fields for each topic element
				encoder.writeUVarInt(0) // empty tagged fields
			}
		}

		// Allow auto topic creation (v4+)
		encoder.writeBoolean(request.allowAutoTopicCreation ?? true)

		// Include cluster authorized operations (v8-v10, removed in v11+)
		if (version >= 8 && version <= 10) {
			encoder.writeBoolean(request.includeClusterAuthorizedOperations ?? false)
		}

		// Include topic authorized operations (v8+)
		if (version >= 8) {
			encoder.writeBoolean(request.includeTopicAuthorizedOperations ?? false)
		}

		// Request-level tagged fields
		encoder.writeUVarInt(0) // empty tagged fields
	} else {
		// Non-flexible encoding (v4-v8): INT32 array length, INT16 string length
		if (request.topics === null) {
			encoder.writeInt32(-1) // null array
		} else {
			encoder.writeInt32(request.topics.length)
			for (const topic of request.topics) {
				// Topic name must be non-null for non-flexible versions
				if (topic.name === null) {
					throw new Error('Topic name cannot be null for Metadata v4-v8. Use topics: null for all topics.')
				}
				encoder.writeString(topic.name)
			}
		}

		// Allow auto topic creation (v4+)
		if (version >= 4) {
			encoder.writeBoolean(request.allowAutoTopicCreation ?? true)
		}

		// Include cluster authorized operations (v8+)
		if (version >= 8) {
			encoder.writeBoolean(request.includeClusterAuthorizedOperations ?? false)
		}

		// Include topic authorized operations (v8+)
		if (version >= 8) {
			encoder.writeBoolean(request.includeTopicAuthorizedOperations ?? false)
		}
	}
}

/**
 * Helper to create a simple metadata request for specific topic names
 *
 * @param topicNames - List of topic names (empty for all topics)
 * @returns A MetadataRequest
 */
export function createMetadataRequest(topicNames: string[] = []): MetadataRequest {
	if (topicNames.length === 0) {
		return { topics: null }
	}

	return {
		topics: topicNames.map(name => ({ name })),
	}
}
