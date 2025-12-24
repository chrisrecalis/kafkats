/**
 * DeleteTopics Request (API Key 20)
 *
 * Used to delete topics from the Kafka cluster.
 *
 * Supports v4-v6 (flexible encoding only):
 * - v4: First flexible version
 * - v5: Added error message in response
 * - v6: Added topicId field for deletion by UUID
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Topic to delete
 */
export interface DeleteTopicsRequestTopic {
	/** Topic name (null if using topicId) */
	name: string | null
	/** Topic ID for deletion (v6+, null if using name) */
	topicId?: string
}

/**
 * DeleteTopics request data
 */
export interface DeleteTopicsRequest {
	/** Topics to delete */
	topics: DeleteTopicsRequestTopic[]
	/** Timeout in milliseconds */
	timeoutMs: number
}

/**
 * Supported API versions for DeleteTopics request
 *
 * - v4-v6: flexible encoding with compact strings and tagged fields
 */
export const DELETE_TOPICS_VERSIONS = {
	min: 4,
	max: 6,
}

/**
 * Encode a DeleteTopics request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDeleteTopicsRequest(encoder: IEncoder, version: number, request: DeleteTopicsRequest): void {
	if (version < DELETE_TOPICS_VERSIONS.min || version > DELETE_TOPICS_VERSIONS.max) {
		throw new Error(`Unsupported DeleteTopics version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.DeleteTopics, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteTopics versions (< 4) not supported')
	}

	// Topics array (compact array)
	encoder.writeCompactArray(request.topics, (topic, enc) => {
		// Topic name (compact nullable string)
		enc.writeCompactNullableString(topic.name)

		// Topic ID (v6+)
		if (version >= 6) {
			enc.writeUUID(topic.topicId ?? '00000000-0000-0000-0000-000000000000')
		}

		enc.writeEmptyTaggedFields()
	})

	// Timeout in milliseconds
	encoder.writeInt32(request.timeoutMs)

	// Request-level tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Default DeleteTopics request configuration
 */
export const DELETE_TOPICS_DEFAULTS = {
	timeoutMs: 30000,
} as const

/**
 * Helper to create a simple DeleteTopics request
 *
 * @param topics - Topic names to delete
 * @param options - Optional configuration
 * @returns A DeleteTopicsRequest
 */
export function createDeleteTopicsRequest(topics: string[], options?: { timeoutMs?: number }): DeleteTopicsRequest {
	return {
		topics: topics.map(name => ({ name })),
		timeoutMs: options?.timeoutMs ?? DELETE_TOPICS_DEFAULTS.timeoutMs,
	}
}
