/**
 * CreateTopics Response (API Key 19)
 *
 * Returns the result of a create topics request including any errors.
 *
 * Supports v5-v7 (flexible encoding only):
 * - v5: Added validateOnly support in request
 * - v6: Added topic configs in response
 * - v7: Added topicId in response
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Configuration entry in response
 */
export interface CreateTopicsResponseConfig {
	/** Configuration key name */
	name: string
	/** Configuration value */
	value: string | null
	/** Whether this config is read-only */
	readOnly: boolean
	/** Config source (v5+) */
	configSource: number
	/** Whether this is a sensitive config */
	isSensitive: boolean
}

/**
 * Topic creation result
 */
export interface CreateTopicsTopicResponse {
	/** Topic name */
	name: string
	/** Topic ID (v7+, null UUID for earlier versions) */
	topicId: string
	/** Error code for this topic */
	errorCode: ErrorCode
	/** Error message (null if no error) */
	errorMessage: string | null
	/** Number of partitions (v5+, -1 if unknown) */
	numPartitions: number
	/** Replication factor (v5+, -1 if unknown) */
	replicationFactor: number
	/** Topic configuration (v5+) */
	configs: CreateTopicsResponseConfig[]
}

/**
 * CreateTopics response data
 */
export interface CreateTopicsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each topic */
	topics: CreateTopicsTopicResponse[]
}

/**
 * Decode a CreateTopics response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeCreateTopicsResponse(decoder: IDecoder, version: number): CreateTopicsResponse {
	const flexible = isFlexibleVersion(ApiKey.CreateTopics, version)

	if (!flexible) {
		throw new Error('Non-flexible CreateTopics versions (< 5) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Topic responses (compact array)
	const topics = decoder.readCompactArray(d => {
		const name = d.readCompactString()

		// Topic ID (v7+)
		let topicId = '00000000-0000-0000-0000-000000000000'
		if (version >= 7) {
			topicId = d.readUUID()
		}

		const errorCode = d.readInt16() as ErrorCode
		const errorMessage = d.readCompactNullableString()

		// Topic configuration info (v5+)
		let numPartitions = -1
		let replicationFactor = -1
		let configs: CreateTopicsResponseConfig[] = []

		if (version >= 5) {
			numPartitions = d.readInt32()
			replicationFactor = d.readInt16()

			// Configs (v5+)
			configs = d.readCompactArray(cd => {
				const configName = cd.readCompactString()
				const configValue = cd.readCompactNullableString()
				const readOnly = cd.readBoolean()
				const configSource = cd.readInt8()
				const isSensitive = cd.readBoolean()
				cd.skipTaggedFields()
				return {
					name: configName,
					value: configValue,
					readOnly,
					configSource,
					isSensitive,
				}
			})
		}

		d.skipTaggedFields()

		return {
			name,
			topicId,
			errorCode,
			errorMessage,
			numPartitions,
			replicationFactor,
			configs,
		}
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, topics }
}

/**
 * Check if a CreateTopics response has any errors
 *
 * @param response - The CreateTopics response
 * @returns true if any topic has an error (excluding TopicAlreadyExists)
 */
export function hasCreateTopicsErrors(response: CreateTopicsResponse): boolean {
	return response.topics.some(
		topic => topic.errorCode !== ErrorCode.None && topic.errorCode !== ErrorCode.TopicAlreadyExists
	)
}

/**
 * Get all errors from a CreateTopics response
 *
 * @param response - The CreateTopics response
 * @returns Array of { topic, errorCode, errorMessage }
 */
export function getCreateTopicsErrors(response: CreateTopicsResponse): Array<{
	topic: string
	errorCode: ErrorCode
	errorMessage: string | null
}> {
	const errors: Array<{
		topic: string
		errorCode: ErrorCode
		errorMessage: string | null
	}> = []

	for (const topic of response.topics) {
		if (topic.errorCode !== ErrorCode.None && topic.errorCode !== ErrorCode.TopicAlreadyExists) {
			errors.push({
				topic: topic.name,
				errorCode: topic.errorCode,
				errorMessage: topic.errorMessage,
			})
		}
	}

	return errors
}
