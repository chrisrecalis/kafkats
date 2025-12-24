/**
 * DeleteTopics Response (API Key 20)
 *
 * Returns the result of a delete topics request including any errors.
 *
 * Supports v4-v6 (flexible encoding only):
 * - v4: First flexible version
 * - v5: Added error message field
 * - v6: Added topicId field
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Topic deletion result
 */
export interface DeleteTopicsTopicResponse {
	/** Topic name (null if deleted by topicId) */
	name: string | null
	/** Topic ID (v6+) */
	topicId: string
	/** Error code for this topic */
	errorCode: ErrorCode
	/** Error message (v5+, null if no error) */
	errorMessage: string | null
}

/**
 * DeleteTopics response data
 */
export interface DeleteTopicsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each topic */
	responses: DeleteTopicsTopicResponse[]
}

/**
 * Decode a DeleteTopics response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDeleteTopicsResponse(decoder: IDecoder, version: number): DeleteTopicsResponse {
	const flexible = isFlexibleVersion(ApiKey.DeleteTopics, version)

	if (!flexible) {
		throw new Error('Non-flexible DeleteTopics versions (< 4) not supported')
	}

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Topic responses (compact array)
	const responses = decoder.readCompactArray(d => {
		// Topic name (compact nullable string)
		const name = d.readCompactNullableString()

		// Topic ID (v6+)
		let topicId = '00000000-0000-0000-0000-000000000000'
		if (version >= 6) {
			topicId = d.readUUID()
		}

		const errorCode = d.readInt16() as ErrorCode

		// Error message (v5+)
		let errorMessage: string | null = null
		if (version >= 5) {
			errorMessage = d.readCompactNullableString()
		}

		d.skipTaggedFields()

		return {
			name,
			topicId,
			errorCode,
			errorMessage,
		}
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, responses }
}

/**
 * Check if a DeleteTopics response has any errors
 *
 * @param response - The DeleteTopics response
 * @returns true if any topic has an error
 */
export function hasDeleteTopicsErrors(response: DeleteTopicsResponse): boolean {
	return response.responses.some(topic => topic.errorCode !== ErrorCode.None)
}

/**
 * Get all errors from a DeleteTopics response
 *
 * @param response - The DeleteTopics response
 * @returns Array of { topic, errorCode, errorMessage }
 */
export function getDeleteTopicsErrors(response: DeleteTopicsResponse): Array<{
	topic: string | null
	errorCode: ErrorCode
	errorMessage: string | null
}> {
	const errors: Array<{
		topic: string | null
		errorCode: ErrorCode
		errorMessage: string | null
	}> = []

	for (const topic of response.responses) {
		if (topic.errorCode !== ErrorCode.None) {
			errors.push({
				topic: topic.name,
				errorCode: topic.errorCode,
				errorMessage: topic.errorMessage,
			})
		}
	}

	return errors
}
