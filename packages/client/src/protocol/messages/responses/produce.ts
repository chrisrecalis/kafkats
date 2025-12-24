/**
 * Produce Response (API Key 0)
 *
 * Returns the result of a produce request including offsets and errors.
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Record error information
 */
export interface RecordError {
	batchIndex: number
	batchIndexErrorMessage: string | null
}

/**
 * Partition response data
 */
export interface ProducePartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
	baseOffset: bigint
	logAppendTimeMs: bigint
	logStartOffset: bigint
	recordErrors: RecordError[]
	errorMessage: string | null
}

/**
 * Topic response data
 */
export interface ProduceTopicResponse {
	name: string
	partitions: ProducePartitionResponse[]
}

/**
 * Produce response data
 */
export interface ProduceResponse {
	/** Topic responses */
	topics: ProduceTopicResponse[]
	/** Throttle time in milliseconds */
	throttleTimeMs: number
}

/**
 * Decode a Produce response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeProduceResponse(decoder: IDecoder, version: number): ProduceResponse {
	const flexible = isFlexibleVersion(ApiKey.Produce, version)

	if (!flexible) {
		throw new Error('Non-flexible Produce versions (< 9) not supported')
	}

	// Topic responses (compact array)
	const topics = decoder.readCompactArray(d => {
		const name = d.readCompactString()

		// Partition responses (compact array)
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const errorCode = pd.readInt16() as ErrorCode
			const baseOffset = pd.readInt64()
			const logAppendTimeMs = pd.readInt64()
			const logStartOffset = pd.readInt64()

			// Record errors (v8+)
			let recordErrors: RecordError[] = []
			if (version >= 8) {
				recordErrors = pd.readCompactArray(rd => {
					const batchIndex = rd.readInt32()
					const batchIndexErrorMessage = rd.readCompactNullableString()
					rd.skipTaggedFields()
					return { batchIndex, batchIndexErrorMessage }
				})
			}

			// Error message (v8+)
			let errorMessage: string | null = null
			if (version >= 8) {
				errorMessage = pd.readCompactNullableString()
			}

			pd.skipTaggedFields()

			return {
				partitionIndex,
				errorCode,
				baseOffset,
				logAppendTimeMs,
				logStartOffset,
				recordErrors,
				errorMessage,
			}
		})

		d.skipTaggedFields()

		return { name, partitions }
	})

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Tagged fields
	decoder.skipTaggedFields()

	return { topics, throttleTimeMs }
}

/**
 * Check if a produce response has any errors
 *
 * @param response - The produce response
 * @returns true if any partition has an error
 */
export function hasProduceErrors(response: ProduceResponse): boolean {
	return response.topics.some(topic => topic.partitions.some(partition => partition.errorCode !== ErrorCode.None))
}

/**
 * Get all errors from a produce response
 *
 * @param response - The produce response
 * @returns Array of { topic, partition, errorCode, errorMessage }
 */
export function getProduceErrors(response: ProduceResponse): Array<{
	topic: string
	partition: number
	errorCode: ErrorCode
	errorMessage: string | null
}> {
	const errors: Array<{
		topic: string
		partition: number
		errorCode: ErrorCode
		errorMessage: string | null
	}> = []

	for (const topic of response.topics) {
		for (const partition of topic.partitions) {
			if (partition.errorCode !== ErrorCode.None) {
				errors.push({
					topic: topic.name,
					partition: partition.partitionIndex,
					errorCode: partition.errorCode,
					errorMessage: partition.errorMessage,
				})
			}
		}
	}

	return errors
}
