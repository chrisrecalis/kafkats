/**
 * DeleteRecords Response (API Key 21)
 *
 * The low watermark (log start offset) of each partition after the deletion, or why it failed.
 *
 * Supports v2 (flexible encoding only).
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { DELETE_RECORDS_VERSIONS } from '@/protocol/messages/requests/delete-records.js'
import type { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Deletion result of one partition
 */
export interface DeleteRecordsPartitionResponse {
	/** Partition index */
	partitionIndex: number
	/** The partition's low watermark after the deletion (-1 on error) */
	lowWatermark: bigint
	/** Error code, None if the deletion succeeded */
	errorCode: ErrorCode
}

/**
 * Deletion results of one topic
 */
export interface DeleteRecordsTopicResponse {
	/** Topic name */
	name: string
	/** Results for each partition */
	partitions: DeleteRecordsPartitionResponse[]
}

/**
 * DeleteRecords response data
 */
export interface DeleteRecordsResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Results for each topic */
	topics: DeleteRecordsTopicResponse[]
}

/**
 * Decode a DeleteRecords response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeDeleteRecordsResponse(decoder: IDecoder, version: number): DeleteRecordsResponse {
	if (version < DELETE_RECORDS_VERSIONS.min || version > DELETE_RECORDS_VERSIONS.max) {
		throw new Error(`Unsupported DeleteRecords version: ${version}`)
	}

	const throttleTimeMs = decoder.readInt32()
	const topics = decoder.readCompactArray(d => {
		const name = d.readCompactString()
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const lowWatermark = pd.readInt64()
			const errorCode = pd.readInt16() as ErrorCode
			pd.skipTaggedFields()
			return { partitionIndex, lowWatermark, errorCode }
		})
		d.skipTaggedFields()
		return { name, partitions }
	})
	decoder.skipTaggedFields()

	return { throttleTimeMs, topics }
}
