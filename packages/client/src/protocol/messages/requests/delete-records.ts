/**
 * DeleteRecords Request (API Key 21)
 *
 * Moves the log start offset of partitions forward, deleting every record before it. Sent to each partition's leader.
 *
 * Supports v2 (flexible encoding only, Kafka 2.4+, like the Produce v9 the client requires).
 */

import type { IEncoder } from '@/protocol/primitives/index.js'

/**
 * Partition whose records before `offset` are deleted
 */
export interface DeleteRecordsRequestPartition {
	/** Partition index */
	partitionIndex: number
	/** The new log start offset; -1n means the high watermark */
	offset: bigint
}

/**
 * Topic whose partitions' records are deleted
 */
export interface DeleteRecordsRequestTopic {
	/** Topic name */
	name: string
	/** Partitions to delete records from */
	partitions: DeleteRecordsRequestPartition[]
}

/**
 * DeleteRecords request data
 */
export interface DeleteRecordsRequest {
	/** Topics to delete records from */
	topics: DeleteRecordsRequestTopic[]
	/** How long the broker waits for the deletion to reach the replicas, in milliseconds */
	timeoutMs: number
}

/**
 * Supported API versions for DeleteRecords request
 *
 * - v2: flexible encoding with compact strings and tagged fields
 */
export const DELETE_RECORDS_VERSIONS = {
	min: 2,
	max: 2,
}

/**
 * Encode a DeleteRecords request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeDeleteRecordsRequest(encoder: IEncoder, version: number, request: DeleteRecordsRequest): void {
	if (version < DELETE_RECORDS_VERSIONS.min || version > DELETE_RECORDS_VERSIONS.max) {
		throw new Error(`Unsupported DeleteRecords version: ${version}`)
	}

	encoder.writeCompactArray(request.topics, (topic, enc) => {
		enc.writeCompactString(topic.name)
		enc.writeCompactArray(topic.partitions, (partition, penc) => {
			penc.writeInt32(partition.partitionIndex)
			penc.writeInt64(partition.offset)
			penc.writeEmptyTaggedFields()
		})
		enc.writeEmptyTaggedFields()
	})
	encoder.writeInt32(request.timeoutMs)
	encoder.writeEmptyTaggedFields()
}
