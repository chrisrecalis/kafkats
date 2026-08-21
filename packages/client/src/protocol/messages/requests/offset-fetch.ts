/**
 * OffsetFetch Request (API Key 9)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'

export interface OffsetFetchPartition {
	partitionIndex: number
}

export interface OffsetFetchTopic {
	name: string
	partitions: OffsetFetchPartition[]
}

export interface OffsetFetchRequest {
	groupId: string
	topics: OffsetFetchTopic[] | null
	requireStable: boolean
}

/**
 * Supported API versions for OffsetFetch request
 *
 * v7 is the minimum safe version for consumer offset initialization because it
 * carries KIP-447's requireStable flag. Older versions can silently expose the
 * previous committed position while a transactional offset commit is pending.
 */
export const OFFSET_FETCH_VERSIONS = { min: 7, max: 7 }

export function encodeOffsetFetchRequest(encoder: IEncoder, version: number, request: OffsetFetchRequest): void {
	if (version !== 7) {
		throw new Error(`Unsupported OffsetFetch version: ${version}`)
	}

	encoder.writeCompactString(request.groupId)
	encoder.writeCompactNullableArray(request.topics, (topic, topicEncoder) => {
		topicEncoder.writeCompactString(topic.name)
		topicEncoder.writeCompactArray(topic.partitions, (partition, partitionEncoder) => {
			partitionEncoder.writeInt32(partition.partitionIndex)
		})
		topicEncoder.writeEmptyTaggedFields()
	})
	encoder.writeBoolean(request.requireStable)
	encoder.writeEmptyTaggedFields()
}
