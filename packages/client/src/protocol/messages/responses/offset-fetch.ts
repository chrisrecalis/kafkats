/**
 * OffsetFetch Response (API Key 9)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface OffsetFetchPartitionResponse {
	partitionIndex: number
	committedOffset: bigint
	committedLeaderEpoch: number
	metadata: string | null
	errorCode: ErrorCode
}

export interface OffsetFetchTopicResponse {
	name: string
	partitions: OffsetFetchPartitionResponse[]
}

export interface OffsetFetchResponse {
	throttleTimeMs: number
	topics: OffsetFetchTopicResponse[]
	errorCode: ErrorCode
}

export function decodeOffsetFetchResponse(decoder: IDecoder, version: number): OffsetFetchResponse {
	if (version !== 7) {
		throw new Error(`Unsupported OffsetFetch version: ${version}`)
	}

	const throttleTimeMs = decoder.readInt32()
	const topics = decoder.readCompactArray(topicDecoder => {
		const name = topicDecoder.readCompactString()
		const partitions = topicDecoder.readCompactArray(partitionDecoder => {
			const partitionIndex = partitionDecoder.readInt32()
			const committedOffset = partitionDecoder.readInt64()
			const committedLeaderEpoch = partitionDecoder.readInt32()
			const metadata = partitionDecoder.readCompactNullableString()
			const errorCode = partitionDecoder.readInt16() as ErrorCode
			partitionDecoder.skipTaggedFields()
			return { partitionIndex, committedOffset, committedLeaderEpoch, metadata, errorCode }
		})
		topicDecoder.skipTaggedFields()
		return { name, partitions }
	})
	const errorCode = decoder.readInt16() as ErrorCode
	decoder.skipTaggedFields()

	return { throttleTimeMs, topics, errorCode }
}
