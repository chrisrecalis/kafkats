/**
 * OffsetCommit Response (API Key 8)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface OffsetCommitPartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
}

export interface OffsetCommitTopicResponse {
	name: string
	partitions: OffsetCommitPartitionResponse[]
}

export interface OffsetCommitResponse {
	throttleTimeMs: number
	topics: OffsetCommitTopicResponse[]
}

export function decodeOffsetCommitResponse(decoder: IDecoder, version: number): OffsetCommitResponse {
	const flexible = isFlexibleVersion(ApiKey.OffsetCommit, version)

	const throttleTimeMs = decoder.readInt32()

	let topics: OffsetCommitTopicResponse[]
	if (flexible) {
		topics = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const partitions = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				pd.skipTaggedFields()
				return { partitionIndex, errorCode }
			})
			d.skipTaggedFields()
			return { name, partitions }
		})
		decoder.skipTaggedFields()
	} else {
		topics = decoder.readArray(d => {
			const name = d.readString()
			const partitions = d.readArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				return { partitionIndex, errorCode }
			})
			return { name, partitions }
		})
	}

	return { throttleTimeMs, topics }
}
