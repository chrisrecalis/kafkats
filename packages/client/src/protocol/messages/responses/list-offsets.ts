/**
 * ListOffsets Response (API Key 2)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface ListOffsetsPartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
	/** Deprecated old-style offsets (v0) */
	oldStyleOffsets?: bigint[]
	/** Timestamp of the offset (v1+) */
	timestamp: bigint
	/** The offset */
	offset: bigint
	/** Leader epoch (v4+) */
	leaderEpoch: number
}

export interface ListOffsetsTopicResponse {
	name: string
	partitions: ListOffsetsPartitionResponse[]
}

export interface ListOffsetsResponse {
	throttleTimeMs: number
	topics: ListOffsetsTopicResponse[]
}

export function decodeListOffsetsResponse(decoder: IDecoder, version: number): ListOffsetsResponse {
	const flexible = isFlexibleVersion(ApiKey.ListOffsets, version)

	const throttleTimeMs = decoder.readInt32()

	let topics: ListOffsetsTopicResponse[]
	if (flexible) {
		topics = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const partitions = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				const timestamp = pd.readInt64()
				const offset = pd.readInt64()
				const leaderEpoch = pd.readInt32()
				pd.skipTaggedFields()
				return { partitionIndex, errorCode, timestamp, offset, leaderEpoch }
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
				const timestamp = pd.readInt64()
				const offset = pd.readInt64()
				const leaderEpoch = pd.readInt32()
				return { partitionIndex, errorCode, timestamp, offset, leaderEpoch }
			})
			return { name, partitions }
		})
	}

	return { throttleTimeMs, topics }
}
