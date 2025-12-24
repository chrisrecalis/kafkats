/**
 * OffsetFetch Response (API Key 9)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
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

export interface OffsetFetchGroupResponse {
	groupId: string
	topics: OffsetFetchTopicResponse[]
	errorCode: ErrorCode
}

export interface OffsetFetchResponse {
	throttleTimeMs: number
	topics: OffsetFetchTopicResponse[]
	errorCode: ErrorCode
	/** Multiple groups (v8+) */
	groups?: OffsetFetchGroupResponse[]
}

export function decodeOffsetFetchResponse(decoder: IDecoder, version: number): OffsetFetchResponse {
	const flexible = isFlexibleVersion(ApiKey.OffsetFetch, version)

	const throttleTimeMs = decoder.readInt32()

	if (version >= 8) {
		// Batch response (v8+)
		let groups: OffsetFetchGroupResponse[]

		if (flexible) {
			groups = decoder.readCompactArray(d => {
				const groupId = d.readCompactString()
				const topics = d.readCompactArray(td => {
					const name = td.readCompactString()
					const partitions = td.readCompactArray(pd => {
						const partitionIndex = pd.readInt32()
						const committedOffset = pd.readInt64()
						const committedLeaderEpoch = pd.readInt32()
						const metadata = pd.readCompactNullableString()
						const errorCode = pd.readInt16() as ErrorCode
						pd.skipTaggedFields()
						return { partitionIndex, committedOffset, committedLeaderEpoch, metadata, errorCode }
					})
					td.skipTaggedFields()
					return { name, partitions }
				})
				const errorCode = d.readInt16() as ErrorCode
				d.skipTaggedFields()
				return { groupId, topics, errorCode }
			})
			decoder.skipTaggedFields()
		} else {
			groups = decoder.readArray(d => {
				const groupId = d.readString()
				const topics = d.readArray(td => {
					const name = td.readString()
					const partitions = td.readArray(pd => {
						const partitionIndex = pd.readInt32()
						const committedOffset = pd.readInt64()
						const committedLeaderEpoch = pd.readInt32()
						const metadata = pd.readNullableString()
						const errorCode = pd.readInt16() as ErrorCode
						return { partitionIndex, committedOffset, committedLeaderEpoch, metadata, errorCode }
					})
					return { name, partitions }
				})
				const errorCode = d.readInt16() as ErrorCode
				return { groupId, topics, errorCode }
			})
		}

		const firstGroup = groups[0]
		return {
			throttleTimeMs,
			topics: firstGroup?.topics ?? [],
			errorCode: firstGroup?.errorCode ?? ErrorCode.None,
			groups,
		}
	} else {
		// Single group response (v6-v7)
		let topics: OffsetFetchTopicResponse[]
		let errorCode: ErrorCode

		if (flexible) {
			topics = decoder.readCompactArray(d => {
				const name = d.readCompactString()
				const partitions = d.readCompactArray(pd => {
					const partitionIndex = pd.readInt32()
					const committedOffset = pd.readInt64()
					const committedLeaderEpoch = pd.readInt32()
					const metadata = pd.readCompactNullableString()
					const partitionErrorCode = pd.readInt16() as ErrorCode
					pd.skipTaggedFields()
					return {
						partitionIndex,
						committedOffset,
						committedLeaderEpoch,
						metadata,
						errorCode: partitionErrorCode,
					}
				})
				d.skipTaggedFields()
				return { name, partitions }
			})
			errorCode = decoder.readInt16() as ErrorCode
			decoder.skipTaggedFields()
		} else {
			topics = decoder.readArray(d => {
				const name = d.readString()
				const partitions = d.readArray(pd => {
					const partitionIndex = pd.readInt32()
					const committedOffset = pd.readInt64()
					const committedLeaderEpoch = pd.readInt32()
					const metadata = pd.readNullableString()
					const partitionErrorCode = pd.readInt16() as ErrorCode
					return {
						partitionIndex,
						committedOffset,
						committedLeaderEpoch,
						metadata,
						errorCode: partitionErrorCode,
					}
				})
				return { name, partitions }
			})
			errorCode = decoder.readInt16() as ErrorCode
		}

		return { throttleTimeMs, topics, errorCode }
	}
}
