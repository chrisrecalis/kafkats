/**
 * ShareFetch Response (API Key 78)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface ShareFetchAcquiredRecordRange {
	firstOffset: bigint
	lastOffset: bigint
	deliveryCount: number
}

export interface ShareFetchPartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
	errorMessage: string | null
	acknowledgeErrorCode: ErrorCode
	acknowledgeErrorMessage: string | null
	currentLeader: { leaderId: number; leaderEpoch: number }
	recordsData: Buffer | null
	acquiredRecords: ShareFetchAcquiredRecordRange[]
}

export interface ShareFetchTopicResponse {
	topicId: string
	partitions: ShareFetchPartitionResponse[]
}

export interface ShareFetchNodeEndpoint {
	nodeId: number
	host: string
	port: number
	rack: string | null
}

export interface ShareFetchResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	errorMessage: string | null
	acquisitionLockTimeoutMs?: number
	topics: ShareFetchTopicResponse[]
	nodeEndpoints: ShareFetchNodeEndpoint[]
}

export function decodeShareFetchResponse(decoder: IDecoder, version: number): ShareFetchResponse {
	const flexible = isFlexibleVersion(ApiKey.ShareFetch, version)
	if (!flexible) {
		throw new Error(`ShareFetch v${version} must be flexible`)
	}

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode
	const errorMessage = decoder.readCompactNullableString()

	const acquisitionLockTimeoutMs = version >= 1 ? decoder.readInt32() : undefined

	const topics = decoder.readCompactArray(d => {
		const topicId = d.readUUID()
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const partitionErrorCode = pd.readInt16() as ErrorCode
			const partitionErrorMessage = pd.readCompactNullableString()
			const acknowledgeErrorCode = pd.readInt16() as ErrorCode
			const acknowledgeErrorMessage = pd.readCompactNullableString()

			const leaderId = pd.readInt32()
			const leaderEpoch = pd.readInt32()
			pd.skipTaggedFields()

			const recordsData = pd.readCompactNullableBytes()

			const acquiredRecords = pd.readCompactArray(ad => {
				const firstOffset = ad.readInt64()
				const lastOffset = ad.readInt64()
				const deliveryCount = ad.readInt16()
				ad.skipTaggedFields()
				return { firstOffset, lastOffset, deliveryCount }
			})

			pd.skipTaggedFields()

			return {
				partitionIndex,
				errorCode: partitionErrorCode,
				errorMessage: partitionErrorMessage,
				acknowledgeErrorCode,
				acknowledgeErrorMessage,
				currentLeader: { leaderId, leaderEpoch },
				recordsData,
				acquiredRecords,
			}
		})

		d.skipTaggedFields()
		return { topicId, partitions }
	})

	const nodeEndpoints = decoder.readCompactArray(d => {
		const nodeId = d.readInt32()
		const host = d.readCompactString()
		const port = d.readInt32()
		const rack = d.readCompactNullableString()
		d.skipTaggedFields()
		return { nodeId, host, port, rack }
	})

	decoder.skipTaggedFields()

	return { throttleTimeMs, errorCode, errorMessage, acquisitionLockTimeoutMs, topics, nodeEndpoints }
}
