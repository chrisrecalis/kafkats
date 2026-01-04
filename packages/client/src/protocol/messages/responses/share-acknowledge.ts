/**
 * ShareAcknowledge Response (API Key 79)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { SHARE_ACKNOWLEDGE_VERSIONS } from '@/protocol/messages/requests/share-acknowledge.js'

export interface ShareAcknowledgePartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
	errorMessage: string | null
	currentLeader: { leaderId: number; leaderEpoch: number }
}

export interface ShareAcknowledgeTopicResponse {
	topicId: string
	partitions: ShareAcknowledgePartitionResponse[]
}

export interface ShareAcknowledgeNodeEndpoint {
	nodeId: number
	host: string
	port: number
	rack: string | null
}

export interface ShareAcknowledgeResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	errorMessage: string | null
	topics: ShareAcknowledgeTopicResponse[]
	nodeEndpoints: ShareAcknowledgeNodeEndpoint[]
}

export function decodeShareAcknowledgeResponse(decoder: IDecoder, version: number): ShareAcknowledgeResponse {
	if (version < SHARE_ACKNOWLEDGE_VERSIONS.min || version > SHARE_ACKNOWLEDGE_VERSIONS.max) {
		throw new Error(`Unsupported ShareAcknowledge version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareAcknowledge, version)
	if (!flexible) {
		throw new Error(`ShareAcknowledge v${version} must be flexible`)
	}

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode
	const errorMessage = decoder.readCompactNullableString()

	const topics = decoder.readCompactArray(d => {
		const topicId = d.readUUID()
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const partitionErrorCode = pd.readInt16() as ErrorCode
			const partitionErrorMessage = pd.readCompactNullableString()

			const leaderId = pd.readInt32()
			const leaderEpoch = pd.readInt32()
			pd.skipTaggedFields() // CurrentLeader (LeaderIdAndEpoch) tagged fields

			// PartitionData is a separate flexible struct; its tagged fields follow immediately.
			pd.skipTaggedFields() // PartitionData tagged fields

			return {
				partitionIndex,
				errorCode: partitionErrorCode,
				errorMessage: partitionErrorMessage,
				currentLeader: { leaderId, leaderEpoch },
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

	return { throttleTimeMs, errorCode, errorMessage, topics, nodeEndpoints }
}
