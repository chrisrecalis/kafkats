/**
 * ShareAcknowledge Request (API Key 79)
 *
 * Used by share consumers to acknowledge (accept/release/reject) acquired records.
 *
 * Flexible (tagged fields) in all supported versions.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export const SHARE_ACKNOWLEDGE_VERSIONS = {
	min: 1,
	max: 1,
}

export interface ShareAcknowledgeAcknowledgementBatch {
	firstOffset: bigint
	lastOffset: bigint
	acknowledgeTypes: number[]
}

export interface ShareAcknowledgeRequestPartition {
	partitionIndex: number
	acknowledgementBatches: ShareAcknowledgeAcknowledgementBatch[]
}

export interface ShareAcknowledgeRequestTopic {
	topicId: string
	partitions: ShareAcknowledgeRequestPartition[]
}

export interface ShareAcknowledgeRequest {
	groupId: string
	memberId: string
	shareSessionEpoch: number
	topics: ShareAcknowledgeRequestTopic[]
}

export function encodeShareAcknowledgeRequest(
	encoder: IEncoder,
	version: number,
	request: ShareAcknowledgeRequest
): void {
	if (version < SHARE_ACKNOWLEDGE_VERSIONS.min || version > SHARE_ACKNOWLEDGE_VERSIONS.max) {
		throw new Error(`Unsupported ShareAcknowledge version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareAcknowledge, version)
	if (!flexible) {
		throw new Error(`ShareAcknowledge v${version} must be flexible`)
	}

	encoder.writeCompactNullableString(request.groupId)
	encoder.writeCompactNullableString(request.memberId)
	encoder.writeInt32(request.shareSessionEpoch)

	encoder.writeCompactArray(request.topics, (topic, te) => {
		te.writeUUID(topic.topicId)
		te.writeCompactArray(topic.partitions, (partition, pe) => {
			pe.writeInt32(partition.partitionIndex)
			pe.writeCompactArray(partition.acknowledgementBatches, (batch, be) => {
				be.writeInt64(batch.firstOffset)
				be.writeInt64(batch.lastOffset)
				be.writeCompactArray(batch.acknowledgeTypes, (t, tEnc) => {
					tEnc.writeInt8(t)
				})
				be.writeEmptyTaggedFields()
			})
			pe.writeEmptyTaggedFields()
		})
		te.writeEmptyTaggedFields()
	})

	encoder.writeEmptyTaggedFields()
}
