/**
 * ShareAcknowledge Request (API Key 79)
 *
 * Used by share consumers to acknowledge (accept/release/reject/renew) acquired records.
 *
 * Flexible (tagged fields) in all supported versions.
 *
 * v2: Production-ready Share Groups in Kafka 4.2, including KIP-1222 Renew acknowledgements.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'

export const SHARE_ACKNOWLEDGE_VERSIONS = {
	min: 2,
	max: 2,
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
	/** True iff any AcknowledgementBatch contains a Renew (4) entry (v2+). */
	isRenewAck?: boolean
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

	encoder.writeCompactNullableString(request.groupId)
	encoder.writeCompactNullableString(request.memberId)
	encoder.writeInt32(request.shareSessionEpoch)

	encoder.writeBoolean(request.isRenewAck ?? false)

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
