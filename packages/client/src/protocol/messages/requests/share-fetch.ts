/**
 * ShareFetch Request (API Key 78)
 *
 * Used by share consumers to fetch records with queue-like semantics and
 * per-record acknowledgements.
 *
 * Flexible (tagged fields) in all supported versions.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export const SHARE_FETCH_VERSIONS = {
	min: 1,
	max: 1,
}

export interface ShareFetchAcknowledgementBatch {
	firstOffset: bigint
	lastOffset: bigint
	acknowledgeTypes: number[]
}

export interface ShareFetchRequestPartition {
	partitionIndex: number
	acknowledgementBatches: ShareFetchAcknowledgementBatch[]
}

export interface ShareFetchRequestTopic<P> {
	topicId: string
	partitions: P[]
}

export interface ShareFetchRequestForgottenTopic {
	topicId: string
	partitions: number[]
}

export interface ShareFetchRequest {
	groupId: string
	memberId: string
	shareSessionEpoch: number
	maxWaitMs: number
	minBytes: number
	maxBytes: number
	maxRecords: number
	batchSize: number
	topics: Array<ShareFetchRequestTopic<ShareFetchRequestPartition>>
	forgottenTopicsData?: ShareFetchRequestForgottenTopic[]
}

export function encodeShareFetchRequest(encoder: IEncoder, version: number, request: ShareFetchRequest): void {
	if (version < SHARE_FETCH_VERSIONS.min || version > SHARE_FETCH_VERSIONS.max) {
		throw new Error(`Unsupported ShareFetch version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareFetch, version)
	if (!flexible) {
		throw new Error(`ShareFetch v${version} must be flexible`)
	}

	if (typeof request.maxRecords !== 'number' || typeof request.batchSize !== 'number') {
		throw new Error('ShareFetch v1 requires maxRecords and batchSize')
	}

	encoder.writeCompactNullableString(request.groupId)
	encoder.writeCompactNullableString(request.memberId)
	encoder.writeInt32(request.shareSessionEpoch)
	encoder.writeInt32(request.maxWaitMs)
	encoder.writeInt32(request.minBytes)
	encoder.writeInt32(request.maxBytes)

	encoder.writeInt32(request.maxRecords)
	encoder.writeInt32(request.batchSize)

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

	encoder.writeCompactArray(request.forgottenTopicsData ?? [], (topic, te) => {
		te.writeUUID(topic.topicId)
		te.writeCompactArray(topic.partitions, (p, pe) => {
			pe.writeInt32(p)
		})
		te.writeEmptyTaggedFields()
	})

	encoder.writeEmptyTaggedFields()
}
