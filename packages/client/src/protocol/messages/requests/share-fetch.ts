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

export interface ShareFetchRequestPartitionV0 {
	partitionIndex: number
	partitionMaxBytes: number
	acknowledgementBatches: ShareFetchAcknowledgementBatch[]
}

export interface ShareFetchRequestPartitionV1 {
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

export interface ShareFetchRequestV0 {
	groupId: string
	memberId: string
	shareSessionEpoch: number
	maxWaitMs: number
	minBytes: number
	maxBytes: number
	topics: Array<ShareFetchRequestTopic<ShareFetchRequestPartitionV0>>
	forgottenTopicsData?: ShareFetchRequestForgottenTopic[]
}

export interface ShareFetchRequestV1 {
	groupId: string
	memberId: string
	shareSessionEpoch: number
	maxWaitMs: number
	minBytes: number
	maxBytes: number
	maxRecords: number
	batchSize: number
	topics: Array<ShareFetchRequestTopic<ShareFetchRequestPartitionV1>>
	forgottenTopicsData?: ShareFetchRequestForgottenTopic[]
}

export type ShareFetchRequest = ShareFetchRequestV0 | ShareFetchRequestV1

export function encodeShareFetchRequest(encoder: IEncoder, version: number, request: ShareFetchRequest): void {
	if (version < SHARE_FETCH_VERSIONS.min || version > SHARE_FETCH_VERSIONS.max) {
		throw new Error(`Unsupported ShareFetch version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareFetch, version)
	if (!flexible) {
		throw new Error(`ShareFetch v${version} must be flexible`)
	}

	// Runtime guard: helps JS callers / `any` avoid sending v0-shaped requests to v1 encoding.
	if (version >= 1) {
		const r1 = request as ShareFetchRequestV1
		if (typeof r1.maxRecords !== 'number' || typeof r1.batchSize !== 'number') {
			throw new Error('ShareFetch v1 requires maxRecords and batchSize')
		}
	}

	encoder.writeCompactNullableString(request.groupId)
	encoder.writeCompactNullableString(request.memberId)
	encoder.writeInt32(request.shareSessionEpoch)
	encoder.writeInt32(request.maxWaitMs)
	encoder.writeInt32(request.minBytes)
	encoder.writeInt32(request.maxBytes)

	if (version >= 1) {
		const r1 = request as ShareFetchRequestV1
		encoder.writeInt32(r1.maxRecords)
		encoder.writeInt32(r1.batchSize)
	}

	encoder.writeCompactArray(request.topics, (topic, te) => {
		te.writeUUID(topic.topicId)
		te.writeCompactArray(topic.partitions, (partition, pe) => {
			pe.writeInt32(partition.partitionIndex)
			if (version >= 1 && (partition as ShareFetchRequestPartitionV0).partitionMaxBytes !== undefined) {
				throw new Error('ShareFetch v1 does not support partitionMaxBytes')
			}
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
