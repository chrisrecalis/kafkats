/**
 * ShareFetch Request (API Key 78)
 *
 * Used by share consumers to fetch records with queue-like semantics and
 * per-record acknowledgements.
 *
 * Flexible (tagged fields) in all supported versions.
 *
 * v2: Production-ready Share Groups in Kafka 4.2, including KIP-1206 ShareAcquireMode
 * and KIP-1222 Renew acknowledgements.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'

export const SHARE_FETCH_VERSIONS = {
	min: 2,
	max: 2,
}

/**
 * ShareAcquireMode controls how the broker honors `maxRecords` (KIP-1206).
 * - `batch_optimized` (0): may return more than `maxRecords` to align batch boundaries
 *   for throughput.
 * - `record_limit` (1): strictly caps the response at `maxRecords`.
 */
export const SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED = 0
export const SHARE_ACQUIRE_MODE_RECORD_LIMIT = 1
export type ShareAcquireMode = typeof SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED | typeof SHARE_ACQUIRE_MODE_RECORD_LIMIT

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
	/** Acquire mode (v2+). Defaults to `batch_optimized` (0). */
	acquireMode?: ShareAcquireMode
	/** True iff any AcknowledgementBatch contains a Renew (4) entry (v2+). */
	isRenewAck?: boolean
	topics: Array<ShareFetchRequestTopic<ShareFetchRequestPartition>>
	forgottenTopicsData?: ShareFetchRequestForgottenTopic[]
}

export function encodeShareFetchRequest(encoder: IEncoder, version: number, request: ShareFetchRequest): void {
	if (version < SHARE_FETCH_VERSIONS.min || version > SHARE_FETCH_VERSIONS.max) {
		throw new Error(`Unsupported ShareFetch version: ${version}`)
	}

	encoder.writeCompactNullableString(request.groupId)
	encoder.writeCompactNullableString(request.memberId)
	encoder.writeInt32(request.shareSessionEpoch)
	encoder.writeInt32(request.maxWaitMs)
	encoder.writeInt32(request.minBytes)
	encoder.writeInt32(request.maxBytes)

	encoder.writeInt32(request.maxRecords)
	encoder.writeInt32(request.batchSize)

	encoder.writeInt8(request.acquireMode ?? SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED)
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

	encoder.writeCompactArray(request.forgottenTopicsData ?? [], (topic, te) => {
		te.writeUUID(topic.topicId)
		te.writeCompactArray(topic.partitions, (p, pe) => {
			pe.writeInt32(p)
		})
		te.writeEmptyTaggedFields()
	})

	encoder.writeEmptyTaggedFields()
}
