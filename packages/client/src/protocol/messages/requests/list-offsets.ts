/**
 * ListOffsets Request (API Key 2)
 *
 * Used to get offset information for partitions.
 *
 * Version 6+ uses flexible encoding.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Special timestamp values for offset queries
 */
export const OFFSET_TIMESTAMP = {
	/** Get the latest offset */
	LATEST: -1n,
	/** Get the earliest offset */
	EARLIEST: -2n,
	/** Get the offset at max timestamp (v7+) */
	MAX_TIMESTAMP: -3n,
} as const

/**
 * Partition to query
 */
export interface ListOffsetsRequestPartition {
	partitionIndex: number
	/** Current leader epoch (-1 if not known) */
	currentLeaderEpoch?: number
	/** Timestamp to query (use OFFSET_TIMESTAMP constants) */
	timestamp: bigint
}

/**
 * Topic to query
 */
export interface ListOffsetsRequestTopic {
	name: string
	partitions: ListOffsetsRequestPartition[]
}

/**
 * ListOffsets request data
 */
export interface ListOffsetsRequest {
	/** Replica ID (-1 for consumers) */
	replicaId?: number
	/** Isolation level (v2+) */
	isolationLevel?: number
	/** Topics to query */
	topics: ListOffsetsRequestTopic[]
}

/**
 * Supported API versions for ListOffsets request
 *
 * - v0-v5: non-flexible encoding
 * - v6+: flexible encoding with compact strings and tagged fields
 */
export const LIST_OFFSETS_VERSIONS = { min: 5, max: 5 }

export function encodeListOffsetsRequest(encoder: IEncoder, version: number, request: ListOffsetsRequest): void {
	if (version < LIST_OFFSETS_VERSIONS.min || version > LIST_OFFSETS_VERSIONS.max) {
		throw new Error(`Unsupported ListOffsets version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ListOffsets, version)

	encoder.writeInt32(request.replicaId ?? -1)
	encoder.writeInt8(request.isolationLevel ?? 0)

	if (flexible) {
		encoder.writeCompactArray(request.topics, (topic, enc) => {
			enc.writeCompactString(topic.name)
			enc.writeCompactArray(topic.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt32(p.currentLeaderEpoch ?? -1)
				penc.writeInt64(p.timestamp)
				penc.writeEmptyTaggedFields()
			})
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeArray(request.topics, (topic, enc) => {
			enc.writeString(topic.name)
			enc.writeArray(topic.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt32(p.currentLeaderEpoch ?? -1)
				penc.writeInt64(p.timestamp)
			})
		})
	}
}
