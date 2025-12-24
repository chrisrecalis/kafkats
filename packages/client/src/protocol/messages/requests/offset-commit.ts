/**
 * OffsetCommit Request (API Key 8)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface OffsetCommitPartition {
	partitionIndex: number
	committedOffset: bigint
	committedLeaderEpoch?: number
	committedMetadata?: string | null
}

export interface OffsetCommitTopic {
	name: string
	partitions: OffsetCommitPartition[]
}

export interface OffsetCommitRequest {
	groupId: string
	generationId: number
	memberId: string
	groupInstanceId?: string | null
	retentionTimeMs?: bigint
	topics: OffsetCommitTopic[]
}

/**
 * Supported API versions for OffsetCommit request
 *
 * - v0-v7: non-flexible encoding
 * - v8+: flexible encoding with compact strings and tagged fields
 */
export const OFFSET_COMMIT_VERSIONS = { min: 7, max: 7 }

export function encodeOffsetCommitRequest(encoder: IEncoder, version: number, request: OffsetCommitRequest): void {
	if (version < OFFSET_COMMIT_VERSIONS.min || version > OFFSET_COMMIT_VERSIONS.max) {
		throw new Error(`Unsupported OffsetCommit version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.OffsetCommit, version)

	if (flexible) {
		encoder.writeCompactString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeCompactString(request.memberId)
		encoder.writeCompactNullableString(request.groupInstanceId ?? null)

		encoder.writeCompactArray(request.topics, (t, enc) => {
			enc.writeCompactString(t.name)
			enc.writeCompactArray(t.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt64(p.committedOffset)
				penc.writeInt32(p.committedLeaderEpoch ?? -1)
				penc.writeCompactNullableString(p.committedMetadata ?? null)
				penc.writeEmptyTaggedFields()
			})
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeString(request.memberId)
		encoder.writeNullableString(request.groupInstanceId ?? null)

		encoder.writeArray(request.topics, (t, enc) => {
			enc.writeString(t.name)
			enc.writeArray(t.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt64(p.committedOffset)
				penc.writeInt32(p.committedLeaderEpoch ?? -1)
				penc.writeNullableString(p.committedMetadata ?? null)
			})
		})
	}
}
