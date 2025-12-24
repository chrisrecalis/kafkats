/**
 * TxnOffsetCommit Request (API Key 28)
 *
 * Used to commit consumer offsets as part of a transaction.
 * This is sent to the group coordinator, not the transaction coordinator.
 *
 * Supports v0-v3:
 * - v0-v2: non-flexible encoding
 * - v3+: flexible encoding
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface TxnOffsetCommitPartition {
	/** Partition index */
	partitionIndex: number
	/** The offset to commit */
	committedOffset: bigint
	/** The leader epoch (v2+), or -1 if not available */
	committedLeaderEpoch?: number
	/** Optional metadata for this offset */
	committedMetadata?: string | null
}

export interface TxnOffsetCommitTopic {
	/** Topic name */
	name: string
	/** Partitions with offsets to commit */
	partitions: TxnOffsetCommitPartition[]
}

export interface TxnOffsetCommitRequest {
	/** The transactional ID */
	transactionalId: string
	/** The consumer group ID */
	groupId: string
	/** Current producer ID */
	producerId: bigint
	/** Current producer epoch */
	producerEpoch: number
	/** The generation ID of the consumer group (-1 for producer-only transactions) */
	generationId: number
	/** The member ID of the consumer ('') for producer-only transactions) */
	memberId: string
	/** The group instance ID (v3+) */
	groupInstanceId?: string | null
	/** Topics with partitions to commit */
	topics: TxnOffsetCommitTopic[]
}

/**
 * Supported API versions for TxnOffsetCommit request
 */
export const TXN_OFFSET_COMMIT_VERSIONS = { min: 0, max: 3 }

export function encodeTxnOffsetCommitRequest(
	encoder: IEncoder,
	version: number,
	request: TxnOffsetCommitRequest
): void {
	if (version < TXN_OFFSET_COMMIT_VERSIONS.min || version > TXN_OFFSET_COMMIT_VERSIONS.max) {
		throw new Error(`Unsupported TxnOffsetCommit version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.TxnOffsetCommit, version)

	if (flexible) {
		encoder.writeCompactString(request.transactionalId)
		encoder.writeCompactString(request.groupId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		if (version >= 3) {
			encoder.writeInt32(request.generationId)
			encoder.writeCompactString(request.memberId)
			encoder.writeCompactNullableString(request.groupInstanceId ?? null)
		}
		encoder.writeCompactArray(request.topics, (t, enc) => {
			enc.writeCompactString(t.name)
			enc.writeCompactArray(t.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt64(p.committedOffset)
				if (version >= 2) {
					penc.writeInt32(p.committedLeaderEpoch ?? -1)
				}
				penc.writeCompactNullableString(p.committedMetadata ?? null)
				penc.writeEmptyTaggedFields()
			})
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.transactionalId)
		encoder.writeString(request.groupId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeArray(request.topics, (t, enc) => {
			enc.writeString(t.name)
			enc.writeArray(t.partitions, (p, penc) => {
				penc.writeInt32(p.partitionIndex)
				penc.writeInt64(p.committedOffset)
				if (version >= 2) {
					penc.writeInt32(p.committedLeaderEpoch ?? -1)
				}
				penc.writeNullableString(p.committedMetadata ?? null)
			})
		})
	}
}
