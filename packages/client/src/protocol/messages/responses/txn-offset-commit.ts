/**
 * TxnOffsetCommit Response (API Key 28)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface TxnOffsetCommitPartitionResponse {
	/** Partition index */
	partitionIndex: number
	/** Error code for this partition */
	errorCode: ErrorCode
}

export interface TxnOffsetCommitTopicResponse {
	/** Topic name */
	name: string
	/** Results for each partition */
	partitions: TxnOffsetCommitPartitionResponse[]
}

export interface TxnOffsetCommitResponse {
	/** Duration of the request throttle */
	throttleTimeMs: number
	/** Results by topic */
	topics: TxnOffsetCommitTopicResponse[]
}

export function decodeTxnOffsetCommitResponse(decoder: IDecoder, version: number): TxnOffsetCommitResponse {
	const flexible = isFlexibleVersion(ApiKey.TxnOffsetCommit, version)

	const throttleTimeMs = decoder.readInt32()

	let topics: TxnOffsetCommitTopicResponse[]
	if (flexible) {
		topics = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const partitions = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				pd.skipTaggedFields()
				return { partitionIndex, errorCode }
			})
			d.skipTaggedFields()
			return { name, partitions }
		})
		decoder.skipTaggedFields()
	} else {
		topics = decoder.readArray(d => {
			const name = d.readString()
			const partitions = d.readArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				return { partitionIndex, errorCode }
			})
			return { name, partitions }
		})
	}

	return { throttleTimeMs, topics }
}
