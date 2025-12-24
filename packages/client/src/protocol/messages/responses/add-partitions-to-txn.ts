/**
 * AddPartitionsToTxn Response (API Key 24)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface AddPartitionsToTxnPartitionResult {
	/** Partition index */
	partitionIndex: number
	/** Error code for this partition */
	errorCode: ErrorCode
}

export interface AddPartitionsToTxnTopicResult {
	/** Topic name */
	name: string
	/** Results for each partition */
	resultsByPartition: AddPartitionsToTxnPartitionResult[]
}

export interface AddPartitionsToTxnResponse {
	/** Duration of the request throttle */
	throttleTimeMs: number
	/** Top-level error code (v4+), or None for earlier versions */
	errorCode: ErrorCode
	/** Results by topic */
	results: AddPartitionsToTxnTopicResult[]
}

export function decodeAddPartitionsToTxnResponse(decoder: IDecoder, version: number): AddPartitionsToTxnResponse {
	const flexible = isFlexibleVersion(ApiKey.AddPartitionsToTxn, version)

	const throttleTimeMs = decoder.readInt32()

	if (version >= 4) {
		// v4+: batched response format
		const resultsByTransaction = flexible
			? decoder.readCompactArray(d => {
					const transactionalId = d.readCompactString()
					const topicResults = d.readCompactArray(td => {
						const name = td.readCompactString()
						const resultsByPartition = td.readCompactArray(pd => {
							const partitionIndex = pd.readInt32()
							const errorCode = pd.readInt16() as ErrorCode
							pd.skipTaggedFields()
							return { partitionIndex, errorCode }
						})
						td.skipTaggedFields()
						return { name, resultsByPartition }
					})
					d.skipTaggedFields()
					return { transactionalId, topicResults }
				})
			: []

		if (flexible) {
			decoder.skipTaggedFields()
		}

		// For single transaction, return the first result
		const firstTxn = resultsByTransaction[0]
		return {
			throttleTimeMs,
			errorCode: ErrorCode.None,
			results: firstTxn?.topicResults ?? [],
		}
	}

	// v0-v3: non-batched response
	let results: AddPartitionsToTxnTopicResult[]
	if (flexible) {
		results = decoder.readCompactArray(d => {
			const name = d.readCompactString()
			const resultsByPartition = d.readCompactArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				pd.skipTaggedFields()
				return { partitionIndex, errorCode }
			})
			d.skipTaggedFields()
			return { name, resultsByPartition }
		})
		decoder.skipTaggedFields()
	} else {
		results = decoder.readArray(d => {
			const name = d.readString()
			const resultsByPartition = d.readArray(pd => {
				const partitionIndex = pd.readInt32()
				const errorCode = pd.readInt16() as ErrorCode
				return { partitionIndex, errorCode }
			})
			return { name, resultsByPartition }
		})
	}

	return { throttleTimeMs, errorCode: ErrorCode.None, results }
}
