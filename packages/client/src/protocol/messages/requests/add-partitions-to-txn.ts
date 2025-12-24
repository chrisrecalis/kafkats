/**
 * AddPartitionsToTxn Request (API Key 24)
 *
 * Used to add partitions to a transaction before sending messages.
 * Must be called before producing to any new partition within a transaction.
 *
 * Supports v0-v4:
 * - v0-v2: non-flexible encoding
 * - v3+: flexible encoding
 * - v4+: batched transactions support (multiple transactionalIds)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface AddPartitionsToTxnTopic {
	/** Topic name */
	name: string
	/** Partitions to add to the transaction */
	partitions: number[]
}

export interface AddPartitionsToTxnRequest {
	/** The transactional ID */
	transactionalId: string
	/** Current producer ID */
	producerId: bigint
	/** Current producer epoch */
	producerEpoch: number
	/** Topics and partitions to add to the transaction */
	topics: AddPartitionsToTxnTopic[]
}

/**
 * Supported API versions for AddPartitionsToTxn request
 */
export const ADD_PARTITIONS_TO_TXN_VERSIONS = { min: 0, max: 4 }

export function encodeAddPartitionsToTxnRequest(
	encoder: IEncoder,
	version: number,
	request: AddPartitionsToTxnRequest
): void {
	if (version < ADD_PARTITIONS_TO_TXN_VERSIONS.min || version > ADD_PARTITIONS_TO_TXN_VERSIONS.max) {
		throw new Error(`Unsupported AddPartitionsToTxn version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.AddPartitionsToTxn, version)

	if (version >= 4) {
		// v4+ uses batched format with transactions array
		if (flexible) {
			encoder.writeCompactArray([request], (txn, enc) => {
				enc.writeCompactString(txn.transactionalId)
				enc.writeInt64(txn.producerId)
				enc.writeInt16(txn.producerEpoch)
				enc.writeBoolean(true) // verifyOnly = false means add partitions
				enc.writeCompactArray(txn.topics, (t, tenc) => {
					tenc.writeCompactString(t.name)
					tenc.writeCompactArray(t.partitions, (p, penc) => {
						penc.writeInt32(p)
						penc.writeEmptyTaggedFields()
					})
					tenc.writeEmptyTaggedFields()
				})
				enc.writeEmptyTaggedFields()
			})
			encoder.writeEmptyTaggedFields()
		}
	} else if (flexible) {
		// v3: flexible encoding, non-batched
		encoder.writeCompactString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeCompactArray(request.topics, (t, enc) => {
			enc.writeCompactString(t.name)
			enc.writeCompactArray(t.partitions, (p, penc) => {
				penc.writeInt32(p)
				penc.writeEmptyTaggedFields()
			})
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		// v0-v2: non-flexible encoding
		encoder.writeString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeArray(request.topics, (t, enc) => {
			enc.writeString(t.name)
			enc.writeArray(t.partitions, (p, penc) => {
				penc.writeInt32(p)
			})
		})
	}
}
