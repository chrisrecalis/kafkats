/**
 * AddOffsetsToTxn Request (API Key 25)
 *
 * Used to add a consumer group's offsets to a transaction.
 * This tells the transaction coordinator that the consumer group's offsets
 * will be committed as part of this transaction.
 *
 * Supports v0-v3:
 * - v0-v2: non-flexible encoding
 * - v3+: flexible encoding
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface AddOffsetsToTxnRequest {
	/** The transactional ID */
	transactionalId: string
	/** Current producer ID */
	producerId: bigint
	/** Current producer epoch */
	producerEpoch: number
	/** The consumer group ID whose offsets will be committed */
	groupId: string
}

/**
 * Supported API versions for AddOffsetsToTxn request
 */
export const ADD_OFFSETS_TO_TXN_VERSIONS = { min: 0, max: 3 }

export function encodeAddOffsetsToTxnRequest(
	encoder: IEncoder,
	version: number,
	request: AddOffsetsToTxnRequest
): void {
	if (version < ADD_OFFSETS_TO_TXN_VERSIONS.min || version > ADD_OFFSETS_TO_TXN_VERSIONS.max) {
		throw new Error(`Unsupported AddOffsetsToTxn version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.AddOffsetsToTxn, version)

	if (flexible) {
		encoder.writeCompactString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeCompactString(request.groupId)
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeString(request.groupId)
	}
}
