/**
 * EndTxn Request (API Key 26)
 *
 * Used to commit or abort a transaction.
 *
 * Supports v0-v3:
 * - v0-v2: non-flexible encoding
 * - v3+: flexible encoding
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface EndTxnRequest {
	/** The transactional ID */
	transactionalId: string
	/** Current producer ID */
	producerId: bigint
	/** Current producer epoch */
	producerEpoch: number
	/** True to commit, false to abort */
	committed: boolean
}

/**
 * Supported API versions for EndTxn request
 */
export const END_TXN_VERSIONS = { min: 0, max: 3 }

export function encodeEndTxnRequest(encoder: IEncoder, version: number, request: EndTxnRequest): void {
	if (version < END_TXN_VERSIONS.min || version > END_TXN_VERSIONS.max) {
		throw new Error(`Unsupported EndTxn version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.EndTxn, version)

	if (flexible) {
		encoder.writeCompactString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeBoolean(request.committed)
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.transactionalId)
		encoder.writeInt64(request.producerId)
		encoder.writeInt16(request.producerEpoch)
		encoder.writeBoolean(request.committed)
	}
}
