/**
 * InitProducerId Request (API Key 22)
 *
 * Used to allocate a producer ID for idempotent/transactional producing.
 * For idempotent-only producers, transactionalId is null.
 *
 * Supports v0-v4:
 * - v0-v1: non-flexible encoding
 * - v2+: flexible encoding
 * - v3+: includes producerId and producerEpoch for epoch bumping
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface InitProducerIdRequest {
	/** Transactional ID, or null for idempotent-only producer */
	transactionalId: string | null
	/** Transaction timeout in ms (use 0 for non-transactional) */
	transactionTimeoutMs: number
	/** Current producer ID for epoch bump (v3+), -1 for new allocation */
	producerId?: bigint
	/** Current producer epoch for epoch bump (v3+), -1 for new allocation */
	producerEpoch?: number
}

/**
 * Supported API versions for InitProducerId request
 */
export const INIT_PRODUCER_ID_VERSIONS = { min: 0, max: 4 }

export function encodeInitProducerIdRequest(encoder: IEncoder, version: number, request: InitProducerIdRequest): void {
	if (version < INIT_PRODUCER_ID_VERSIONS.min || version > INIT_PRODUCER_ID_VERSIONS.max) {
		throw new Error(`Unsupported InitProducerId version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.InitProducerId, version)

	if (flexible) {
		// Flexible encoding (v2+)
		// Transactional ID (nullable compact string)
		if (request.transactionalId === null) {
			encoder.writeUVarInt(0) // null marker for compact nullable string
		} else {
			encoder.writeCompactNullableString(request.transactionalId)
		}

		// Transaction timeout
		encoder.writeInt32(request.transactionTimeoutMs)

		// Producer ID and epoch (v3+)
		if (version >= 3) {
			encoder.writeInt64(request.producerId ?? -1n)
			encoder.writeInt16(request.producerEpoch ?? -1)
		}

		// Tagged fields
		encoder.writeUVarInt(0)
	} else {
		// Non-flexible encoding (v0-v1)
		encoder.writeNullableString(request.transactionalId)
		encoder.writeInt32(request.transactionTimeoutMs)
	}
}
