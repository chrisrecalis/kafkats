/**
 * InitProducerId Response (API Key 22)
 *
 * Returns allocated producer ID and epoch for idempotent/transactional producing.
 *
 * Supports v0-v4:
 * - v0-v1: non-flexible encoding
 * - v2+: flexible encoding
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface InitProducerIdResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Error code */
	errorCode: ErrorCode
	/** Allocated producer ID (-1 on error) */
	producerId: bigint
	/** Allocated producer epoch (-1 on error) */
	producerEpoch: number
}

export function decodeInitProducerIdResponse(decoder: IDecoder, version: number): InitProducerIdResponse {
	const flexible = isFlexibleVersion(ApiKey.InitProducerId, version)

	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Error code
	const errorCode = decoder.readInt16() as ErrorCode

	// Producer ID
	const producerId = decoder.readInt64()

	// Producer epoch
	const producerEpoch = decoder.readInt16()

	// Skip tagged fields for flexible versions
	if (flexible) {
		decoder.skipTaggedFields()
	}

	return {
		throttleTimeMs,
		errorCode,
		producerId,
		producerEpoch,
	}
}
