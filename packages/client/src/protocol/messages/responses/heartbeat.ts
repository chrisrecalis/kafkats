/**
 * Heartbeat Response (API Key 12)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface HeartbeatResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
}

export function decodeHeartbeatResponse(decoder: IDecoder, version: number): HeartbeatResponse {
	const flexible = isFlexibleVersion(ApiKey.Heartbeat, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode

	if (flexible) {
		decoder.skipTaggedFields()
	}

	return { throttleTimeMs, errorCode }
}
