/**
 * EndTxn Response (API Key 26)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface EndTxnResponse {
	/** Duration of the request throttle */
	throttleTimeMs: number
	/** Error code */
	errorCode: ErrorCode
}

export function decodeEndTxnResponse(decoder: IDecoder, version: number): EndTxnResponse {
	const flexible = isFlexibleVersion(ApiKey.EndTxn, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode

	if (flexible) {
		decoder.skipTaggedFields()
	}

	return { throttleTimeMs, errorCode }
}
