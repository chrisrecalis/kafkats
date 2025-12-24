/**
 * AddOffsetsToTxn Response (API Key 25)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface AddOffsetsToTxnResponse {
	/** Duration of the request throttle */
	throttleTimeMs: number
	/** Error code */
	errorCode: ErrorCode
}

export function decodeAddOffsetsToTxnResponse(decoder: IDecoder, version: number): AddOffsetsToTxnResponse {
	const flexible = isFlexibleVersion(ApiKey.AddOffsetsToTxn, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode

	if (flexible) {
		decoder.skipTaggedFields()
	}

	return { throttleTimeMs, errorCode }
}
