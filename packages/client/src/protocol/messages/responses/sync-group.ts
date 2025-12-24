/**
 * SyncGroup Response (API Key 14)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface SyncGroupResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	protocolType: string | null
	protocolName: string | null
	assignment: Buffer
}

export function decodeSyncGroupResponse(decoder: IDecoder, version: number): SyncGroupResponse {
	const flexible = isFlexibleVersion(ApiKey.SyncGroup, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode

	let protocolType: string | null = null
	let protocolName: string | null = null
	let assignment: Buffer

	if (flexible) {
		if (version >= 5) {
			protocolType = decoder.readCompactNullableString()
			protocolName = decoder.readCompactNullableString()
		}
		assignment = decoder.readCompactBytes()
		decoder.skipTaggedFields()
	} else {
		assignment = decoder.readBytes()
	}

	return { throttleTimeMs, errorCode, protocolType, protocolName, assignment }
}
