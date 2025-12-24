/**
 * SaslAuthenticate Response (API Key 36)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import type { ErrorCode } from '@/protocol/messages/error-codes.js'
import { SASL_AUTHENTICATE_VERSIONS } from '@/protocol/messages/requests/sasl-authenticate.js'

export interface SaslAuthenticateResponse {
	/** Error code (0 = success) */
	errorCode: ErrorCode
	/** Error message if authentication failed */
	errorMessage: string | null
	/** The SASL authentication bytes from the server */
	authBytes: Buffer
	/** Session lifetime in milliseconds (v1+) */
	sessionLifetimeMs?: bigint
}

export function decodeSaslAuthenticateResponse(decoder: IDecoder, version: number): SaslAuthenticateResponse {
	if (version < SASL_AUTHENTICATE_VERSIONS.min || version > SASL_AUTHENTICATE_VERSIONS.max) {
		throw new Error(`Unsupported SaslAuthenticate version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.SaslAuthenticate, version)

	const errorCode = decoder.readInt16() as ErrorCode

	let errorMessage: string | null
	let authBytes: Buffer

	if (flexible) {
		errorMessage = decoder.readCompactNullableString()
		authBytes = decoder.readCompactBytes()
	} else {
		errorMessage = decoder.readNullableString()
		authBytes = decoder.readBytes()
	}

	let sessionLifetimeMs: bigint | undefined
	if (version >= 1) {
		sessionLifetimeMs = decoder.readInt64()
	}

	if (flexible) {
		decoder.skipTaggedFields()
	}

	return { errorCode, errorMessage, authBytes, sessionLifetimeMs }
}
