/**
 * SaslHandshake Response (API Key 17)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import type { ErrorCode } from '@/protocol/messages/error-codes.js'
import { SASL_HANDSHAKE_VERSIONS } from '@/protocol/messages/requests/sasl-handshake.js'

export interface SaslHandshakeResponse {
	/** Error code (0 = success) */
	errorCode: ErrorCode
	/** Mechanisms enabled on the broker */
	enabledMechanisms: string[]
}

export function decodeSaslHandshakeResponse(decoder: IDecoder, version: number): SaslHandshakeResponse {
	if (version < SASL_HANDSHAKE_VERSIONS.min || version > SASL_HANDSHAKE_VERSIONS.max) {
		throw new Error(`Unsupported SaslHandshake version: ${version}`)
	}

	// SaslHandshake is NOT flexible for any version
	const errorCode = decoder.readInt16() as ErrorCode
	const enabledMechanisms = decoder.readArray(d => d.readString())

	return { errorCode, enabledMechanisms }
}
