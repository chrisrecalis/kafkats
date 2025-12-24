/**
 * SaslAuthenticate Request (API Key 36)
 *
 * Used to send SASL authentication data.
 * v0-v1: non-flexible encoding
 * v2+: flexible encoding
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface SaslAuthenticateRequest {
	/** The SASL authentication bytes from the client */
	authBytes: Buffer
}

/**
 * Supported API versions for SaslAuthenticate request
 * v0: initial version
 * v1: adds session lifetime in response
 * v2: flexible encoding
 */
export const SASL_AUTHENTICATE_VERSIONS = { min: 0, max: 2 }

export function encodeSaslAuthenticateRequest(
	encoder: IEncoder,
	version: number,
	request: SaslAuthenticateRequest
): void {
	if (version < SASL_AUTHENTICATE_VERSIONS.min || version > SASL_AUTHENTICATE_VERSIONS.max) {
		throw new Error(`Unsupported SaslAuthenticate version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.SaslAuthenticate, version)

	if (flexible) {
		encoder.writeCompactBytes(request.authBytes)
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeBytes(request.authBytes)
	}
}
