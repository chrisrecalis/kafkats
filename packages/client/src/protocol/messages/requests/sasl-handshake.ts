/**
 * SaslHandshake Request (API Key 17)
 *
 * Used to initiate SASL authentication and verify mechanism support.
 * Note: SaslHandshake is NOT flexible for any version.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'

export interface SaslHandshakeRequest {
	/** The SASL mechanism chosen by the client */
	mechanism: string
}

/**
 * Supported API versions for SaslHandshake request
 * v0: initial version
 * v1: adds enabled mechanism list in response (NOT flexible)
 */
export const SASL_HANDSHAKE_VERSIONS = { min: 0, max: 1 }

export function encodeSaslHandshakeRequest(encoder: IEncoder, version: number, request: SaslHandshakeRequest): void {
	if (version < SASL_HANDSHAKE_VERSIONS.min || version > SASL_HANDSHAKE_VERSIONS.max) {
		throw new Error(`Unsupported SaslHandshake version: ${version}`)
	}

	// SaslHandshake is NOT flexible for any version
	encoder.writeString(request.mechanism)
}
