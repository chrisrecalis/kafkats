/**
 * FindCoordinator Request (API Key 10)
 *
 * Used to find the coordinator for a group or transactional ID.
 *
 * Supports v1-v5:
 * - v1-v2: non-flexible encoding (single key)
 * - v3+: flexible encoding with batch lookup (coordinatorKeys array)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export const COORDINATOR_TYPE = {
	GROUP: 0,
	TRANSACTION: 1,
} as const

export interface FindCoordinatorRequest {
	/** The key to look up (group ID or transactional ID) - used for v0-v3 */
	key: string
	/** Key type: 0=group, 1=transaction (v1+) */
	keyType: number
	/** Multiple keys to look up (v4+), if provided overrides single key */
	coordinatorKeys?: string[]
}

/**
 * Supported API versions for FindCoordinator request
 *
 * - v1-v2: non-flexible encoding
 * - v3-v5: flexible encoding with compact strings and tagged fields
 */
export const FIND_COORDINATOR_VERSIONS = { min: 1, max: 2 }

export function encodeFindCoordinatorRequest(
	encoder: IEncoder,
	version: number,
	request: FindCoordinatorRequest
): void {
	if (version < FIND_COORDINATOR_VERSIONS.min || version > FIND_COORDINATOR_VERSIONS.max) {
		throw new Error(`Unsupported FindCoordinator version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.FindCoordinator, version)

	if (flexible) {
		// Flexible encoding (v3+)
		// Key (v3 still has single key field for backwards compat, empty string in v4+)
		if (version >= 4) {
			encoder.writeCompactString('') // Empty key for v4+, uses coordinatorKeys instead
		} else {
			encoder.writeCompactString(request.key)
		}

		// Key type
		encoder.writeInt8(request.keyType)

		// Coordinator keys array (v4+)
		if (version >= 4) {
			const keys = request.coordinatorKeys ?? [request.key]
			encoder.writeCompactArray(keys, (k, enc) => {
				enc.writeCompactString(k)
			})
		}

		// Tagged fields
		encoder.writeUVarInt(0) // empty tagged fields
	} else {
		// Non-flexible encoding (v1-v2)
		encoder.writeString(request.key)

		// Key type (v1+)
		if (version >= 1) {
			encoder.writeInt8(request.keyType)
		}
	}
}
