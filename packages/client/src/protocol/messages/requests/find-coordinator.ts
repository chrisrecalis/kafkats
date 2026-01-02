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
	SHARE: 2,
} as const

export interface FindCoordinatorRequest {
	/** The key to look up (group ID or transactional ID) - used for v0-v3 */
	key: string
	/** Key type: 0=group, 1=transaction, 2=share (v1+) */
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
export const FIND_COORDINATOR_VERSIONS = { min: 1, max: 6 }

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
		if (version >= 4) {
			// v4+: key field removed, replaced by coordinatorKeys array
			encoder.writeInt8(request.keyType)

			const keys = request.coordinatorKeys ?? [request.key]
			// In flexible versions, coordinatorKeys is a compact array of structs.
			// Each element includes the key and its own tagged-fields section.
			encoder.writeCompactArray(keys, (k, enc) => {
				enc.writeCompactString(k)
				enc.writeEmptyTaggedFields()
			})
		} else {
			// v3: single key field
			encoder.writeCompactString(request.key)
			encoder.writeInt8(request.keyType)
		}

		// Tagged fields
		encoder.writeEmptyTaggedFields()
	} else {
		// Non-flexible encoding (v1-v2)
		encoder.writeString(request.key)

		// Key type (v1+)
		if (version >= 1) {
			encoder.writeInt8(request.keyType)
		}
	}
}
