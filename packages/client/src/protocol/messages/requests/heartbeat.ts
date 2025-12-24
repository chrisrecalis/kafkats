/**
 * Heartbeat Request (API Key 12)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface HeartbeatRequest {
	groupId: string
	generationId: number
	memberId: string
	groupInstanceId?: string | null
}

/**
 * Supported API versions for Heartbeat request
 *
 * - v0-v3: non-flexible encoding
 * - v4+: flexible encoding with compact strings and tagged fields
 */
export const HEARTBEAT_VERSIONS = { min: 3, max: 3 }

export function encodeHeartbeatRequest(encoder: IEncoder, version: number, request: HeartbeatRequest): void {
	if (version < HEARTBEAT_VERSIONS.min || version > HEARTBEAT_VERSIONS.max) {
		throw new Error(`Unsupported Heartbeat version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.Heartbeat, version)

	if (flexible) {
		encoder.writeCompactString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeCompactString(request.memberId)
		encoder.writeCompactNullableString(request.groupInstanceId ?? null)
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeString(request.memberId)
		encoder.writeNullableString(request.groupInstanceId ?? null)
	}
}
