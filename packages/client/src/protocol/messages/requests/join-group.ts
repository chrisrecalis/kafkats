/**
 * JoinGroup Request (API Key 11)
 * Version 6+ uses flexible encoding.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface JoinGroupProtocol {
	name: string
	metadata: Buffer
}

export interface JoinGroupRequest {
	groupId: string
	sessionTimeoutMs: number
	rebalanceTimeoutMs: number
	memberId: string
	groupInstanceId?: string | null
	protocolType: string
	protocols: JoinGroupProtocol[]
	reason?: string | null
}

/**
 * Supported API versions for JoinGroup request
 *
 * - v0-v5: non-flexible encoding
 * - v6+: flexible encoding with compact strings and tagged fields
 */
export const JOIN_GROUP_VERSIONS = { min: 5, max: 5 }

export function encodeJoinGroupRequest(encoder: IEncoder, version: number, request: JoinGroupRequest): void {
	if (version < JOIN_GROUP_VERSIONS.min || version > JOIN_GROUP_VERSIONS.max) {
		throw new Error(`Unsupported JoinGroup version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.JoinGroup, version)

	if (flexible) {
		encoder.writeCompactString(request.groupId)
		encoder.writeInt32(request.sessionTimeoutMs)
		encoder.writeInt32(request.rebalanceTimeoutMs)
		encoder.writeCompactString(request.memberId)
		encoder.writeCompactNullableString(request.groupInstanceId ?? null)
		encoder.writeCompactString(request.protocolType)
		encoder.writeCompactArray(request.protocols, (p, enc) => {
			enc.writeCompactString(p.name)
			enc.writeCompactBytes(p.metadata)
			enc.writeEmptyTaggedFields()
		})

		if (version >= 8) {
			encoder.writeCompactNullableString(request.reason ?? null)
		}
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.groupId)
		encoder.writeInt32(request.sessionTimeoutMs)
		encoder.writeInt32(request.rebalanceTimeoutMs)
		encoder.writeString(request.memberId)
		encoder.writeNullableString(request.groupInstanceId ?? null)
		encoder.writeString(request.protocolType)
		encoder.writeArray(request.protocols, (p, enc) => {
			enc.writeString(p.name)
			enc.writeBytes(p.metadata)
		})
	}
}
