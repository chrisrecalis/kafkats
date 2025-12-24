/**
 * LeaveGroup Request (API Key 13)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface LeaveGroupMember {
	memberId: string
	groupInstanceId?: string | null
	reason?: string | null
}

export interface LeaveGroupRequest {
	groupId: string
	/** Single member (v0-v2) */
	memberId?: string
	/** Multiple members (v3+) */
	members?: LeaveGroupMember[]
}

/**
 * Supported API versions for LeaveGroup request
 *
 * - v0-v3: non-flexible encoding
 * - v4+: flexible encoding with compact strings and tagged fields
 */
export const LEAVE_GROUP_VERSIONS = { min: 3, max: 3 }

export function encodeLeaveGroupRequest(encoder: IEncoder, version: number, request: LeaveGroupRequest): void {
	if (version < LEAVE_GROUP_VERSIONS.min || version > LEAVE_GROUP_VERSIONS.max) {
		throw new Error(`Unsupported LeaveGroup version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.LeaveGroup, version)

	if (flexible) {
		encoder.writeCompactString(request.groupId)
		encoder.writeCompactArray(request.members ?? [], (m, enc) => {
			enc.writeCompactString(m.memberId)
			enc.writeCompactNullableString(m.groupInstanceId ?? null)
			if (version >= 5) {
				enc.writeCompactNullableString(m.reason ?? null)
			}
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.groupId)
		encoder.writeArray(request.members ?? [], (m, enc) => {
			enc.writeString(m.memberId)
			enc.writeNullableString(m.groupInstanceId ?? null)
		})
	}
}
