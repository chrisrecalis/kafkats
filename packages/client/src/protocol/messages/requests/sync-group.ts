/**
 * SyncGroup Request (API Key 14)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface SyncGroupAssignment {
	memberId: string
	assignment: Buffer
}

export interface SyncGroupRequest {
	groupId: string
	generationId: number
	memberId: string
	groupInstanceId?: string | null
	protocolType?: string | null
	protocolName?: string | null
	assignments: SyncGroupAssignment[]
}

/**
 * Supported API versions for SyncGroup request
 *
 * - v0-v3: non-flexible encoding
 * - v4+: flexible encoding with compact strings and tagged fields
 */
export const SYNC_GROUP_VERSIONS = { min: 3, max: 3 }

export function encodeSyncGroupRequest(encoder: IEncoder, version: number, request: SyncGroupRequest): void {
	if (version < SYNC_GROUP_VERSIONS.min || version > SYNC_GROUP_VERSIONS.max) {
		throw new Error(`Unsupported SyncGroup version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.SyncGroup, version)

	if (flexible) {
		encoder.writeCompactString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeCompactString(request.memberId)
		encoder.writeCompactNullableString(request.groupInstanceId ?? null)

		if (version >= 5) {
			encoder.writeCompactNullableString(request.protocolType ?? null)
			encoder.writeCompactNullableString(request.protocolName ?? null)
		}

		encoder.writeCompactArray(request.assignments, (a, enc) => {
			enc.writeCompactString(a.memberId)
			enc.writeCompactBytes(a.assignment)
			enc.writeEmptyTaggedFields()
		})
		encoder.writeEmptyTaggedFields()
	} else {
		encoder.writeString(request.groupId)
		encoder.writeInt32(request.generationId)
		encoder.writeString(request.memberId)
		encoder.writeNullableString(request.groupInstanceId ?? null)
		encoder.writeArray(request.assignments, (a, enc) => {
			enc.writeString(a.memberId)
			enc.writeBytes(a.assignment)
		})
	}
}
