/**
 * ShareGroupHeartbeat Request (API Key 76)
 *
 * Used by share consumers to join/maintain membership in a Share Group.
 *
 * Flexible (tagged fields) in all supported versions.
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export const SHARE_GROUP_HEARTBEAT_VERSIONS = {
	min: 1,
	max: 1,
}

export interface ShareGroupHeartbeatRequest {
	groupId: string
	memberId: string
	memberEpoch: number
	rackId?: string | null
	subscribedTopicNames: string[] | null
}

export function encodeShareGroupHeartbeatRequest(
	encoder: IEncoder,
	version: number,
	request: ShareGroupHeartbeatRequest
): void {
	if (version < SHARE_GROUP_HEARTBEAT_VERSIONS.min || version > SHARE_GROUP_HEARTBEAT_VERSIONS.max) {
		throw new Error(`Unsupported ShareGroupHeartbeat version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareGroupHeartbeat, version)
	if (!flexible) {
		throw new Error(`ShareGroupHeartbeat v${version} must be flexible`)
	}

	encoder.writeCompactString(request.groupId)
	encoder.writeCompactString(request.memberId)
	encoder.writeInt32(request.memberEpoch)
	encoder.writeCompactNullableString(request.rackId ?? null)
	encoder.writeCompactNullableArray(request.subscribedTopicNames, (name, e) => {
		e.writeCompactString(name)
	})
	encoder.writeEmptyTaggedFields()
}
