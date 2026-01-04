/**
 * ShareGroupHeartbeat Response (API Key 76)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { SHARE_GROUP_HEARTBEAT_VERSIONS } from '@/protocol/messages/requests/share-group-heartbeat.js'

export interface ShareGroupAssignment {
	topicId: string
	partitions: number[]
}

export interface ShareGroupHeartbeatResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	errorMessage: string | null
	memberId: string | null
	memberEpoch: number
	heartbeatIntervalMs: number
	assignment: ShareGroupAssignment[] | null
}

export function decodeShareGroupHeartbeatResponse(decoder: IDecoder, version: number): ShareGroupHeartbeatResponse {
	if (version < SHARE_GROUP_HEARTBEAT_VERSIONS.min || version > SHARE_GROUP_HEARTBEAT_VERSIONS.max) {
		throw new Error(`Unsupported ShareGroupHeartbeat version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.ShareGroupHeartbeat, version)
	if (!flexible) {
		throw new Error(`ShareGroupHeartbeat v${version} must be flexible`)
	}

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode
	const errorMessage = decoder.readCompactNullableString()
	const memberId = decoder.readCompactNullableString()
	const memberEpoch = decoder.readInt32()
	const heartbeatIntervalMs = decoder.readInt32()

	// Assignment is a nullable struct. Kafka encodes nullable structs with a single-byte marker:
	// - -1 (0xff): null
	// -  1: present, followed by the struct contents.
	let assignment: ShareGroupAssignment[] | null = null
	const assignmentMarker = decoder.readInt8()
	if (assignmentMarker === -1) {
		assignment = null
	} else if (assignmentMarker === 1) {
		assignment = decoder.readCompactArray(d => {
			const topicId = d.readUUID()
			const partitions = d.readCompactArray(pd => pd.readInt32())
			d.skipTaggedFields()
			return { topicId, partitions }
		})
		decoder.skipTaggedFields()
	} else {
		throw new Error(`Invalid assignment marker: ${assignmentMarker}`)
	}

	decoder.skipTaggedFields()

	return {
		throttleTimeMs,
		errorCode,
		errorMessage,
		memberId,
		memberEpoch,
		heartbeatIntervalMs,
		assignment,
	}
}
