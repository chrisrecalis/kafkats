/**
 * JoinGroup Response (API Key 11)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface JoinGroupMember {
	memberId: string
	groupInstanceId: string | null
	metadata: Buffer
}

export interface JoinGroupResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	generationId: number
	protocolType: string | null
	protocolName: string | null
	leader: string
	skipAssignment: boolean
	memberId: string
	members: JoinGroupMember[]
}

export function decodeJoinGroupResponse(decoder: IDecoder, version: number): JoinGroupResponse {
	const flexible = isFlexibleVersion(ApiKey.JoinGroup, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode
	const generationId = decoder.readInt32()

	let protocolType: string | null = null
	let protocolName: string | null
	let leader: string
	let skipAssignment = false
	let memberId: string
	let members: JoinGroupMember[]

	if (flexible) {
		if (version >= 7) {
			protocolType = decoder.readCompactNullableString()
		}
		protocolName = decoder.readCompactNullableString()
		leader = decoder.readCompactString()

		if (version >= 9) {
			skipAssignment = decoder.readBoolean()
		}

		memberId = decoder.readCompactString()
		members = decoder.readCompactArray(d => {
			const mid = d.readCompactString()
			const gid = d.readCompactNullableString()
			const metadata = d.readCompactBytes()
			d.skipTaggedFields()
			return { memberId: mid, groupInstanceId: gid, metadata }
		})
		decoder.skipTaggedFields()
	} else {
		protocolName = decoder.readNullableString()
		leader = decoder.readString()
		memberId = decoder.readString()
		members = decoder.readArray(d => {
			const mid = d.readString()
			const gid = d.readNullableString()
			const metadata = d.readBytes()
			return { memberId: mid, groupInstanceId: gid, metadata }
		})
	}

	return {
		throttleTimeMs,
		errorCode,
		generationId,
		protocolType,
		protocolName,
		leader,
		skipAssignment,
		memberId,
		members,
	}
}
