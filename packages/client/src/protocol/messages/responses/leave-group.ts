/**
 * LeaveGroup Response (API Key 13)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

export interface LeaveGroupMemberResponse {
	memberId: string
	groupInstanceId: string | null
	errorCode: ErrorCode
}

export interface LeaveGroupResponse {
	throttleTimeMs: number
	errorCode: ErrorCode
	members: LeaveGroupMemberResponse[]
}

export function decodeLeaveGroupResponse(decoder: IDecoder, version: number): LeaveGroupResponse {
	const flexible = isFlexibleVersion(ApiKey.LeaveGroup, version)

	const throttleTimeMs = decoder.readInt32()
	const errorCode = decoder.readInt16() as ErrorCode

	let members: LeaveGroupMemberResponse[] = []

	if (version >= 3) {
		if (flexible) {
			members = decoder.readCompactArray(d => {
				const memberId = d.readCompactString()
				const groupInstanceId = d.readCompactNullableString()
				const memberErrorCode = d.readInt16() as ErrorCode
				d.skipTaggedFields()
				return { memberId, groupInstanceId, errorCode: memberErrorCode }
			})
			decoder.skipTaggedFields()
		} else {
			members = decoder.readArray(d => {
				const memberId = d.readString()
				const groupInstanceId = d.readNullableString()
				const memberErrorCode = d.readInt16() as ErrorCode
				return { memberId, groupInstanceId, errorCode: memberErrorCode }
			})
		}
	}

	return { throttleTimeMs, errorCode, members }
}
