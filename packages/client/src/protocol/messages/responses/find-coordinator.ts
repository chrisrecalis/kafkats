/**
 * FindCoordinator Response (API Key 10)
 *
 * Supports v1-v5:
 * - v1-v2: non-flexible encoding (single coordinator)
 * - v3+: flexible encoding with compact strings
 * - v4+: batch lookup support (coordinators array)
 */

import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

/**
 * Coordinator info (used in v4+ batch responses)
 */
export interface Coordinator {
	key: string
	nodeId: number
	host: string
	port: number
	errorCode: ErrorCode
	errorMessage: string | null
}

export interface FindCoordinatorResponse {
	throttleTimeMs: number
	/** Error code (v0-v3, always NONE for v4+) */
	errorCode: ErrorCode
	/** Error message (v1-v3) */
	errorMessage: string | null
	/** Node ID (v0-v3, use coordinators array in v4+) */
	nodeId: number
	/** Host (v0-v3, use coordinators array in v4+) */
	host: string
	/** Port (v0-v3, use coordinators array in v4+) */
	port: number
	/** Array of coordinators (v4+) */
	coordinators?: Coordinator[]
}

export function decodeFindCoordinatorResponse(decoder: IDecoder, version: number): FindCoordinatorResponse {
	const flexible = isFlexibleVersion(ApiKey.FindCoordinator, version)

	// Throttle time (v1+)
	const throttleTimeMs = version >= 1 ? decoder.readInt32() : 0

	if (version >= 4) {
		// v4+: Batch response format with coordinators array
		const coordinators = decoder.readCompactArray(d => {
			const key = d.readCompactString()
			const nodeId = d.readInt32()
			const host = d.readCompactString()
			const port = d.readInt32()
			const errorCode = d.readInt16() as ErrorCode
			const errorMessage = d.readCompactNullableString()
			d.skipTaggedFields()
			return { key, nodeId, host, port, errorCode, errorMessage }
		})

		// Skip response-level tagged fields
		decoder.skipTaggedFields()

		// For backwards compatibility, populate single-coordinator fields from first result
		const first = coordinators[0]
		return {
			throttleTimeMs,
			errorCode: first?.errorCode ?? ErrorCode.None,
			errorMessage: first?.errorMessage ?? null,
			nodeId: first?.nodeId ?? -1,
			host: first?.host ?? '',
			port: first?.port ?? -1,
			coordinators,
		}
	}

	// v0-v3: Single coordinator format
	let errorCode: ErrorCode
	let errorMessage: string | null
	let nodeId: number
	let host: string
	let port: number

	if (flexible) {
		// v3: Flexible encoding
		errorCode = decoder.readInt16() as ErrorCode
		errorMessage = decoder.readCompactNullableString()
		nodeId = decoder.readInt32()
		host = decoder.readCompactString()
		port = decoder.readInt32()
		decoder.skipTaggedFields()
	} else {
		// v1-v2: Non-flexible encoding
		errorCode = decoder.readInt16() as ErrorCode
		errorMessage = version >= 1 ? decoder.readNullableString() : null
		nodeId = decoder.readInt32()
		host = decoder.readString()
		port = decoder.readInt32()
	}

	return { throttleTimeMs, errorCode, errorMessage, nodeId, host, port }
}
