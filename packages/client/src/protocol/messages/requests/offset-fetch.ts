/**
 * OffsetFetch Request (API Key 9)
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

export interface OffsetFetchPartition {
	partitionIndex: number
}

export interface OffsetFetchTopic {
	name: string
	partitions: OffsetFetchPartition[]
}

export interface OffsetFetchGroup {
	groupId: string
	topics: OffsetFetchTopic[] | null
}

export interface OffsetFetchRequest {
	groupId: string
	topics: OffsetFetchTopic[] | null
	/** Multiple groups (v8+) */
	groups?: OffsetFetchGroup[]
	requireStable?: boolean
}

/**
 * Supported API versions for OffsetFetch request
 *
 * - v0-v5: non-flexible encoding
 * - v6+: flexible encoding with compact strings and tagged fields
 */
export const OFFSET_FETCH_VERSIONS = { min: 5, max: 5 }

export function encodeOffsetFetchRequest(encoder: IEncoder, version: number, request: OffsetFetchRequest): void {
	if (version < OFFSET_FETCH_VERSIONS.min || version > OFFSET_FETCH_VERSIONS.max) {
		throw new Error(`Unsupported OffsetFetch version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.OffsetFetch, version)

	if (version >= 8) {
		// Batch API (v8+)
		const groups = request.groups ?? [{ groupId: request.groupId, topics: request.topics }]

		if (flexible) {
			encoder.writeCompactArray(groups, (g, enc) => {
				enc.writeCompactString(g.groupId)
				enc.writeCompactNullableArray(g.topics, (t, tenc) => {
					tenc.writeCompactString(t.name)
					tenc.writeCompactArray(t.partitions, (p, penc) => {
						penc.writeInt32(p.partitionIndex)
						penc.writeEmptyTaggedFields()
					})
					tenc.writeEmptyTaggedFields()
				})
				enc.writeEmptyTaggedFields()
			})
			encoder.writeBoolean(request.requireStable ?? false)
			encoder.writeEmptyTaggedFields()
		} else {
			encoder.writeArray(groups, (g, enc) => {
				enc.writeString(g.groupId)
				enc.writeNullableArray(g.topics, (t, tenc) => {
					tenc.writeString(t.name)
					tenc.writeArray(t.partitions, (p, penc) => {
						penc.writeInt32(p.partitionIndex)
					})
				})
			})
			encoder.writeBoolean(request.requireStable ?? false)
		}
	} else {
		// Single group API (v6-v7)
		if (flexible) {
			encoder.writeCompactString(request.groupId)
			encoder.writeCompactNullableArray(request.topics, (t, enc) => {
				enc.writeCompactString(t.name)
				enc.writeCompactArray(t.partitions, (p, penc) => {
					penc.writeInt32(p.partitionIndex)
					penc.writeEmptyTaggedFields()
				})
				enc.writeEmptyTaggedFields()
			})
			// requireStable was added in v7 (KIP-320)
			if (version >= 7) {
				encoder.writeBoolean(request.requireStable ?? false)
			}
			encoder.writeEmptyTaggedFields()
		} else {
			encoder.writeString(request.groupId)
			encoder.writeNullableArray(request.topics, (t, enc) => {
				enc.writeString(t.name)
				enc.writeArray(t.partitions, (p, penc) => {
					penc.writeInt32(p.partitionIndex)
				})
			})
		}
	}
}
