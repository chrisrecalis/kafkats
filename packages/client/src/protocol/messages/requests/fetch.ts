/**
 * Fetch Request (API Key 1)
 *
 * Used to fetch records from Kafka brokers.
 *
 * Version 12+ uses flexible encoding (compact types + tagged fields).
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Partition to fetch data from
 */
export interface FetchRequestPartition {
	/** Partition index */
	partitionIndex: number
	/** Current leader epoch (v9+, -1 if not known) */
	currentLeaderEpoch?: number
	/** Offset to start fetching from */
	fetchOffset: bigint
	/** Last fetched epoch (v12+, -1 if not known) */
	lastFetchedEpoch?: number
	/** Log start offset (v5+) */
	logStartOffset?: bigint
	/** Maximum bytes to fetch from this partition */
	partitionMaxBytes: number
}

/**
 * Topic to fetch data from
 */
export interface FetchRequestTopic {
	/** Topic name (or null to use topicId) */
	topic: string
	/** Topic ID (v13+) */
	topicId?: string
	/** Partitions to fetch */
	partitions: FetchRequestPartition[]
}

/**
 * Forgotten topic for incremental fetch (v7+)
 */
export interface ForgottenTopic {
	/** Topic name (or null to use topicId) */
	topic: string
	/** Topic ID (v13+) */
	topicId?: string
	/** Partition indices to forget */
	partitions: number[]
}

/**
 * Fetch request data
 */
export interface FetchRequest {
	/** Cluster ID (v12+, null for clients) */
	clusterId?: string | null
	/** Replica ID (-1 for consumers) */
	replicaId?: number
	/** Maximum time to wait in milliseconds */
	maxWaitMs: number
	/** Minimum bytes to accumulate before responding */
	minBytes: number
	/** Maximum bytes to return (v3+) */
	maxBytes?: number
	/** Isolation level: 0=read_uncommitted, 1=read_committed (v4+) */
	isolationLevel?: number
	/** Fetch session ID (v7+, 0 for new session) */
	sessionId?: number
	/** Fetch session epoch (v7+, -1 for initial fetch) */
	sessionEpoch?: number
	/** Topics to fetch */
	topics: FetchRequestTopic[]
	/** Forgotten topics for incremental fetch (v7+) */
	forgottenTopicsData?: ForgottenTopic[]
	/** Rack ID for rack-aware fetch (v11+) */
	rackId?: string
}

/**
 * Supported API versions for Fetch request
 *
 * - v0-v11: non-flexible encoding
 * - v12+: flexible encoding with compact strings and tagged fields
 */
export const FETCH_VERSIONS = {
	min: 11,
	max: 11,
}

/**
 * Isolation level constants
 */
export const ISOLATION_LEVEL = {
	READ_UNCOMMITTED: 0,
	READ_COMMITTED: 1,
} as const

/** Null UUID constant */
const NULL_UUID = '00000000-0000-0000-0000-000000000000'

/**
 * Encode a Fetch request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeFetchRequest(encoder: IEncoder, version: number, request: FetchRequest): void {
	if (version < FETCH_VERSIONS.min || version > FETCH_VERSIONS.max) {
		throw new Error(`Unsupported Fetch version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.Fetch, version)

	if (flexible) {
		encodeFetchRequestFlexible(encoder, version, request)
	} else {
		encodeFetchRequestNonFlexible(encoder, version, request)
	}
}

/**
 * Encode a Fetch request using non-flexible encoding (v4-v11)
 */
function encodeFetchRequestNonFlexible(encoder: IEncoder, version: number, request: FetchRequest): void {
	// Replica ID
	encoder.writeInt32(request.replicaId ?? -1)

	// Max wait time
	encoder.writeInt32(request.maxWaitMs)

	// Min bytes
	encoder.writeInt32(request.minBytes)

	// Max bytes (v3+)
	if (version >= 3) {
		encoder.writeInt32(request.maxBytes ?? 52428800) // 50 MB default
	}

	// Isolation level (v4+)
	if (version >= 4) {
		encoder.writeInt8(request.isolationLevel ?? ISOLATION_LEVEL.READ_UNCOMMITTED)
	}

	// Session ID (v7+)
	if (version >= 7) {
		encoder.writeInt32(request.sessionId ?? 0)
	}

	// Session epoch (v7+)
	if (version >= 7) {
		encoder.writeInt32(request.sessionEpoch ?? -1)
	}

	// Topics (INT32 array)
	encoder.writeInt32(request.topics.length)
	for (const topic of request.topics) {
		encoder.writeString(topic.topic)

		// Partitions (INT32 array)
		encoder.writeInt32(topic.partitions.length)
		for (const partition of topic.partitions) {
			encoder.writeInt32(partition.partitionIndex)

			// Current leader epoch (v9+)
			if (version >= 9) {
				encoder.writeInt32(partition.currentLeaderEpoch ?? -1)
			}

			encoder.writeInt64(partition.fetchOffset)

			// Log start offset (v5+)
			if (version >= 5) {
				encoder.writeInt64(partition.logStartOffset ?? -1n)
			}

			encoder.writeInt32(partition.partitionMaxBytes)
		}
	}

	// Forgotten topics (v7+)
	if (version >= 7) {
		const forgottenTopics = request.forgottenTopicsData ?? []
		encoder.writeInt32(forgottenTopics.length)
		for (const topic of forgottenTopics) {
			encoder.writeString(topic.topic)
			encoder.writeInt32(topic.partitions.length)
			for (const p of topic.partitions) {
				encoder.writeInt32(p)
			}
		}
	}

	// Rack ID (v11+)
	if (version >= 11) {
		encoder.writeString(request.rackId ?? '')
	}
}

/**
 * Encode a Fetch request using flexible encoding (v12+)
 */
function encodeFetchRequestFlexible(encoder: IEncoder, version: number, request: FetchRequest): void {
	// Cluster ID (v12+, tagged field in some versions)
	if (version >= 12 && version < 15) {
		// Pre-v15: clusterId is in request body
		encoder.writeCompactNullableString(request.clusterId ?? null)
	}

	// Replica ID
	encoder.writeInt32(request.replicaId ?? -1)

	// Max wait time
	encoder.writeInt32(request.maxWaitMs)

	// Min bytes
	encoder.writeInt32(request.minBytes)

	// Max bytes (v3+)
	encoder.writeInt32(request.maxBytes ?? 52428800) // 50 MB default

	// Isolation level (v4+)
	encoder.writeInt8(request.isolationLevel ?? ISOLATION_LEVEL.READ_UNCOMMITTED)

	// Session ID (v7+)
	encoder.writeInt32(request.sessionId ?? 0)

	// Session epoch (v7+)
	encoder.writeInt32(request.sessionEpoch ?? -1)

	// Topics (compact array)
	encoder.writeCompactArray(request.topics, (topic, enc) => {
		// Topic ID (v13+)
		if (version >= 13) {
			enc.writeUUID(topic.topicId ?? NULL_UUID)
		}

		enc.writeCompactString(topic.topic)

		// Partitions (compact array)
		enc.writeCompactArray(topic.partitions, (partition, penc) => {
			penc.writeInt32(partition.partitionIndex)
			penc.writeInt32(partition.currentLeaderEpoch ?? -1)
			penc.writeInt64(partition.fetchOffset)
			penc.writeInt32(partition.lastFetchedEpoch ?? -1)
			penc.writeInt64(partition.logStartOffset ?? -1n)
			penc.writeInt32(partition.partitionMaxBytes)
			penc.writeEmptyTaggedFields()
		})

		enc.writeEmptyTaggedFields()
	})

	// Forgotten topics (v7+, compact array)
	encoder.writeCompactArray(request.forgottenTopicsData ?? [], (topic, enc) => {
		if (version >= 13) {
			enc.writeUUID(topic.topicId ?? NULL_UUID)
		}
		enc.writeCompactString(topic.topic)
		enc.writeCompactArray(topic.partitions, (p, penc) => {
			penc.writeInt32(p)
		})
		enc.writeEmptyTaggedFields()
	})

	// Rack ID (v11+)
	encoder.writeCompactString(request.rackId ?? '')

	// Tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Default fetch request configuration
 */
export const FETCH_DEFAULTS = {
	maxWaitMs: 500,
	minBytes: 1,
	maxBytes: 52428800, // 50 MB
	isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
} as const
