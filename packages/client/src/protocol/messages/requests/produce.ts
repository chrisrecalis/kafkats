/**
 * Produce Request (API Key 0)
 *
 * Used to send records to Kafka brokers.
 *
 * Version 9+ uses flexible encoding (compact types + tagged fields).
 */

import type { IEncoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'

/**
 * Partition data for produce request
 */
export interface ProduceRequestPartition {
	/** Partition index */
	partitionIndex: number
	/** Pre-encoded record batch (already compressed if applicable) */
	records: Buffer
}

/**
 * Topic data for produce request
 */
export interface ProduceRequestTopic {
	/** Topic name */
	name: string
	/** Partition data */
	partitions: ProduceRequestPartition[]
}

/**
 * Produce request data
 */
export interface ProduceRequest {
	/** Transactional ID (null for non-transactional) */
	transactionalId?: string | null
	/** Required acknowledgments: 0=none, 1=leader, -1=all */
	acks: number
	/** Timeout in milliseconds */
	timeoutMs: number
	/** Topic data */
	topics: ProduceRequestTopic[]
}

/**
 * Supported API versions for Produce request
 *
 * - v0-v8: non-flexible encoding
 * - v9+: flexible encoding with compact strings and tagged fields
 */
export const PRODUCE_VERSIONS = {
	min: 9,
	max: 9,
}

/**
 * Acks constants
 */
export const PRODUCE_ACKS = {
	/** No acknowledgment required */
	NONE: 0,
	/** Only leader acknowledgment required */
	LEADER: 1,
	/** All replicas must acknowledge */
	ALL: -1,
} as const

/**
 * Encode a Produce request
 *
 * @param encoder - The encoder to write to
 * @param version - The API version to use
 * @param request - The request data
 */
export function encodeProduceRequest(encoder: IEncoder, version: number, request: ProduceRequest): void {
	if (version < PRODUCE_VERSIONS.min || version > PRODUCE_VERSIONS.max) {
		throw new Error(`Unsupported Produce version: ${version}`)
	}

	const flexible = isFlexibleVersion(ApiKey.Produce, version)

	if (!flexible) {
		throw new Error('Non-flexible Produce versions (< 9) not supported')
	}

	// Transactional ID (compact nullable string)
	encoder.writeCompactNullableString(request.transactionalId ?? null)

	// Acks
	encoder.writeInt16(request.acks)

	// Timeout
	encoder.writeInt32(request.timeoutMs)

	// Topic data (compact array)
	encoder.writeCompactArray(request.topics, (topic, enc) => {
		enc.writeCompactString(topic.name)

		// Partition data (compact array)
		enc.writeCompactArray(topic.partitions, (partition, penc) => {
			penc.writeInt32(partition.partitionIndex)
			// Records as compact bytes (UVARINT length + 1)
			penc.writeCompactBytes(partition.records)
			penc.writeEmptyTaggedFields()
		})

		enc.writeEmptyTaggedFields()
	})

	// Tagged fields
	encoder.writeEmptyTaggedFields()
}

/**
 * Default produce request configuration
 */
export const PRODUCE_DEFAULTS = {
	acks: PRODUCE_ACKS.ALL,
	timeoutMs: 30000,
} as const
