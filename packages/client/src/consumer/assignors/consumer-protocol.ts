/**
 * Consumer group protocol encoding/decoding
 *
 * The Kafka consumer protocol uses opaque byte buffers for subscription
 * metadata (in JoinGroup) and partition assignments (in SyncGroup).
 * This module provides encoding and decoding for these buffers.
 *
 * Format follows the Kafka consumer protocol specification:
 * - Subscription metadata: version (int16), topics (array of strings), userData (nullable bytes)
 * - Assignment: version (int16), topic assignments (array), userData (nullable bytes)
 */

import { Encoder } from '@/protocol/primitives/encoder.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

// ==================== Subscription Metadata ====================

/**
 * Subscription metadata sent in JoinGroup request
 */
export interface SubscriptionMetadata {
	/** Protocol version (0, 1, or 2) */
	version: number
	/** Subscribed topics */
	topics: string[]
	/** User-provided data (optional) */
	userData: Buffer | null
	/** Owned partitions for cooperative rebalancing (v1+) */
	ownedPartitions?: TopicPartitionList[]
}

/**
 * Topic with list of partitions
 */
export interface TopicPartitionList {
	topic: string
	partitions: number[]
}

/**
 * Encode subscription metadata to buffer
 */
export function encodeSubscriptionMetadata(metadata: SubscriptionMetadata): Buffer {
	const encoder = new Encoder()

	// Version (int16)
	encoder.writeInt16(metadata.version)

	// Topics array (int32 length + strings)
	encoder.writeInt32(metadata.topics.length)
	for (const topic of metadata.topics) {
		encoder.writeString(topic)
	}

	// User data (nullable bytes)
	encoder.writeNullableBytes(metadata.userData)

	// Owned partitions (v1+) - for cooperative rebalancing
	if (metadata.version >= 1 && metadata.ownedPartitions) {
		encoder.writeInt32(metadata.ownedPartitions.length)
		for (const tp of metadata.ownedPartitions) {
			encoder.writeString(tp.topic)
			encoder.writeInt32(tp.partitions.length)
			for (const partition of tp.partitions) {
				encoder.writeInt32(partition)
			}
		}
	}

	return encoder.toBuffer()
}

/**
 * Decode subscription metadata from buffer
 */
export function decodeSubscriptionMetadata(buffer: Buffer): SubscriptionMetadata {
	const decoder = new Decoder(buffer)

	// Version (int16)
	const version = decoder.readInt16()

	// Topics array
	const topicCount = decoder.readInt32()
	const topics: string[] = []
	for (let i = 0; i < topicCount; i++) {
		topics.push(decoder.readString())
	}

	// User data (nullable bytes)
	const userData = decoder.readNullableBytes()

	// Owned partitions (v1+)
	let ownedPartitions: TopicPartitionList[] | undefined
	if (version >= 1 && decoder.remaining() > 0) {
		const ownedCount = decoder.readInt32()
		ownedPartitions = []
		for (let i = 0; i < ownedCount; i++) {
			const topic = decoder.readString()
			const partitionCount = decoder.readInt32()
			const partitions: number[] = []
			for (let j = 0; j < partitionCount; j++) {
				partitions.push(decoder.readInt32())
			}
			ownedPartitions.push({ topic, partitions })
		}
	}

	return {
		version,
		topics,
		userData,
		ownedPartitions,
	}
}

// ==================== Member Assignment ====================

/**
 * Partition assignment for a member
 */
export interface MemberAssignment {
	/** Protocol version (0, 1, or 2) */
	version: number
	/** Assigned partitions by topic */
	partitions: TopicPartitionList[]
	/** User-provided data (optional) */
	userData: Buffer | null
}

/**
 * Encode member assignment to buffer
 */
export function encodeAssignment(assignment: MemberAssignment): Buffer {
	const encoder = new Encoder()

	// Version (int16)
	encoder.writeInt16(assignment.version)

	// Topic assignments array
	encoder.writeInt32(assignment.partitions.length)
	for (const tp of assignment.partitions) {
		encoder.writeString(tp.topic)
		encoder.writeInt32(tp.partitions.length)
		for (const partition of tp.partitions) {
			encoder.writeInt32(partition)
		}
	}

	// User data (nullable bytes)
	encoder.writeNullableBytes(assignment.userData)

	return encoder.toBuffer()
}

/**
 * Decode member assignment from buffer
 */
export function decodeAssignment(buffer: Buffer): MemberAssignment {
	// Handle empty buffer (no assignment)
	if (buffer.length === 0) {
		return {
			version: 0,
			partitions: [],
			userData: null,
		}
	}

	const decoder = new Decoder(buffer)

	// Version (int16)
	const version = decoder.readInt16()

	// Topic assignments array
	const topicCount = decoder.readInt32()
	const partitions: TopicPartitionList[] = []
	for (let i = 0; i < topicCount; i++) {
		const topic = decoder.readString()
		const partitionCount = decoder.readInt32()
		const partitionList: number[] = []
		for (let j = 0; j < partitionCount; j++) {
			partitionList.push(decoder.readInt32())
		}
		partitions.push({ topic, partitions: partitionList })
	}

	// User data (nullable bytes)
	const userData = decoder.readNullableBytes()

	return {
		version,
		partitions,
		userData,
	}
}

/**
 * Convert MemberAssignment to a flat list of TopicPartitions
 */
export function assignmentToTopicPartitions(assignment: MemberAssignment): Array<{ topic: string; partition: number }> {
	const result: Array<{ topic: string; partition: number }> = []
	for (const tp of assignment.partitions) {
		for (const partition of tp.partitions) {
			result.push({ topic: tp.topic, partition })
		}
	}
	return result
}

/**
 * Consumer protocol type identifier
 */
export const CONSUMER_PROTOCOL_TYPE = 'consumer'

/**
 * Default consumer protocol version
 */
export const DEFAULT_PROTOCOL_VERSION = 0
