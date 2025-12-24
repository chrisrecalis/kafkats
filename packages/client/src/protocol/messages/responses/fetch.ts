/**
 * Fetch Response (API Key 1)
 *
 * Returns fetched records from Kafka brokers.
 */

import { Decoder } from '@/protocol/primitives/index.js'
import type { IDecoder } from '@/protocol/primitives/index.js'
import { ApiKey, isFlexibleVersion } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { decodeRecordBatchFrom, type DecodedRecordBatch } from '@/protocol/records/index.js'

/**
 * Aborted transaction info
 */
export interface AbortedTransaction {
	producerId: bigint
	firstOffset: bigint
}

/**
 * Partition response data
 */
export interface FetchPartitionResponse {
	partitionIndex: number
	errorCode: ErrorCode
	highWatermark: bigint
	lastStableOffset: bigint
	logStartOffset: bigint
	divergingEpoch?: {
		epoch: number
		endOffset: bigint
	}
	currentLeader?: {
		leaderId: number
		leaderEpoch: number
	}
	snapshotId?: {
		endOffset: bigint
		epoch: number
	}
	abortedTransactions: AbortedTransaction[]
	preferredReadReplica: number
	/** Raw records data (use decodeRecordBatches to decode) */
	recordsData: Buffer | null
	/** Decoded record batches (populated after calling decodeRecordBatches) */
	recordBatches?: DecodedRecordBatch[]
}

/**
 * Topic response data
 */
export interface FetchTopicResponse {
	topicId: string
	topic: string
	partitions: FetchPartitionResponse[]
}

/**
 * Fetch response data
 */
export interface FetchResponse {
	/** Throttle time in milliseconds */
	throttleTimeMs: number
	/** Error code (v7+) */
	errorCode: ErrorCode
	/** Session ID (v7+) */
	sessionId: number
	/** Topic responses */
	topics: FetchTopicResponse[]
}

/** Null UUID constant */
const NULL_UUID = '00000000-0000-0000-0000-000000000000'

/**
 * Decode a Fetch response
 *
 * @param decoder - The decoder to read from
 * @param version - The API version that was requested
 * @returns The decoded response
 */
export function decodeFetchResponse(decoder: IDecoder, version: number): FetchResponse {
	const flexible = isFlexibleVersion(ApiKey.Fetch, version)

	if (flexible) {
		return decodeFetchResponseFlexible(decoder, version)
	} else {
		return decodeFetchResponseNonFlexible(decoder, version)
	}
}

/**
 * Decode a Fetch response using non-flexible encoding (v4-v11)
 */
function decodeFetchResponseNonFlexible(decoder: IDecoder, version: number): FetchResponse {
	// Throttle time (v1+)
	const throttleTimeMs = version >= 1 ? decoder.readInt32() : 0

	// Error code (v7+)
	const errorCode = version >= 7 ? (decoder.readInt16() as ErrorCode) : ErrorCode.None

	// Session ID (v7+)
	const sessionId = version >= 7 ? decoder.readInt32() : 0

	// Topics (INT32 array)
	const topicCount = decoder.readInt32()
	const topics: FetchTopicResponse[] = []

	for (let i = 0; i < topicCount; i++) {
		const topic = decoder.readString()

		// Partitions (INT32 array)
		const partitionCount = decoder.readInt32()
		const partitions: FetchPartitionResponse[] = []

		for (let j = 0; j < partitionCount; j++) {
			const partitionIndex = decoder.readInt32()
			const partitionErrorCode = decoder.readInt16() as ErrorCode
			const highWatermark = decoder.readInt64()

			// Last stable offset (v4+)
			const lastStableOffset = version >= 4 ? decoder.readInt64() : highWatermark

			// Log start offset (v5+)
			const logStartOffset = version >= 5 ? decoder.readInt64() : -1n

			// Aborted transactions (v4+)
			const abortedTransactions: AbortedTransaction[] = []
			if (version >= 4) {
				const abortedCount = decoder.readInt32()
				if (abortedCount >= 0) {
					for (let k = 0; k < abortedCount; k++) {
						const producerId = decoder.readInt64()
						const firstOffset = decoder.readInt64()
						abortedTransactions.push({ producerId, firstOffset })
					}
				}
			}

			// Preferred read replica (v11+)
			const preferredReadReplica = version >= 11 ? decoder.readInt32() : -1

			// Records (INT32 length prefix + bytes)
			const recordsData = decoder.readNullableBytes()

			partitions.push({
				partitionIndex,
				errorCode: partitionErrorCode,
				highWatermark,
				lastStableOffset,
				logStartOffset,
				abortedTransactions,
				preferredReadReplica,
				recordsData,
			})
		}

		topics.push({ topicId: NULL_UUID, topic, partitions })
	}

	return { throttleTimeMs, errorCode, sessionId, topics }
}

/**
 * Decode a Fetch response using flexible encoding (v12+)
 */
function decodeFetchResponseFlexible(decoder: IDecoder, version: number): FetchResponse {
	// Throttle time
	const throttleTimeMs = decoder.readInt32()

	// Error code (v7+)
	const errorCode = decoder.readInt16() as ErrorCode

	// Session ID (v7+)
	const sessionId = decoder.readInt32()

	// Topics (compact array)
	const topics = decoder.readCompactArray(d => {
		// Topic ID (v13+)
		let topicId = NULL_UUID
		if (version >= 13) {
			topicId = d.readUUID()
		}

		const topic = d.readCompactString()

		// Partitions (compact array)
		const partitions = d.readCompactArray(pd => {
			const partitionIndex = pd.readInt32()
			const partitionErrorCode = pd.readInt16() as ErrorCode
			const highWatermark = pd.readInt64()
			const lastStableOffset = pd.readInt64()
			const logStartOffset = pd.readInt64()

			// Diverging epoch (v12+, from tagged fields typically)
			let divergingEpoch: { epoch: number; endOffset: bigint } | undefined

			// Current leader (v12+, from tagged fields typically)
			let currentLeader: { leaderId: number; leaderEpoch: number } | undefined

			// Snapshot ID (v12+, from tagged fields typically)
			let snapshotId: { endOffset: bigint; epoch: number } | undefined

			// Aborted transactions (v4+)
			const abortedTransactions =
				pd.readCompactNullableArray(ad => {
					const producerId = ad.readInt64()
					const firstOffset = ad.readInt64()
					ad.skipTaggedFields()
					return { producerId, firstOffset }
				}) ?? []

			// Preferred read replica (v11+)
			const preferredReadReplica = pd.readInt32()

			// Records (compact bytes)
			const recordsData = pd.readCompactNullableBytes()

			pd.skipTaggedFields()

			return {
				partitionIndex,
				errorCode: partitionErrorCode,
				highWatermark,
				lastStableOffset,
				logStartOffset,
				divergingEpoch,
				currentLeader,
				snapshotId,
				abortedTransactions,
				preferredReadReplica,
				recordsData,
			}
		})

		d.skipTaggedFields()

		return { topicId, topic, partitions }
	})

	// Tagged fields
	decoder.skipTaggedFields()

	return { throttleTimeMs, errorCode, sessionId, topics }
}

/**
 * Decode record batches from a partition response
 *
 * @param partition - The partition response containing raw records
 * @returns The decoded record batches
 */
export async function decodePartitionRecordBatches(partition: FetchPartitionResponse): Promise<DecodedRecordBatch[]> {
	if (!partition.recordsData || partition.recordsData.length === 0) {
		return []
	}

	const batches: DecodedRecordBatch[] = []
	const decoder = new Decoder(partition.recordsData)

	while (decoder.remaining() > 0) {
		// Check if we have enough data for a batch header
		if (decoder.remaining() < 12) {
			break // Not enough data for baseOffset + batchLength
		}

		try {
			const batch = await decodeRecordBatchFrom(decoder)
			batches.push(batch)
		} catch {
			// If we fail to decode, we've probably hit the end of complete batches
			break
		}
	}

	return batches
}

/**
 * Decode all record batches in a fetch response
 *
 * @param response - The fetch response
 * @returns The response with recordBatches populated
 */
export async function decodeAllRecordBatches(response: FetchResponse): Promise<FetchResponse> {
	for (const topic of response.topics) {
		for (const partition of topic.partitions) {
			partition.recordBatches = await decodePartitionRecordBatches(partition)
		}
	}
	return response
}

/**
 * Check if a fetch response has any errors
 *
 * @param response - The fetch response
 * @returns true if any error occurred
 */
export function hasFetchErrors(response: FetchResponse): boolean {
	if (response.errorCode !== ErrorCode.None) {
		return true
	}
	return response.topics.some(topic => topic.partitions.some(partition => partition.errorCode !== ErrorCode.None))
}
