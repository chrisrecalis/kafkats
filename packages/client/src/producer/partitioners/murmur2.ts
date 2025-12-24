/**
 * Murmur2 partitioner - Java Kafka client compatible
 *
 * This implementation matches the Kafka Java client's DefaultPartitioner
 * which uses murmur2 with seed 0x9747b28c.
 */

import type { PartitionerFunction } from '../types.js'

/**
 * Java-compatible murmur2 hash function
 *
 * Produces the same hash values as the Kafka Java client's
 * org.apache.kafka.common.utils.Utils.murmur2 implementation.
 *
 * @param data - Buffer to hash
 * @returns 32-bit signed hash value
 */
export function murmur2(data: Buffer): number {
	const seed = 0x9747b28c
	const m = 0x5bd1e995
	const r = 24

	let h = seed ^ data.length
	let length = data.length
	let offset = 0

	while (length >= 4) {
		let k =
			(data[offset]! & 0xff) |
			((data[offset + 1]! & 0xff) << 8) |
			((data[offset + 2]! & 0xff) << 16) |
			((data[offset + 3]! & 0xff) << 24)

		k = Math.imul(k, m)
		k ^= k >>> r
		k = Math.imul(k, m)

		h = Math.imul(h, m)
		h ^= k

		offset += 4
		length -= 4
	}

	switch (length) {
		case 3:
			h ^= (data[offset + 2]! & 0xff) << 16
		// falls through
		case 2:
			h ^= (data[offset + 1]! & 0xff) << 8
		// falls through
		case 1:
			h ^= data[offset]! & 0xff
			h = Math.imul(h, m)
	}

	h ^= h >>> 13
	h = Math.imul(h, m)
	h ^= h >>> 15

	return h
}

/**
 * Convert a signed 32-bit integer to unsigned for positive modulo
 */
function toPositive(n: number): number {
	return n >>> 0
}

/**
 * Murmur2 partitioner function
 *
 * When a key is present, uses murmur2 hash to determine partition.
 * When key is null, returns -1 to indicate the caller should use
 * sticky partitioning (assign to a random partition and stick to it).
 *
 * This matches the Java client's DefaultPartitioner behavior.
 *
 * @param topic - Topic name (unused in this partitioner)
 * @param key - Message key (null if not provided)
 * @param value - Encoded message value (unused in this partitioner)
 * @param partitionCount - Number of partitions for the topic
 * @returns Partition index, or -1 for sticky partitioning
 */
export const murmur2Partitioner: PartitionerFunction = (
	_topic: string,
	key: Buffer | null,
	_value: Buffer,
	partitionCount: number
): number => {
	if (key === null) {
		return -1
	}

	const hash = murmur2(key)
	return toPositive(hash) % partitionCount
}
