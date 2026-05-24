/**
 * Round-robin partitioner
 *
 * Distributes messages evenly across partitions regardless of key.
 */

import type { PartitionerFunction } from '../types.js'

/**
 * Compute the next partition in a round-robin sequence given the previously used one
 * (or -1 to start). The result is always in `[0, partitionCount)`.
 *
 * The partitioner tracks the last partition it returned rather than an ever-incrementing
 * counter: an unbounded counter would eventually exceed Number.MAX_SAFE_INTEGER, after which
 * `counter + 1 === counter` and every subsequent message would be pinned to one partition.
 * Storing the bounded partition index avoids that entirely.
 */
export function nextRoundRobinPartition(previousPartition: number, partitionCount: number): number {
	return (previousPartition + 1) % partitionCount
}

/**
 * Create a round-robin partitioner
 *
 * The partitioner maintains per-topic state to distribute messages evenly across all
 * partitions. Unlike murmur2, this ignores the key and focuses purely on load balancing.
 *
 * @returns A PartitionerFunction that distributes messages round-robin
 *
 * @example
 * ```typescript
 * const producer = client.producer({
 *   partitioner: 'round-robin',
 * })
 * ```
 */
export function createRoundRobinPartitioner(): PartitionerFunction {
	const lastPartitionByTopic = new Map<string, number>()

	return (topic: string, _key: Buffer | null, _value: Buffer, partitionCount: number): number => {
		const previous = lastPartitionByTopic.get(topic) ?? -1
		const partition = nextRoundRobinPartition(previous, partitionCount)
		lastPartitionByTopic.set(topic, partition)
		return partition
	}
}
