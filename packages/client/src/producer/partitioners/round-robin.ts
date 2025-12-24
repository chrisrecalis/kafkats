/**
 * Round-robin partitioner
 *
 * Distributes messages evenly across partitions regardless of key.
 */

import type { PartitionerFunction } from '../types.js'

/**
 * Create a round-robin partitioner
 *
 * The partitioner maintains per-topic counters to distribute messages
 * evenly across all partitions. Unlike murmur2, this ignores the key
 * and focuses purely on load balancing.
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
	const counters = new Map<string, number>()

	return (topic: string, _key: Buffer | null, _value: Buffer, partitionCount: number): number => {
		const current = counters.get(topic) ?? 0
		const partition = current % partitionCount
		counters.set(topic, current + 1)
		return partition
	}
}
