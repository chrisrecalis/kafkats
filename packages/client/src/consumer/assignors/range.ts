/**
 * Range partition assignor
 *
 * The range assignor works on a per-topic basis. For each topic, it:
 * 1. Sorts consumers by member ID
 * 2. Divides partitions contiguously among consumers
 * 3. If partitions don't divide evenly, earlier consumers get one extra
 *
 * Example: topic with 7 partitions, 3 consumers
 * - Consumer 0: partitions 0, 1, 2 (3 partitions)
 * - Consumer 1: partitions 3, 4 (2 partitions)
 * - Consumer 2: partitions 5, 6 (2 partitions)
 */

import type { PartitionAssignor, MemberSubscription, RebalanceProtocol } from './types.js'
import type { MemberAssignment, TopicPartitionList } from './consumer-protocol.js'
import { DEFAULT_PROTOCOL_VERSION } from './consumer-protocol.js'

/**
 * Range partition assignor implementation
 */
export class RangeAssignor implements PartitionAssignor {
	readonly name = 'range'
	readonly protocolType: RebalanceProtocol = 'eager'

	assign(members: MemberSubscription[], topicPartitions: Map<string, number[]>): Map<string, MemberAssignment> {
		// Initialize assignments for each member
		const assignments = new Map<string, Map<string, number[]>>()
		for (const member of members) {
			assignments.set(member.memberId, new Map())
		}

		// Get all topics that any member is subscribed to
		const allTopics = new Set<string>()
		for (const member of members) {
			for (const topic of member.metadata.topics) {
				allTopics.add(topic)
			}
		}

		// Assign each topic independently
		for (const topic of allTopics) {
			const partitions = topicPartitions.get(topic)
			if (!partitions || partitions.length === 0) {
				continue
			}

			// Get members subscribed to this topic, sorted by member ID
			const subscribedMembers = members
				.filter(m => m.metadata.topics.includes(topic))
				.sort((a, b) => a.memberId.localeCompare(b.memberId))

			if (subscribedMembers.length === 0) {
				continue
			}

			// Sort partitions for deterministic assignment
			const sortedPartitions = [...partitions].sort((a, b) => a - b)

			// Calculate partitions per consumer
			const numPartitions = sortedPartitions.length
			const numConsumers = subscribedMembers.length
			const partitionsPerConsumer = Math.floor(numPartitions / numConsumers)
			const extraPartitions = numPartitions % numConsumers

			// Assign partitions to each consumer
			let partitionIndex = 0
			for (let i = 0; i < subscribedMembers.length; i++) {
				const member = subscribedMembers[i]!
				const memberAssignment = assignments.get(member.memberId)!

				// Calculate how many partitions this consumer gets
				// First `extraPartitions` consumers get one extra partition
				const partitionCount = partitionsPerConsumer + (i < extraPartitions ? 1 : 0)

				// Assign contiguous range of partitions
				const memberPartitions: number[] = []
				for (let j = 0; j < partitionCount; j++) {
					memberPartitions.push(sortedPartitions[partitionIndex]!)
					partitionIndex++
				}

				if (memberPartitions.length > 0) {
					memberAssignment.set(topic, memberPartitions)
				}
			}
		}

		// Convert to MemberAssignment format
		const result = new Map<string, MemberAssignment>()
		for (const [memberId, topicAssignments] of assignments) {
			const partitions: TopicPartitionList[] = []
			for (const [topic, partitionList] of topicAssignments) {
				partitions.push({ topic, partitions: partitionList })
			}

			result.set(memberId, {
				version: DEFAULT_PROTOCOL_VERSION,
				partitions,
				userData: null,
			})
		}

		return result
	}
}

/**
 * Default range assignor instance
 */
export const rangeAssignor = new RangeAssignor()
