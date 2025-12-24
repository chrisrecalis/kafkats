/**
 * Sticky partition assignor
 *
 * The sticky assignor aims to:
 * 1. Preserve existing partition assignments when possible (stickiness)
 * 2. Balance partition count across consumers
 *
 * It reads current ownership from member.metadata.ownedPartitions for
 * cross-language compatibility with other Kafka clients.
 *
 * This is an eager protocol assignor - during rebalance, all partitions
 * are revoked before reassignment.
 */

import type { PartitionAssignor, MemberSubscription, RebalanceProtocol } from './types.js'
import type { MemberAssignment, TopicPartitionList } from './consumer-protocol.js'
import { DEFAULT_PROTOCOL_VERSION } from './consumer-protocol.js'
import { tpKey, parseKey, type TopicPartition } from '@/utils/topic-partition.js'

/**
 * Sticky partition assignor implementation
 */
export class StickyAssignor implements PartitionAssignor {
	readonly name = 'sticky'
	readonly protocolType: RebalanceProtocol = 'eager'

	assign(members: MemberSubscription[], topicPartitions: Map<string, number[]>): Map<string, MemberAssignment> {
		// Build current ownership from member metadata (cross-language compatible)
		const currentOwnership = new Map<string, Set<string>>() // memberId -> Set of tp keys
		for (const member of members) {
			const owned = new Set<string>()
			if (member.metadata.ownedPartitions) {
				for (const tp of member.metadata.ownedPartitions) {
					for (const partition of tp.partitions) {
						owned.add(tpKey(tp.topic, partition))
					}
				}
			}
			currentOwnership.set(member.memberId, owned)
		}

		// Get all topics that any member is subscribed to
		const allTopics = new Set<string>()
		for (const member of members) {
			for (const topic of member.metadata.topics) {
				allTopics.add(topic)
			}
		}

		// Build list of all partitions to assign
		const allPartitions: TopicPartition[] = []
		for (const topic of allTopics) {
			const partitions = topicPartitions.get(topic)
			if (partitions) {
				for (const partition of partitions) {
					allPartitions.push({ topic, partition })
				}
			}
		}

		// Build member subscriptions map
		const memberSubscriptions = new Map<string, Set<string>>()
		for (const member of members) {
			memberSubscriptions.set(member.memberId, new Set(member.metadata.topics))
		}

		// Initialize assignments
		const assignments = new Map<string, Set<string>>() // memberId -> Set of tp keys
		for (const member of members) {
			assignments.set(member.memberId, new Set())
		}

		// Track unassigned partitions
		const unassigned = new Set<string>()
		for (const tp of allPartitions) {
			unassigned.add(tpKey(tp.topic, tp.partition))
		}

		// Phase 1: Keep sticky assignments where possible
		// Only keep if member is still subscribed to the topic
		for (const member of members) {
			const memberTopics = memberSubscriptions.get(member.memberId)!
			const owned = currentOwnership.get(member.memberId)!
			const memberAssignment = assignments.get(member.memberId)!

			for (const key of owned) {
				const { topic } = parseKey(key)
				// Only keep if:
				// 1. Member is still subscribed to this topic
				// 2. Partition still exists (is in unassigned)
				if (memberTopics.has(topic) && unassigned.has(key)) {
					memberAssignment.add(key)
					unassigned.delete(key)
				}
			}
		}

		// Phase 2: Assign remaining partitions using round-robin to balance
		// Sort members by current assignment count (ascending) for better balance
		const sortedMembers = [...members].sort((a, b) => {
			const aCount = assignments.get(a.memberId)!.size
			const bCount = assignments.get(b.memberId)!.size
			if (aCount !== bCount) return aCount - bCount
			// Tie-break by member ID for determinism
			return a.memberId.localeCompare(b.memberId)
		})

		// Sort unassigned partitions for determinism
		const unassignedList = [...unassigned].sort()

		for (const key of unassignedList) {
			const { topic } = parseKey(key)

			// Find member with fewest partitions that is subscribed to this topic
			let bestMember: string | null = null
			let bestCount = Infinity

			for (const member of sortedMembers) {
				const memberTopics = memberSubscriptions.get(member.memberId)!
				if (!memberTopics.has(topic)) continue

				const count = assignments.get(member.memberId)!.size
				if (count < bestCount) {
					bestCount = count
					bestMember = member.memberId
				}
			}

			if (bestMember) {
				assignments.get(bestMember)!.add(key)
			}
		}

		// Convert to MemberAssignment format
		const result = new Map<string, MemberAssignment>()
		for (const [memberId, assignedKeys] of assignments) {
			// Group by topic
			const topicMap = new Map<string, number[]>()
			for (const key of assignedKeys) {
				const { topic, partition } = parseKey(key)
				if (!topicMap.has(topic)) {
					topicMap.set(topic, [])
				}
				topicMap.get(topic)!.push(partition)
			}

			// Sort partitions within each topic for determinism
			const partitions: TopicPartitionList[] = []
			for (const [topic, partitionList] of topicMap) {
				partitions.push({ topic, partitions: partitionList.sort((a, b) => a - b) })
			}
			// Sort topics for determinism
			partitions.sort((a, b) => a.topic.localeCompare(b.topic))

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
 * Default sticky assignor instance
 */
export const stickyAssignor = new StickyAssignor()
