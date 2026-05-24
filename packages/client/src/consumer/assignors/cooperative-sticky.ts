/**
 * Cooperative-Sticky partition assignor
 *
 * This assignor uses the cooperative rebalance protocol for incremental
 * partition reassignment. Key difference from eager sticky assignor:
 *
 * With cooperative rebalancing:
 * - Partitions that need to move are WITHHELD (not assigned) in phase 1
 * - Current owner sees their partition was revoked and releases it
 * - In phase 2, after revocation, the partition is assigned to new owner
 * - Partitions that stay with same owner continue without interruption
 *
 * This is Kafka's default assignor since version 2.4.
 */

import type { PartitionAssignor, MemberSubscription, RebalanceProtocol } from './types.js'
import type { MemberAssignment, TopicPartitionList } from './consumer-protocol.js'
import { DEFAULT_PROTOCOL_VERSION } from './consumer-protocol.js'
import { tpKey, parseKey, type TopicPartition } from '@/utils/topic-partition.js'

/**
 * Cooperative-Sticky partition assignor implementation
 *
 * The cooperative protocol works in two phases:
 * 1. Phase 1: Compute target assignment, but WITHHOLD partitions that would move
 *    - Keep partitions with current owners
 *    - Assign truly unowned partitions (new/no current owner)
 *    - DON'T assign partitions that are owned but need to move to a different member
 * 2. Phase 2: After revocation, the withheld partitions become unowned and can be assigned
 */
export class CooperativeStickyAssignor implements PartitionAssignor {
	readonly name = 'cooperative-sticky'
	readonly protocolType: RebalanceProtocol = 'cooperative'

	assign(members: MemberSubscription[], topicPartitions: Map<string, number[]>): Map<string, MemberAssignment> {
		// Build current ownership from member metadata (cross-language compatible)
		// This is what each member claims to currently own
		const currentOwnership = new Map<string, Set<string>>() // memberId -> Set of tp keys
		const ownerOf = new Map<string, string>() // tp key -> memberId (reverse lookup)

		for (const member of members) {
			const owned = new Set<string>()
			if (member.metadata.ownedPartitions) {
				for (const tp of member.metadata.ownedPartitions) {
					for (const partition of tp.partitions) {
						const key = tpKey(tp.topic, partition)
						owned.add(key)
						ownerOf.set(key, member.memberId)
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

		const targetAssignment = this.computeTargetAssignment(
			members,
			allPartitions,
			memberSubscriptions,
			currentOwnership
		)

		// Partitions that need to move are withheld - not assigned until current owner releases
		const inTransit = new Set<string>()
		for (const [key, currentOwner] of ownerOf) {
			// Find target owner
			let targetOwner: string | null = null
			for (const [memberId, partitions] of targetAssignment) {
				if (partitions.has(key)) {
					targetOwner = memberId
					break
				}
			}

			// If partition is owned by someone and target is a DIFFERENT member,
			// withhold it (don't assign to anyone in phase 1)
			if (targetOwner !== null && targetOwner !== currentOwner) {
				inTransit.add(key)
			}
		}

		const finalAssignment = new Map<string, Set<string>>()
		for (const member of members) {
			finalAssignment.set(member.memberId, new Set())
		}

		for (const [memberId, targetPartitions] of targetAssignment) {
			const memberAssignment = finalAssignment.get(memberId)!

			for (const key of targetPartitions) {
				// Skip if this partition is in-transit (withheld - not assigned to anyone)
				if (inTransit.has(key)) {
					continue
				}

				// Assign the partition
				memberAssignment.add(key)
			}
		}

		// Convert to MemberAssignment format
		// For cooperative protocol, use version 1 to include ownedPartitions support
		const result = new Map<string, MemberAssignment>()
		for (const [memberId, assignedKeys] of finalAssignment) {
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

	/**
	 * Compute the target (ideal) assignment using sticky + balanced logic
	 */
	private computeTargetAssignment(
		members: MemberSubscription[],
		allPartitions: TopicPartition[],
		memberSubscriptions: Map<string, Set<string>>,
		currentOwnership: Map<string, Set<string>>
	): Map<string, Set<string>> {
		// Initialize assignments
		const assignments = new Map<string, Set<string>>()
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
		// Sort unassigned partitions for determinism
		const unassignedList = [...unassigned].sort()

		for (const key of unassignedList) {
			const { topic } = parseKey(key)

			// Find member with fewest partitions that is subscribed to this topic
			let bestMember: string | null = null
			let bestCount = Infinity

			for (const member of members) {
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

		// Phase 3: Rebalance toward an even distribution. The sticky phase keeps every owned
		// partition, so without this a member that owns all partitions would starve a
		// newly-joined member.
		//
		// Repeatedly move one partition from the most-loaded member to a least-loaded member
		// that is subscribed to the partition's topic, as long as their counts differ by >= 2.
		// Each move strictly decreases the sum of squared counts, so the loop terminates, and it
		// converges to max-min <= 1 when subscriptions are uniform. A fixed idealCount/extra cap
		// (the previous approach) wrongly let every member rise to idealCount+1, leaving e.g.
		// {2,2,0} for 4 partitions / 3 members — a stable point that starves the third member.
		//
		// This is a greedy, direct-move balancer: it does not perform relay/transitive moves
		// across heterogeneous subscriptions, matching Kafka's relaxed sticky guarantee (KIP-54).
		let rebalanced = true
		while (rebalanced) {
			rebalanced = false

			const byCount = [...members].sort((a, b) => {
				const diff = assignments.get(b.memberId)!.size - assignments.get(a.memberId)!.size
				if (diff !== 0) return diff
				return a.memberId.localeCompare(b.memberId)
			})

			for (const donor of byCount) {
				const donorAssignment = assignments.get(donor.memberId)!

				// Recipients from least-loaded upward.
				for (const recipient of [...byCount].reverse()) {
					if (recipient.memberId === donor.memberId) continue

					const recipientAssignment = assignments.get(recipient.memberId)!
					// Only move when it improves balance (gap of >= 2).
					if (donorAssignment.size - recipientAssignment.size < 2) continue

					const recipientTopics = memberSubscriptions.get(recipient.memberId)!
					for (const key of donorAssignment) {
						const { topic } = parseKey(key)
						if (!recipientTopics.has(topic)) continue
						donorAssignment.delete(key)
						recipientAssignment.add(key)
						rebalanced = true
						break
					}

					if (rebalanced) break
				}

				if (rebalanced) break
			}
		}

		return assignments
	}
}

/**
 * Default cooperative-sticky assignor instance
 */
export const cooperativeStickyAssignor = new CooperativeStickyAssignor()
