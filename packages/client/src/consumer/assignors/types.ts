/**
 * Partition assignor types and interfaces
 */

import type { SubscriptionMetadata, MemberAssignment } from './consumer-protocol.js'

/**
 * Member subscription information from JoinGroup response
 */
export interface MemberSubscription {
	/** Member ID assigned by the broker */
	memberId: string
	/** Optional group instance ID for static membership */
	groupInstanceId?: string
	/** Decoded subscription metadata */
	metadata: SubscriptionMetadata
}

/**
 * Rebalance protocol type
 * - 'eager': Stop-the-world rebalancing (all partitions revoked before reassignment)
 * - 'cooperative': Incremental rebalancing (only revoked partitions stop fetching)
 */
export type RebalanceProtocol = 'eager' | 'cooperative'

/**
 * Partition assignor interface
 *
 * Assignors are responsible for distributing partitions among consumers
 * in a consumer group. The group leader uses the assignor to compute
 * the assignment, then sends it via SyncGroup.
 */
export interface PartitionAssignor {
	/** Unique name identifying this assignor (e.g., 'range', 'roundrobin', 'cooperative-sticky') */
	readonly name: string

	/**
	 * Rebalance protocol type:
	 * - 'eager': Stop-the-world rebalancing (range, roundrobin, sticky)
	 * - 'cooperative': Incremental rebalancing (cooperative-sticky)
	 */
	readonly protocolType: RebalanceProtocol

	/**
	 * Compute partition assignments for all members
	 *
	 * @param members - List of group members with their subscriptions
	 * @param topicPartitions - Map of topic to partition indices
	 * @returns Map of memberId to their assignment
	 */
	assign(members: MemberSubscription[], topicPartitions: Map<string, number[]>): Map<string, MemberAssignment>
}

/**
 * Topic partition info for assignment
 */
export interface TopicPartitionInfo {
	topic: string
	partitions: number[]
}

/**
 * Group metadata passed to assignor
 */
export interface GroupMetadata {
	groupId: string
	generationId: number
	leaderId: string
	members: MemberSubscription[]
}
