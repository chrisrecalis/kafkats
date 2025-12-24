/**
 * Consumer group coordination
 *
 * Handles the consumer group protocol:
 * - JoinGroup: Join or rejoin the group
 * - SyncGroup: Receive partition assignment
 * - Heartbeat: Maintain group membership
 * - LeaveGroup: Leave the group gracefully
 */

import { EventEmitter } from 'node:events'
import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError, CoordinatorNotAvailableError } from '@/client/errors.js'
import type {
	TopicPartition,
	ConsumerGroupEvents,
	ConsumerConfig,
	ResolvedConsumerConfig,
	PartitionAssignmentStrategy,
} from './types.js'
import { ConsumerGroupState, DEFAULT_CONSUMER_CONFIG } from './types.js'
import {
	encodeSubscriptionMetadata,
	decodeSubscriptionMetadata,
	encodeAssignment,
	decodeAssignment,
	assignmentToTopicPartitions,
	CONSUMER_PROTOCOL_TYPE,
} from './assignors/index.js'
import type { PartitionAssignor, MemberSubscription, RebalanceProtocol, TopicPartitionList } from './assignors/index.js'
import { rangeAssignor, stickyAssignor, cooperativeStickyAssignor } from './assignors/index.js'
import { noopLogger, type Logger } from '@/logger.js'
import { sleep } from '@/utils/sleep.js'

/**
 * Result of join operation with partition change info
 */
export interface JoinResult {
	/** Full assignment after join */
	assignment: TopicPartition[]
	/** Partitions that were revoked (owned before, not in new assignment) */
	revoked: TopicPartition[]
	/** Partitions that were added (not owned before, in new assignment) */
	added: TopicPartition[]
	/** Partitions that were kept (owned before and still in new assignment) */
	kept: TopicPartition[]
	/** Rebalance protocol selected by broker */
	protocol: RebalanceProtocol
	/** For cooperative: true if revoked partitions require rejoin after commit/pause */
	needsRejoin: boolean
}

/**
 * Consumer group coordinator
 */
export class ConsumerGroup extends EventEmitter<ConsumerGroupEvents> {
	private readonly cluster: Cluster
	private readonly config: ResolvedConsumerConfig
	private readonly assignor: PartitionAssignor
	private readonly logger: Logger

	private coordinator: Broker | null = null
	private memberId: string = ''
	private generationId: number = -1
	private state: ConsumerGroupState = ConsumerGroupState.UNJOINED
	private assignment: TopicPartition[] = []
	private heartbeatTimer: ReturnType<typeof setTimeout> | null = null
	private abortController: AbortController | null = null

	// Cooperative rebalance state
	private ownedPartitions: TopicPartition[] = [] // Current ownership (updated after each assignment)
	private pendingRevocation: TopicPartition[] = [] // Partitions pending revoke in phase 1
	private rebalanceProtocol: RebalanceProtocol = 'eager' // Set from broker-selected protocol
	private selectedProtocolName: string | null = null // Broker-selected protocol name from JoinGroup

	constructor(cluster: Cluster, config: ConsumerConfig, logger?: Logger, assignor?: PartitionAssignor) {
		super()
		this.cluster = cluster
		this.logger = logger?.child({ component: 'consumer-group', groupId: config.groupId }) ?? noopLogger
		this.config = {
			groupId: config.groupId,
			groupInstanceId: config.groupInstanceId || undefined,
			sessionTimeoutMs: config.sessionTimeoutMs ?? DEFAULT_CONSUMER_CONFIG.sessionTimeoutMs,
			rebalanceTimeoutMs: config.rebalanceTimeoutMs ?? DEFAULT_CONSUMER_CONFIG.rebalanceTimeoutMs,
			heartbeatIntervalMs: config.heartbeatIntervalMs ?? DEFAULT_CONSUMER_CONFIG.heartbeatIntervalMs,
			maxBytesPerPartition: config.maxBytesPerPartition ?? DEFAULT_CONSUMER_CONFIG.maxBytesPerPartition,
			minBytes: config.minBytes ?? DEFAULT_CONSUMER_CONFIG.minBytes,
			maxWaitMs: config.maxWaitMs ?? DEFAULT_CONSUMER_CONFIG.maxWaitMs,
			autoOffsetReset: config.autoOffsetReset ?? DEFAULT_CONSUMER_CONFIG.autoOffsetReset,
			isolationLevel: config.isolationLevel ?? DEFAULT_CONSUMER_CONFIG.isolationLevel,
			partitionAssignmentStrategy:
				config.partitionAssignmentStrategy ?? DEFAULT_CONSUMER_CONFIG.partitionAssignmentStrategy,
		}
		// If a custom assignor is provided, use it; otherwise derive from strategy
		this.assignor = assignor ?? this.getAssignorForStrategy(this.config.partitionAssignmentStrategy)
	}

	/**
	 * Get the primary assignor for a given strategy
	 */
	private getAssignorForStrategy(strategy: PartitionAssignmentStrategy): PartitionAssignor {
		switch (strategy) {
			case 'cooperative-sticky':
				return cooperativeStickyAssignor
			case 'sticky':
				return stickyAssignor
			case 'range':
				return rangeAssignor
		}
	}

	/**
	 * Build protocol list for JoinGroup based on strategy preference
	 * Returns protocols in preference order for broker selection
	 */
	private buildProtocolList(
		topics: string[],
		ownedPartitions: TopicPartitionList[] | undefined
	): Array<{ name: string; metadata: Buffer }> {
		const strategy = this.config.partitionAssignmentStrategy

		// Build metadata for cooperative (v1 with ownedPartitions)
		const v1Metadata = encodeSubscriptionMetadata({
			version: 1,
			topics,
			userData: null,
			ownedPartitions,
		})

		// Build metadata for eager (v0 without ownedPartitions)
		const v0Metadata = encodeSubscriptionMetadata({
			version: 0,
			topics,
			userData: null,
		})

		switch (strategy) {
			case 'cooperative-sticky':
				// Propose all protocols in preference order: cooperative-sticky → sticky → range
				// Broker picks the first mutually-supported protocol
				return [
					{ name: 'cooperative-sticky', metadata: v1Metadata },
					{ name: 'sticky', metadata: v0Metadata },
					{ name: 'range', metadata: v0Metadata },
				]

			case 'sticky':
				// Propose sticky → range (no cooperative)
				return [
					{ name: 'sticky', metadata: v0Metadata },
					{ name: 'range', metadata: v0Metadata },
				]

			case 'range':
				// Propose only range
				return [{ name: 'range', metadata: v0Metadata }]
		}
	}

	/**
	 * Get current group state
	 */
	get groupState(): ConsumerGroupState {
		return this.state
	}

	/**
	 * Get current member ID
	 */
	get currentMemberId(): string {
		return this.memberId
	}

	/**
	 * Get current generation ID
	 */
	get currentGenerationId(): number {
		return this.generationId
	}

	/**
	 * Get current partition assignment
	 */
	get currentAssignment(): TopicPartition[] {
		return [...this.assignment]
	}

	/**
	 * Get currently owned partitions (for cooperative rebalance)
	 */
	get currentOwnedPartitions(): TopicPartition[] {
		return [...this.ownedPartitions]
	}

	/**
	 * Get current rebalance protocol
	 */
	get currentRebalanceProtocol(): RebalanceProtocol {
		return this.rebalanceProtocol
	}

	/**
	 * Get current coordinator broker (for offset commits)
	 */
	getCoordinator(): Broker | null {
		return this.coordinator
	}

	/**
	 * Join the consumer group
	 *
	 * @param topics - Topics to subscribe to
	 * @returns Join result with assigned partitions and change info
	 */
	async join(topics: string[]): Promise<JoinResult> {
		this.abortController = new AbortController()
		this.logger.info('joining group', { topics })

		try {
			await this.ensureCoordinator()
			await this.joinGroup(topics)
			await this.syncGroup(topics)

			const result = this.syncGroupResult!
			this.syncGroupResult = null

			// For cooperative protocol, check for NEWLY revoked partitions
			// (partitions in result.revoked but not already in pendingRevocation)
			const tpKey = (tp: TopicPartition) => `${tp.topic}:${tp.partition}`
			const pendingKeys = new Set(this.pendingRevocation.map(tpKey))
			const newlyRevoked = result.revoked.filter(tp => !pendingKeys.has(tpKey(tp)))

			const needsRejoin = this.rebalanceProtocol === 'cooperative' && newlyRevoked.length > 0

			if (needsRejoin) {
				// Phase 1: new partitions being revoked, caller should commit/pause them and call rejoin
				this.logger.info('cooperative rebalance: partitions need revocation', {
					newlyRevokedCount: newlyRevoked.length,
					totalRevokedCount: result.revoked.length,
					pendingRevocationCount: this.pendingRevocation.length,
				})
				// Don't update ownedPartitions yet - that happens after the final assignment
				return {
					assignment: this.assignment,
					revoked: newlyRevoked, // Only return the NEW revocations
					added: result.added,
					kept: result.kept,
					protocol: this.rebalanceProtocol,
					needsRejoin: true,
				}
			}

			// Final assignment - update owned partitions
			this.ownedPartitions = [...this.assignment]
			this.pendingRevocation = []

			this.state = ConsumerGroupState.STABLE
			this.startHeartbeat()

			this.logger.info('group joined', {
				memberId: this.memberId,
				generationId: this.generationId,
				assignedPartitions: this.assignment.length,
				protocol: this.rebalanceProtocol,
			})
			this.emit('joined', this.assignment)

			return {
				assignment: this.assignment,
				revoked: result.revoked,
				added: result.added,
				kept: result.kept,
				protocol: this.rebalanceProtocol,
				needsRejoin: false,
			}
		} catch (error) {
			this.logger.error('join failed', { error: (error as Error).message })
			this.state = ConsumerGroupState.UNJOINED
			throw error
		}
	}

	/**
	 * Rejoin the consumer group (after rebalance)
	 *
	 * For cooperative rebalance, pass the revoked partitions from phase 1
	 * so they are excluded from the ownedPartitions in the rejoin metadata.
	 *
	 * @param topics - Topics to subscribe to
	 * @param revokedPartitions - Partitions revoked in phase 1 (cooperative only)
	 */
	async rejoin(topics: string[], revokedPartitions?: TopicPartition[]): Promise<JoinResult> {
		this.logger.debug('rejoining group', {
			topics,
			currentMemberId: this.memberId,
			revokedCount: revokedPartitions?.length ?? 0,
		})
		this.stopHeartbeat()
		this.state = ConsumerGroupState.UNJOINED

		// For cooperative rebalance, ACCUMULATE pendingRevocation so they are
		// excluded from ownedPartitions in the rejoin metadata
		if (revokedPartitions && revokedPartitions.length > 0) {
			// Add to existing pending revocations (don't replace)
			const existingKeys = new Set(this.pendingRevocation.map(tp => `${tp.topic}:${tp.partition}`))
			for (const tp of revokedPartitions) {
				const key = `${tp.topic}:${tp.partition}`
				if (!existingKeys.has(key)) {
					this.pendingRevocation.push(tp)
					existingKeys.add(key)
				}
			}
			// Remove revoked partitions from owned
			const revokedKeys = new Set(revokedPartitions.map(tp => `${tp.topic}:${tp.partition}`))
			this.ownedPartitions = this.ownedPartitions.filter(tp => !revokedKeys.has(`${tp.topic}:${tp.partition}`))
		}

		return this.join(topics)
	}

	/**
	 * Leave the consumer group
	 */
	async leave(): Promise<void> {
		this.logger.info('leaving group', { memberId: this.memberId })
		this.stopHeartbeat()
		this.state = ConsumerGroupState.LEAVING

		try {
			if (this.coordinator && this.memberId) {
				await this.coordinator.leaveGroup({
					groupId: this.config.groupId,
					members: [
						{
							memberId: this.memberId,
							groupInstanceId: this.config.groupInstanceId ?? null,
							reason: 'Consumer leaving group',
						},
					],
				})
				this.logger.debug('leave group response received')
			}
		} catch (error) {
			// Ignore errors during leave - we're leaving anyway
			this.logger.error('leave group error', { error: (error as Error).message })
			this.emit('error', error as Error)
		} finally {
			this.state = ConsumerGroupState.UNJOINED
			this.memberId = ''
			this.generationId = -1
			this.assignment = []
			this.ownedPartitions = []
			this.pendingRevocation = []
			this.coordinator = null
			this.logger.info('left group')
			this.emit('left')
		}
	}

	/**
	 * Stop the consumer group (abort heartbeat, leave group)
	 */
	async stop(): Promise<void> {
		this.abortController?.abort()
		await this.leave()
	}

	/**
	 * Ensure we have a valid coordinator connection
	 * Retries on CoordinatorNotAvailableError with exponential backoff
	 */
	private async ensureCoordinator(): Promise<void> {
		if (this.coordinator) {
			return
		}

		const maxRetries = 5
		const initialDelayMs = 500
		const maxDelayMs = 5000

		for (let attempt = 0; attempt < maxRetries; attempt++) {
			try {
				this.coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
				return
			} catch (error) {
				if (error instanceof CoordinatorNotAvailableError && attempt < maxRetries - 1) {
					const delay = Math.min(initialDelayMs * Math.pow(2, attempt), maxDelayMs)
					this.logger.debug('coordinator not available, retrying', {
						attempt: attempt + 1,
						maxRetries,
						delayMs: delay,
					})
					await new Promise(resolve => setTimeout(resolve, delay))
					continue
				}
				throw error
			}
		}
	}

	/**
	 * Refresh coordinator connection (after NOT_COORDINATOR error)
	 */
	private async refreshCoordinator(): Promise<void> {
		this.cluster.invalidateCoordinator('GROUP', this.config.groupId)
		this.coordinator = null
		await this.ensureCoordinator()
	}

	/**
	 * Get owned partitions for metadata (excludes pending revocations)
	 */
	private getOwnedPartitionsForMetadata(): TopicPartitionList[] {
		// Exclude partitions pending revocation (already committed in phase 1)
		const owned = this.ownedPartitions.filter(
			tp => !this.pendingRevocation.some(r => r.topic === tp.topic && r.partition === tp.partition)
		)

		// Convert to TopicPartitionList format
		const topicMap = new Map<string, number[]>()
		for (const tp of owned) {
			if (!topicMap.has(tp.topic)) {
				topicMap.set(tp.topic, [])
			}
			topicMap.get(tp.topic)!.push(tp.partition)
		}

		return Array.from(topicMap.entries()).map(([topic, partitions]) => ({
			topic,
			partitions: partitions.sort((a, b) => a - b),
		}))
	}

	/**
	 * Send JoinGroup request
	 */
	private async joinGroup(topics: string[]): Promise<void> {
		this.state = ConsumerGroupState.JOINING

		// Build owned partitions for v1 metadata (cooperative protocol)
		const ownedPartitions = this.getOwnedPartitionsForMetadata()

		// Build protocol list based on strategy preference
		const protocols = this.buildProtocolList(topics, ownedPartitions)

		this.logger.debug('sending JoinGroup request', {
			memberId: this.memberId,
			topics,
			strategy: this.config.partitionAssignmentStrategy,
			protocolCount: protocols.length,
			protocolNames: protocols.map(p => p.name),
			ownedPartitionCount: ownedPartitions.reduce((sum, tp) => sum + tp.partitions.length, 0),
		})

		const response = await this.coordinator!.joinGroup({
			groupId: this.config.groupId,
			sessionTimeoutMs: this.config.sessionTimeoutMs,
			rebalanceTimeoutMs: this.config.rebalanceTimeoutMs,
			memberId: this.memberId,
			groupInstanceId: this.config.groupInstanceId ?? null,
			protocolType: CONSUMER_PROTOCOL_TYPE,
			protocols,
		})

		this.logger.debug('JoinGroup response', {
			errorCode: response.errorCode,
			memberId: response.memberId,
			generationId: response.generationId,
			protocolName: response.protocolName,
			isLeader: response.leader === response.memberId,
		})

		// Handle errors
		if (response.errorCode !== ErrorCode.None) {
			this.logger.debug('JoinGroup error', { errorCode: response.errorCode })
			if (response.errorCode === ErrorCode.MemberIdRequired && response.memberId) {
				// Coordinator generated a member ID for us; reuse it on the retry
				this.memberId = response.memberId
			}
			await this.handleJoinGroupError(response.errorCode, topics)
			return
		}

		// Update member info
		this.memberId = response.memberId
		this.generationId = response.generationId
		this.selectedProtocolName = response.protocolName

		// Set rebalance protocol from broker-selected protocol name
		if (response.protocolName === 'cooperative-sticky') {
			this.rebalanceProtocol = 'cooperative'
		} else {
			this.rebalanceProtocol = 'eager'
		}

		// If we're the leader, we need to compute assignments
		if (response.leader === this.memberId) {
			this.logger.debug('this member is leader, computing assignments', {
				memberCount: response.members.length,
				protocolName: response.protocolName,
			})
			await this.computeAssignments(topics, response.members, response.protocolName)
		}

		this.state = ConsumerGroupState.AWAITING_SYNC
	}

	/**
	 * Handle JoinGroup errors
	 */
	private async handleJoinGroupError(errorCode: ErrorCode, topics: string[]): Promise<void> {
		switch (errorCode) {
			case ErrorCode.MemberIdRequired:
				// Need to retry with the assigned member ID
				// The member ID should already be set from the response
				await this.joinGroup(topics)
				break

			case ErrorCode.UnknownMemberId:
			case ErrorCode.IllegalGeneration:
				// Clear member ID and rejoin
				this.memberId = ''
				this.generationId = -1
				await this.joinGroup(topics)
				break

			case ErrorCode.RebalanceInProgress:
				// Wait a bit and retry
				await sleep(1000, { signal: this.abortController?.signal })
				await this.joinGroup(topics)
				break
			case ErrorCode.CoordinatorLoadInProgress:
				// Coordinator is starting up - backoff and retry
				await sleep(500, { signal: this.abortController?.signal })
				await this.joinGroup(topics)
				break

			case ErrorCode.NotCoordinator:
			case ErrorCode.CoordinatorNotAvailable:
				// Refresh coordinator and retry
				await this.refreshCoordinator()
				await this.joinGroup(topics)
				break

			case ErrorCode.FencedInstanceId:
				// Fatal error - another instance with same groupInstanceId is active
				throw new KafkaProtocolError(
					errorCode,
					`Static member instance ${this.config.groupInstanceId} is fenced`
				)

			default:
				throw new KafkaProtocolError(errorCode, 'JoinGroup failed')
		}
	}

	/**
	 * Get the assignor for a given protocol name
	 */
	private getAssignorForProtocol(protocolName: string | null): PartitionAssignor {
		switch (protocolName) {
			case 'cooperative-sticky':
				return cooperativeStickyAssignor
			case 'sticky':
				return stickyAssignor
			case 'range':
				return rangeAssignor
			default:
				// Fall back to configured assignor
				return this.assignor
		}
	}

	/**
	 * Compute partition assignments (leader only)
	 */
	private async computeAssignments(
		topics: string[],
		members: Array<{ memberId: string; groupInstanceId: string | null; metadata: Buffer }>,
		protocolName: string | null
	): Promise<void> {
		// Fetch metadata for all subscribed topics
		const metadata = await this.cluster.refreshMetadata(topics)

		// Build topic -> partitions map
		const topicPartitions = new Map<string, number[]>()
		for (const topic of topics) {
			const topicMeta = metadata.topics.get(topic)
			if (topicMeta) {
				topicPartitions.set(topic, Array.from(topicMeta.partitions.keys()))
			}
		}

		// Build member subscriptions
		const memberSubscriptions: MemberSubscription[] = members.map(m => ({
			memberId: m.memberId,
			groupInstanceId: m.groupInstanceId ?? undefined,
			metadata: decodeSubscriptionMetadata(m.metadata),
		}))

		// Use the assignor matching the broker-selected protocol
		const assignor = this.getAssignorForProtocol(protocolName)
		this.logger.debug('computing assignments', {
			assignor: assignor.name,
			protocolName,
			memberCount: members.length,
			topicCount: topics.length,
		})

		// Compute assignments using the selected assignor
		this.leaderAssignments = assignor.assign(memberSubscriptions, topicPartitions)
	}

	// Temporary storage for leader assignments (used in syncGroup)
	private leaderAssignments: Map<
		string,
		{ version: number; partitions: Array<{ topic: string; partitions: number[] }>; userData: Buffer | null }
	> | null = null

	/**
	 * Result from syncGroup with partition change information
	 */
	private syncGroupResult: {
		revoked: TopicPartition[]
		added: TopicPartition[]
		kept: TopicPartition[]
	} | null = null

	/**
	 * Send SyncGroup request
	 */
	private async syncGroup(topics: string[]): Promise<void> {
		this.logger.debug('sending SyncGroup request', { memberId: this.memberId, generationId: this.generationId })

		// Prepare assignments (only leader sends non-empty assignments)
		const assignments: Array<{ memberId: string; assignment: Buffer }> = []

		if (this.leaderAssignments) {
			for (const [memberId, assignment] of this.leaderAssignments) {
				assignments.push({
					memberId,
					assignment: encodeAssignment(assignment),
				})
			}
			this.leaderAssignments = null
		}

		const response = await this.coordinator!.syncGroup({
			groupId: this.config.groupId,
			generationId: this.generationId,
			memberId: this.memberId,
			groupInstanceId: this.config.groupInstanceId ?? null,
			protocolType: CONSUMER_PROTOCOL_TYPE,
			protocolName: this.selectedProtocolName ?? this.assignor.name,
			assignments,
		})

		// Handle errors
		if (response.errorCode !== ErrorCode.None) {
			this.logger.debug('SyncGroup error', { errorCode: response.errorCode })
			await this.handleSyncGroupError(response.errorCode, topics)
			return
		}

		// Set rebalance protocol from broker-selected protocol name.
		//
		// SyncGroupResponse only includes protocolName for flexible versions (v5+). For older versions,
		// fall back to the JoinGroup-selected protocol name.
		const protocolName = response.protocolName || this.selectedProtocolName
		if (protocolName === 'cooperative-sticky') {
			this.rebalanceProtocol = 'cooperative'
		} else {
			this.rebalanceProtocol = 'eager'
		}

		// Decode our assignment
		const assignment = decodeAssignment(response.assignment)
		const newAssignment = assignmentToTopicPartitions(assignment)

		// Compute partition changes
		const tpKey = (tp: TopicPartition) => `${tp.topic}:${tp.partition}`
		const oldKeys = new Set(this.ownedPartitions.map(tpKey))
		const newKeys = new Set(newAssignment.map(tpKey))

		const revoked = this.ownedPartitions.filter(tp => !newKeys.has(tpKey(tp)))
		const added = newAssignment.filter(tp => !oldKeys.has(tpKey(tp)))
		const kept = newAssignment.filter(tp => oldKeys.has(tpKey(tp)))

		this.syncGroupResult = { revoked, added, kept }
		this.assignment = newAssignment

		this.logger.debug('SyncGroup response', {
			protocol: this.rebalanceProtocol,
			assignedPartitions: this.assignment.length,
			revokedCount: revoked.length,
			addedCount: added.length,
			keptCount: kept.length,
		})
	}

	/**
	 * Handle SyncGroup errors
	 */
	private async handleSyncGroupError(errorCode: ErrorCode, topics: string[]): Promise<void> {
		switch (errorCode) {
			case ErrorCode.RebalanceInProgress:
				// Rejoin from scratch
				this.state = ConsumerGroupState.UNJOINED
				await this.joinGroup(topics)
				await this.syncGroup(topics)
				break

			case ErrorCode.UnknownMemberId:
			case ErrorCode.IllegalGeneration:
				// Clear member ID and rejoin
				this.memberId = ''
				this.generationId = -1
				this.state = ConsumerGroupState.UNJOINED
				await this.joinGroup(topics)
				await this.syncGroup(topics)
				break

			case ErrorCode.NotCoordinator:
			case ErrorCode.CoordinatorNotAvailable:
				// Refresh coordinator and rejoin
				await this.refreshCoordinator()
				this.state = ConsumerGroupState.UNJOINED
				await this.joinGroup(topics)
				await this.syncGroup(topics)
				break

			case ErrorCode.FencedInstanceId:
				throw new KafkaProtocolError(
					errorCode,
					`Static member instance ${this.config.groupInstanceId} is fenced`
				)

			default:
				throw new KafkaProtocolError(errorCode, 'SyncGroup failed')
		}
	}

	/**
	 * Start heartbeat loop
	 */
	private startHeartbeat(): void {
		if (this.heartbeatTimer) {
			return
		}

		this.logger.debug('starting heartbeat loop', { intervalMs: this.config.heartbeatIntervalMs })

		const runHeartbeat = () => {
			if (this.state !== ConsumerGroupState.STABLE || this.abortController?.signal.aborted) {
				return
			}

			this.logger.debug('sending heartbeat', { generationId: this.generationId })

			this.coordinator!.heartbeat({
				groupId: this.config.groupId,
				generationId: this.generationId,
				memberId: this.memberId,
				groupInstanceId: this.config.groupInstanceId ?? null,
			})
				.then(response => {
					if (response.errorCode === ErrorCode.RebalanceInProgress) {
						this.logger.debug('heartbeat indicates rebalance in progress')
						this.emit('rebalance')
						return
					}

					if (response.errorCode !== ErrorCode.None) {
						this.logger.debug('heartbeat error', { errorCode: response.errorCode })
						return this.handleHeartbeatError(response.errorCode)
					}

					// Schedule next heartbeat
					this.heartbeatTimer = setTimeout(runHeartbeat, this.config.heartbeatIntervalMs)
				})
				.catch(error => {
					this.logger.error('heartbeat failed', { error: (error as Error).message })
					this.emit('error', error as Error)
					// Try to recover
					this.heartbeatTimer = setTimeout(runHeartbeat, this.config.heartbeatIntervalMs)
				})
		}

		// Start first heartbeat
		this.heartbeatTimer = setTimeout(runHeartbeat, this.config.heartbeatIntervalMs)
	}

	/**
	 * Stop heartbeat loop
	 */
	private stopHeartbeat(): void {
		if (this.heartbeatTimer) {
			clearTimeout(this.heartbeatTimer)
			this.heartbeatTimer = null
		}
	}

	/**
	 * Handle heartbeat errors
	 */
	private async handleHeartbeatError(errorCode: ErrorCode): Promise<void> {
		switch (errorCode) {
			case ErrorCode.RebalanceInProgress:
				// For cooperative: don't stop fetching, trigger rejoin while keeping partitions
				// For eager: stop everything and rejoin
				this.emit('rebalance')
				break

			case ErrorCode.UnknownMemberId: {
				// Fatal session loss - lost generation without ability to commit
				// Emit sessionLost with the partitions we lost (can't commit them anymore)
				this.logger.info('session lost - unknown member id', { memberId: this.memberId })
				const lostPartitions = [...this.ownedPartitions]
				this.memberId = ''
				this.generationId = -1
				this.ownedPartitions = []
				this.pendingRevocation = []
				// Emit sessionLost first (Consumer translates to partitionsLost)
				if (lostPartitions.length > 0) {
					this.emit('sessionLost', lostPartitions)
				}

				// For static members, UNKNOWN_MEMBER_ID after being replaced is a fencing scenario.
				// Surface this as FENCED_INSTANCE_ID and stop instead of trying to transparently rejoin.
				if (this.config.groupInstanceId) {
					this.emit(
						'error',
						new KafkaProtocolError(ErrorCode.FencedInstanceId, 'Static member instance is fenced')
					)
					break
				}

				this.emit('rebalance')
				break
			}

			case ErrorCode.IllegalGeneration:
				// Common during cooperative rebalances
				// Clear generation but keep memberId for rejoin attempt
				this.logger.info('illegal generation', { generationId: this.generationId })
				this.generationId = -1
				this.emit('rebalance')
				break

			case ErrorCode.NotCoordinator:
			case ErrorCode.CoordinatorNotAvailable:
				this.coordinator = null
				await this.refreshCoordinator()
				break

			case ErrorCode.FencedInstanceId: {
				// Fatal error - another instance with same groupInstanceId is active
				// Emit sessionLost before resetting - can't commit offsets for these partitions
				this.logger.info('session lost - instance fenced', { groupInstanceId: this.config.groupInstanceId })
				const fencedPartitions = [...this.ownedPartitions]
				this.ownedPartitions = []
				this.pendingRevocation = []
				// Emit sessionLost first (Consumer translates to partitionsLost)
				if (fencedPartitions.length > 0) {
					this.emit('sessionLost', fencedPartitions)
				}
				this.emit('error', new KafkaProtocolError(errorCode, 'Static member instance is fenced'))
				break
			}

			default:
				this.emit('error', new KafkaProtocolError(errorCode, 'Heartbeat failed'))
		}
	}

	/**
	 * Reset ownership after fatal session loss
	 * Called when session times out or member is fenced
	 */
	resetOwnership(): void {
		this.ownedPartitions = []
		this.pendingRevocation = []
	}
}
