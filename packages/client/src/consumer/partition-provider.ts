/**
 * Partition provider abstraction for unified assignment handling
 *
 * Abstracts the source of partition assignments (manual vs consumer group)
 * so the Consumer can use a single code path for the fetch loop.
 */

import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import type { Logger } from '@/logger.js'
import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	executeCooperativeRejoinLoop,
	handleCooperativePhase1,
	addNewPartitions,
	updateGroupStateAfterRejoin,
	type RebalanceContext,
} from './rebalance-handler.js'
import type { ConsumerGroup } from './consumer-group.js'
import type { OffsetManager } from './offset-manager.js'
import type { FetchManager } from './fetch-manager.js'
import type { TopicPartition, ManualAssignment, AutoOffsetReset } from './types.js'
import { retry } from '@/utils/retry.js'
import { CoordinatorNotAvailableError, NotCoordinatorError } from '@/client/errors.js'

/**
 * Callbacks for partition provider events
 */
export interface PartitionProviderCallbacks {
	onPartitionsAssigned: (partitions: TopicPartition[]) => Promise<void>
	onPartitionsRevoked: (partitions: TopicPartition[]) => Promise<void>
	onPartitionsLost: (partitions: TopicPartition[]) => void
	onError: (error: Error) => void
	resolveOffsets: (partitions: TopicPartition[]) => Promise<Array<TopicPartition & { offset: bigint }>>
}

/**
 * Partition provider interface
 */
export interface PartitionProvider {
	/** Start and return initial partitions */
	start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]>
	/** Stop (leave group for GroupProvider, no-op for ManualProvider) */
	stop(): Promise<void>
}

/**
 * Configuration for ManualPartitionProvider
 */
export interface ManualPartitionProviderConfig {
	assignment: ManualAssignment[]
	cluster: Cluster
	groupId: string
	offsetManager: OffsetManager
	signal?: AbortSignal
}

/**
 * Manual partition provider - static assignment, no rebalancing
 */
export class ManualPartitionProvider implements PartitionProvider {
	private readonly assignment: ManualAssignment[]
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly offsetManager: OffsetManager
	private readonly signal?: AbortSignal

	constructor(config: ManualPartitionProviderConfig) {
		this.assignment = config.assignment
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.offsetManager = config.offsetManager
		this.signal = config.signal
	}

	async start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		// Get coordinator for offset commits (with retry)
		const coordinator = await this.getCoordinatorWithRetry()
		this.offsetManager.updateGroupState('', -1, coordinator)

		// Build partition list
		const partitions: TopicPartition[] = this.assignment.map(tp => ({
			topic: tp.topic,
			partition: tp.partition,
		}))

		// Resolve offsets and notify
		await callbacks.onPartitionsAssigned(partitions)

		return partitions
	}

	async stop(): Promise<void> {
		// No-op for manual assignment
	}

	/**
	 * Get manual offset overrides as a map
	 */
	getManualOffsets(): Map<string, bigint> {
		const offsets = new Map<string, bigint>()
		for (const tp of this.assignment) {
			if (tp.offset !== undefined) {
				offsets.set(`${tp.topic}:${tp.partition}`, tp.offset)
			}
		}
		return offsets
	}

	private async getCoordinatorWithRetry(): Promise<Broker> {
		return await retry(() => this.cluster.getCoordinator('GROUP', this.groupId), {
			maxAttempts: 10,
			initialDelayMs: 100,
			maxDelayMs: 1_000,
			multiplier: 2,
			jitter: 0,
			signal: this.signal,
			resolveOnAbort: true,
			shouldRetry: error => {
				if (this.signal?.aborted) {
					return false
				}
				return error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError
			},
		})
	}
}

/**
 * Configuration for GroupPartitionProvider
 */
export interface GroupPartitionProviderConfig {
	consumerGroup: ConsumerGroup
	cluster: Cluster
	groupId: string
	autoOffsetReset: AutoOffsetReset
	commitOffsets: boolean
	offsetManager: OffsetManager
	fetchManager: FetchManager
	logger: Logger
	signal?: AbortSignal
	isRunning: () => boolean
}

/**
 * Group partition provider - dynamic assignment via consumer group
 */
export class GroupPartitionProvider implements PartitionProvider {
	private readonly consumerGroup: ConsumerGroup
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly autoOffsetReset: AutoOffsetReset
	private readonly commitOffsets: boolean
	private readonly offsetManager: OffsetManager
	private readonly fetchManager: FetchManager
	private readonly logger: Logger
	private readonly signal?: AbortSignal
	private readonly isRunning: () => boolean

	private callbacks: PartitionProviderCallbacks | null = null
	private topics: string[] = []
	private rebalanceRunner: Promise<void> | null = null
	private rebalanceRequested = false

	constructor(config: GroupPartitionProviderConfig) {
		this.consumerGroup = config.consumerGroup
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.autoOffsetReset = config.autoOffsetReset
		this.commitOffsets = config.commitOffsets
		this.offsetManager = config.offsetManager
		this.fetchManager = config.fetchManager
		this.logger = config.logger
		this.signal = config.signal
		this.isRunning = config.isRunning
	}

	async start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		this.callbacks = callbacks
		this.topics = topics

		// Attach event handlers
		this.consumerGroup.on('rebalance', () => this.onRebalanceEvent())
		this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
			this.offsetManager.clearPartitions(partitions)
			this.fetchManager.removePartitions(partitions)
			callbacks.onPartitionsLost(partitions)
		})
		this.consumerGroup.on('error', (error: Error) => {
			callbacks.onError(error)
			if (error instanceof KafkaProtocolError && error.errorCode === ErrorCode.FencedInstanceId) {
				// Fatal error - caller should stop
			}
		})

		// Join group
		const joinResult = await this.consumerGroup.join(topics)

		// Setup initial partitions
		await this.setupPartitions(joinResult.assignment, callbacks)

		return joinResult.assignment
	}

	async stop(): Promise<void> {
		await this.consumerGroup.stop()
	}

	private onRebalanceEvent(): void {
		this.logger.debug('rebalance event received')
		this.rebalanceRequested = true

		if (this.rebalanceRunner) {
			return
		}

		this.rebalanceRunner = (async () => {
			while (this.rebalanceRequested && this.isRunning()) {
				this.rebalanceRequested = false
				await this.handleRebalance()
			}
		})()
			.catch(error => {
				const err = error instanceof Error ? error : new Error(String(error))
				this.logger.error('rebalance handler failed', { error: err.message })
				this.callbacks?.onError(err)
			})
			.finally(() => {
				this.rebalanceRunner = null
			})
	}

	private async handleRebalance(): Promise<void> {
		if (!this.isRunning() || !this.callbacks) {
			this.logger.debug('ignoring rebalance - consumer is stopping')
			return
		}

		const ctx = this.createRebalanceContext()
		const previousAssignment = this.consumerGroup.currentAssignment

		// Eager rebalance: stop-the-world
		if (this.consumerGroup.currentRebalanceProtocol === 'eager') {
			await this.handleEagerRebalance(ctx, previousAssignment)
			return
		}

		// Cooperative rebalance
		await this.handleCooperativeRebalance(ctx)
	}

	private async handleEagerRebalance(ctx: RebalanceContext, previousAssignment: TopicPartition[]): Promise<void> {
		this.logger.info('eager rebalance: stopping all partitions')

		// Emit partitionsRevoked for all previously-owned partitions
		if (previousAssignment.length > 0) {
			await this.callbacks!.onPartitionsRevoked(previousAssignment)
		}

		// Stop fetching
		this.fetchManager.setPartitions([])

		// Commit pending offsets
		if (this.commitOffsets) {
			await this.offsetManager.commitPendingOffsets()
		}
		this.offsetManager.clearConsumedOffsets()

		// Rejoin group
		const rejoinResult = await this.consumerGroup.rejoin(this.topics)

		if (!this.isRunning()) {
			this.logger.debug('aborting eager rebalance - consumer is stopping')
			return
		}

		// Update group state
		await updateGroupStateAfterRejoin(ctx)

		// Setup new partitions (this also emits partitionsAssigned via the callback)
		await this.setupPartitions(rejoinResult.assignment, this.callbacks!)
	}

	private async handleCooperativeRebalance(ctx: RebalanceContext): Promise<void> {
		// First rejoin to learn what must be revoked
		let rejoinResult = await this.consumerGroup.rejoin(this.topics)

		if (!this.isRunning()) {
			this.logger.debug('aborting rebalance - consumer is stopping')
			return
		}

		await updateGroupStateAfterRejoin(ctx)

		if (rejoinResult.protocol === 'cooperative') {
			this.logger.info('cooperative rebalance: phase 1 - revoking partitions', {
				revokedCount: rejoinResult.revoked.length,
				keptCount: rejoinResult.kept.length,
			})

			// Phase 1: Handle revoked partitions
			await handleCooperativePhase1(ctx, rejoinResult.revoked, {
				onPartitionsRevoked: p => this.callbacks!.onPartitionsRevoked(p),
				onPartitionsAssigned: p => this.callbacks!.onPartitionsAssigned(p),
			})

			// Phase 2: Rejoin loop until stable
			rejoinResult = await executeCooperativeRejoinLoop(ctx, this.topics, rejoinResult, {
				onPartitionsRevoked: p => this.callbacks!.onPartitionsRevoked(p),
				onPartitionsAssigned: p => this.callbacks!.onPartitionsAssigned(p),
			})

			this.logger.info('cooperative rebalance: phase 2 complete - applying final assignment', {
				assignmentCount: rejoinResult.assignment.length,
				addedCount: rejoinResult.added.length,
			})

			// Final cleanup
			this.fetchManager.removePartitions(rejoinResult.revoked)

			// Add new partitions
			await addNewPartitions(ctx, rejoinResult.added, {
				onPartitionsRevoked: p => this.callbacks!.onPartitionsRevoked(p),
				onPartitionsAssigned: p => this.callbacks!.onPartitionsAssigned(p),
			})

			await updateGroupStateAfterRejoin(ctx)
		}
	}

	private async setupPartitions(assignment: TopicPartition[], callbacks: PartitionProviderCallbacks): Promise<void> {
		// Update group state
		const coordinator = await this.cluster.getCoordinator('GROUP', this.groupId)
		this.offsetManager.updateGroupState(
			this.consumerGroup.currentMemberId,
			this.consumerGroup.currentGenerationId,
			coordinator
		)

		// Let the callback handle offset resolution, partition setup, and event emission
		await callbacks.onPartitionsAssigned(assignment)
	}

	private createRebalanceContext(): RebalanceContext {
		return {
			cluster: this.cluster,
			groupId: this.groupId,
			consumerGroup: this.consumerGroup,
			offsetManager: this.offsetManager,
			fetchManager: this.fetchManager,
			autoOffsetReset: this.autoOffsetReset,
			commitOffsets: this.commitOffsets,
			signal: this.signal,
			isRunning: this.isRunning,
			logger: this.logger,
		}
	}
}
