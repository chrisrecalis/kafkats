/**
 * Partition provider abstraction for unified assignment handling
 *
 * Abstracts the source of partition assignments (manual vs consumer group)
 * so the Consumer can use a single code path for the fetch loop.
 */

import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import type { Logger } from '@/logger.js'
import type { ConsumerGroup, JoinResult } from './consumer-group.js'
import type { OffsetManager } from './offset-manager.js'
import type { TopicPartition, ManualAssignment, AutoOffsetReset } from './types.js'
import { retry } from '@/utils/retry.js'
import { sleep } from '@/utils/sleep.js'
import { CoordinatorNotAvailableError, NotCoordinatorError } from '@/client/errors.js'

/**
 * Partition with resolved offset (internal type)
 */
export type TopicPartitionOffset = TopicPartition & { offset: bigint }

export interface PartitionProviderCallbacks {
	// Awaited at the start of the rebalance protocol so transactional consumers can commit
	// in-flight work before partitions are revoked.
	onRebalance: () => Promise<void>
	onPartitionsAssigned: (partitions: TopicPartitionOffset[]) => Promise<void>
	onPartitionsRevoked: (partitions: TopicPartition[]) => Promise<void>
	onPartitionsLost: (partitions: TopicPartition[]) => void
	onError: (error: Error) => void
}

export interface PartitionProvider {
	start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]>
	stop(): Promise<void>
	/**
	 * Check for and handle any pending rebalance synchronously.
	 * Called by the poll loop before processing batches to ensure
	 * rebalance happens between poll iterations, not during handler execution.
	 */
	checkAndHandleRebalance(): Promise<void>
}

export interface ManualPartitionProviderConfig {
	assignment: ManualAssignment[]
	cluster: Cluster
	groupId: string
	offsetManager: OffsetManager
	autoOffsetReset: AutoOffsetReset
	signal?: AbortSignal
}

export class ManualPartitionProvider implements PartitionProvider {
	private readonly assignment: ManualAssignment[]
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly offsetManager: OffsetManager
	private readonly autoOffsetReset: AutoOffsetReset
	private readonly signal?: AbortSignal

	constructor(config: ManualPartitionProviderConfig) {
		this.assignment = config.assignment
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.offsetManager = config.offsetManager
		this.autoOffsetReset = config.autoOffsetReset
		this.signal = config.signal
	}

	async start(_topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		const coordinator = await this.getCoordinatorWithRetry()
		this.offsetManager.updateGroupState('', -1, coordinator)

		const partitions: TopicPartition[] = this.assignment.map(tp => ({
			topic: tp.topic,
			partition: tp.partition,
		}))
		const partitionsWithOffsets = await this.resolveOffsets(partitions)
		await callbacks.onPartitionsAssigned(partitionsWithOffsets)

		return partitions
	}

	async stop(): Promise<void> {}

	async checkAndHandleRebalance(): Promise<void> {
		// Manual assignment doesn't do rebalancing
	}

	private async resolveOffsets(partitions: TopicPartition[]): Promise<TopicPartitionOffset[]> {
		const manualOffsets = new Map<string, bigint>()
		for (const tp of this.assignment) {
			if (tp.offset !== undefined) {
				manualOffsets.set(`${tp.topic}:${tp.partition}`, tp.offset)
			}
		}

		const committedOffsets = await this.offsetManager.fetchCommittedOffsets(partitions)

		const result: TopicPartitionOffset[] = []
		for (const tp of partitions) {
			const key = `${tp.topic}:${tp.partition}`
			const manualOffset = manualOffsets.get(key)
			const offset =
				manualOffset !== undefined
					? manualOffset
					: await this.offsetManager.resolveStartingOffset(
							tp.topic,
							tp.partition,
							this.autoOffsetReset,
							committedOffsets
						)
			result.push({ ...tp, offset })
		}

		return result
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

export interface GroupPartitionProviderConfig {
	consumerGroup: ConsumerGroup
	cluster: Cluster
	groupId: string
	autoOffsetReset: AutoOffsetReset
	offsetManager: OffsetManager
	logger: Logger
	signal?: AbortSignal
	isRunning: () => boolean
}

export class GroupPartitionProvider implements PartitionProvider {
	private readonly consumerGroup: ConsumerGroup
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly autoOffsetReset: AutoOffsetReset
	private readonly offsetManager: OffsetManager
	private readonly logger: Logger
	private readonly signal?: AbortSignal
	private readonly isRunning: () => boolean

	private callbacks: PartitionProviderCallbacks | null = null
	private topics: string[] = []
	private rebalancePending = false

	constructor(config: GroupPartitionProviderConfig) {
		this.consumerGroup = config.consumerGroup
		this.cluster = config.cluster
		this.groupId = config.groupId
		this.autoOffsetReset = config.autoOffsetReset
		this.offsetManager = config.offsetManager
		this.logger = config.logger
		this.signal = config.signal
		this.isRunning = config.isRunning
	}

	async start(topics: string[], callbacks: PartitionProviderCallbacks): Promise<TopicPartition[]> {
		this.callbacks = callbacks
		this.topics = topics

		this.consumerGroup.on('rebalance', () => this.onRebalanceEvent())
		this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => callbacks.onPartitionsLost(partitions))
		this.consumerGroup.on('error', (error: Error) => callbacks.onError(error))

		const joinResult = await this.consumerGroup.join(topics)
		await this.updateGroupState()

		const partitionsWithOffsets = await this.resolveOffsets(joinResult.assignment)
		await callbacks.onPartitionsAssigned(partitionsWithOffsets)

		return joinResult.assignment
	}

	async stop(): Promise<void> {
		await this.consumerGroup.stop()
	}

	private onRebalanceEvent(): void {
		this.logger.debug('rebalance event received')
		this.rebalancePending = true
	}

	async checkAndHandleRebalance(): Promise<void> {
		if (!this.rebalancePending || !this.isRunning() || !this.callbacks) {
			return
		}

		this.rebalancePending = false

		try {
			// Awaited so transactional consumers can commit in-flight work before partitions move.
			await this.callbacks.onRebalance()

			const previousAssignment = this.consumerGroup.currentAssignment
			const wasEager = this.consumerGroup.currentRebalanceProtocol === 'eager'

			// Eager protocol: revoke ALL partitions (which commits their final offsets)
			// BEFORE rejoining. The commit runs under the OLD generation, which the broker
			// accepts until the member rejoins — committing after JoinGroup would let the
			// new owner fetch a stale committed offset and reprocess records.
			if (wasEager && previousAssignment.length > 0) {
				await this.callbacks.onPartitionsRevoked(previousAssignment)
			}

			const rejoinResult = await this.consumerGroup.rejoin(this.topics)
			if (!this.isRunning()) return

			await this.updateGroupState()

			if (rejoinResult.protocol === 'eager') {
				// If the previous generation was eager everything was already revoked
				// pre-join; only a cooperative→eager protocol switch still needs the revoke.
				await this.handleEagerRebalance(wasEager ? [] : previousAssignment, rejoinResult.assignment)
			} else {
				// If the previous generation was eager, all partitions were revoked pre-join,
				// so the full final assignment (not just the added delta) must be assigned.
				await this.handleCooperativeRebalance(rejoinResult, wasEager)
			}
		} catch (error) {
			const err = error instanceof Error ? error : new Error(String(error))
			this.logger.error('rebalance handler failed', { error: err.message })
			// Stop the group: if the rebalance failed (e.g. EOS commit in onRebalance),
			// continuing the fetch loop on the stale assignment would be unsafe. Rethrow so
			// the poll loop that drives checkAndHandleRebalance exits instead of fetching
			// partitions this consumer no longer owns after leaving the group.
			void this.consumerGroup.stop().catch(() => {})
			throw err
		}
	}

	private async handleEagerRebalance(
		previousAssignment: TopicPartition[],
		assignment: TopicPartition[]
	): Promise<void> {
		this.logger.debug('eager rebalance: assigning new generation', {
			pendingRevokeCount: previousAssignment.length,
		})

		// Normally empty: the eager protocol revokes before the rejoin. Non-empty only
		// on a cooperative→eager protocol switch, where the revoke could not happen early.
		if (previousAssignment.length > 0) {
			await this.callbacks!.onPartitionsRevoked(previousAssignment)
		}

		const partitionsWithOffsets = await this.resolveOffsets(assignment)
		await this.callbacks!.onPartitionsAssigned(partitionsWithOffsets)
	}

	/**
	 * @param assignFullAssignment - assign the full final assignment instead of only the
	 * added delta. Used when the previous generation ran the eager protocol (all
	 * partitions were revoked before the rejoin), so kept partitions must be re-added.
	 */
	private async handleCooperativeRebalance(rejoinResult: JoinResult, assignFullAssignment = false): Promise<void> {
		if (rejoinResult.protocol === 'cooperative') {
			this.logger.debug('cooperative rebalance: phase 1', {
				revokedCount: rejoinResult.revoked.length,
				keptCount: rejoinResult.kept.length,
			})

			if (rejoinResult.revoked.length > 0) {
				await this.callbacks!.onPartitionsRevoked(rejoinResult.revoked)
			}

			// Phase 2: rejoin loop until stable
			let rejoinAttempt = 0
			const MAX_REJOIN_ATTEMPTS = 5
			const REJOIN_BACKOFF_MS = 500

			while (rejoinResult.needsRejoin) {
				rejoinAttempt++
				if (rejoinAttempt > MAX_REJOIN_ATTEMPTS) {
					throw new Error(`Max cooperative rejoin attempts exceeded (${MAX_REJOIN_ATTEMPTS})`)
				}

				const backoffMs = Math.min(REJOIN_BACKOFF_MS * rejoinAttempt, 3000)
				await sleep(backoffMs, { signal: this.signal, resolveOnAbort: true })
				if (!this.isRunning()) return

				rejoinResult = await this.consumerGroup.rejoin(this.topics, rejoinResult.revoked)
				await this.updateGroupState()

				if (rejoinResult.needsRejoin && rejoinResult.revoked.length > 0) {
					await this.callbacks!.onPartitionsRevoked(rejoinResult.revoked)
				}
			}

			this.logger.debug('cooperative rebalance: complete', {
				assignmentCount: rejoinResult.assignment.length,
				addedCount: rejoinResult.added.length,
			})

			const toAssign = assignFullAssignment ? rejoinResult.assignment : rejoinResult.added
			if (toAssign.length > 0) {
				const assignedWithOffsets = await this.resolveOffsets(toAssign)
				await this.callbacks!.onPartitionsAssigned(assignedWithOffsets)
			}
		}
	}

	private async updateGroupState(): Promise<void> {
		const coordinator = await this.cluster.getCoordinator('GROUP', this.groupId)
		this.offsetManager.updateGroupState(
			this.consumerGroup.currentMemberId,
			this.consumerGroup.currentGenerationId,
			coordinator
		)
	}

	private async resolveOffsets(partitions: TopicPartition[]): Promise<TopicPartitionOffset[]> {
		const committedOffsets = await this.offsetManager.fetchCommittedOffsets(partitions)

		const result: TopicPartitionOffset[] = []
		for (const tp of partitions) {
			const offset = await this.offsetManager.resolveStartingOffset(
				tp.topic,
				tp.partition,
				this.autoOffsetReset,
				committedOffsets
			)
			result.push({ ...tp, offset })
		}

		return result
	}
}
