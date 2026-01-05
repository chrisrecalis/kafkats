/**
 * Rebalance handling utilities for the consumer
 *
 * Provides shared logic for handling consumer group rebalances.
 * Supports both eager and cooperative rebalance protocols.
 */

import type { Cluster } from '@/client/cluster.js'
import type { Logger } from '@/logger.js'
import { sleep } from '@/utils/sleep.js'
import type { ConsumerGroup, JoinResult } from './consumer-group.js'
import type { OffsetManager } from './offset-manager.js'
import type { FetchManager } from './fetch-manager.js'
import type { TopicPartition, TopicSubscription, AutoOffsetReset } from './types.js'

/**
 * Cooperative rebalance retry limits
 */
const MAX_COOPERATIVE_REJOIN_ATTEMPTS = 5
const REJOIN_BACKOFF_MS = 500

/**
 * Callbacks for rebalance events
 */
export interface RebalanceCallbacks {
	onPartitionsRevoked: (partitions: TopicPartition[]) => void | Promise<void>
	onPartitionsAssigned: (partitions: TopicPartition[]) => void | Promise<void>
}

/**
 * Context needed for rebalance operations
 */
export interface RebalanceContext {
	cluster: Cluster
	groupId: string
	consumerGroup: ConsumerGroup
	offsetManager: OffsetManager
	fetchManager: FetchManager
	autoOffsetReset: AutoOffsetReset
	commitOffsets: boolean
	signal?: AbortSignal
	isRunning: () => boolean
	logger: Logger
}

/**
 * Execute the cooperative rejoin loop (phase 2)
 *
 * This loop handles the iterative rejoin process during cooperative rebalance,
 * with proper backoff, retry limits, and partition cleanup.
 */
export async function executeCooperativeRejoinLoop(
	ctx: RebalanceContext,
	topics: string[],
	initialRejoinResult: JoinResult,
	callbacks: RebalanceCallbacks
): Promise<JoinResult> {
	const { consumerGroup, offsetManager, fetchManager, commitOffsets, signal, isRunning, logger } = ctx

	let rejoinResult = initialRejoinResult
	let rejoinAttempt = 0

	while (rejoinResult.needsRejoin) {
		rejoinAttempt++

		// Check max retry limit
		if (rejoinAttempt > MAX_COOPERATIVE_REJOIN_ATTEMPTS) {
			logger.error('max cooperative rejoin attempts exceeded', {
				attempts: rejoinAttempt,
				maxAttempts: MAX_COOPERATIVE_REJOIN_ATTEMPTS,
			})
			throw new Error(`Max cooperative rejoin attempts exceeded (${MAX_COOPERATIVE_REJOIN_ATTEMPTS})`)
		}

		// Backoff before rejoin (linear with cap)
		const backoffMs = Math.min(REJOIN_BACKOFF_MS * rejoinAttempt, 3000)
		logger.debug('cooperative rebalance: backoff before rejoin', {
			attempt: rejoinAttempt,
			backoffMs,
		})
		await sleep(backoffMs, { signal, resolveOnAbort: true })

		// Check if we should stop
		if (!isRunning()) {
			logger.debug('aborting rejoin - consumer is stopping')
			return rejoinResult
		}

		rejoinResult = await consumerGroup.rejoin(topics, rejoinResult.revoked)
		const groupCoordinator =
			consumerGroup.getCoordinator() ?? (await ctx.cluster.getCoordinator('GROUP', ctx.groupId))
		offsetManager.updateGroupState(
			consumerGroup.currentMemberId,
			consumerGroup.currentGenerationId,
			groupCoordinator
		)

		// If still needs rejoin, handle newly revoked partitions
		if (rejoinResult.needsRejoin) {
			if (rejoinResult.revoked.length > 0) {
				callbacks.onPartitionsRevoked(rejoinResult.revoked)
			}
			fetchManager.removePartitions(rejoinResult.revoked)
			if (commitOffsets) {
				await offsetManager.commitPartitions(rejoinResult.revoked)
			}
			offsetManager.clearPartitions(rejoinResult.revoked)
		}
	}

	return rejoinResult
}

/**
 * Add new partitions after cooperative rebalance completes
 *
 * Resolves starting offsets and adds partitions to the fetch manager.
 */
export async function addNewPartitions(
	ctx: RebalanceContext,
	addedPartitions: TopicPartition[],
	callbacks: RebalanceCallbacks
): Promise<void> {
	if (addedPartitions.length === 0) {
		return
	}

	const { cluster, groupId, consumerGroup, offsetManager, fetchManager, autoOffsetReset } = ctx

	const coordinator = await cluster.getCoordinator('GROUP', groupId)
	offsetManager.updateGroupState(consumerGroup.currentMemberId, consumerGroup.currentGenerationId, coordinator)

	// Fetch committed offsets for new partitions
	const committedOffsets = await offsetManager.fetchCommittedOffsets(addedPartitions)

	// Resolve starting offsets for new partitions
	const newPartitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []
	for (const tp of addedPartitions) {
		const offset = await offsetManager.resolveStartingOffset(
			tp.topic,
			tp.partition,
			autoOffsetReset,
			committedOffsets
		)
		newPartitionsWithOffsets.push({ ...tp, offset })
	}

	// Add new partitions to fetch manager
	fetchManager.addPartitions(newPartitionsWithOffsets)

	// Emit partitionsAssigned for new partitions
	callbacks.onPartitionsAssigned(addedPartitions)
}

/**
 * Handle cooperative rebalance phase 1 - revoke partitions
 */
export async function handleCooperativePhase1(
	ctx: RebalanceContext,
	revokedPartitions: TopicPartition[],
	callbacks: RebalanceCallbacks
): Promise<void> {
	const { offsetManager, fetchManager, commitOffsets } = ctx

	// Emit partitionsRevoked before committing
	if (revokedPartitions.length > 0) {
		callbacks.onPartitionsRevoked(revokedPartitions)
	}

	// Stop fetching revoked partitions while kept partitions continue
	fetchManager.removePartitions(revokedPartitions)

	// Commit offsets for revoked partitions only
	if (commitOffsets) {
		await offsetManager.commitPartitions(revokedPartitions)
	}

	// Clear revoked partitions from offset tracking
	offsetManager.clearPartitions(revokedPartitions)
}

/**
 * Update group state after rejoin
 */
export async function updateGroupStateAfterRejoin(ctx: RebalanceContext): Promise<void> {
	const { cluster, groupId, consumerGroup, offsetManager } = ctx
	const groupCoordinator = consumerGroup.getCoordinator() ?? (await cluster.getCoordinator('GROUP', groupId))
	offsetManager.updateGroupState(consumerGroup.currentMemberId, consumerGroup.currentGenerationId, groupCoordinator)
}

/**
 * Handle eager rebalance for standard run modes (runEach/runBatch)
 */
export async function handleEagerRebalance(
	ctx: RebalanceContext,
	subscriptions: TopicSubscription<unknown, unknown>[],
	previousAssignment: TopicPartition[],
	callbacks: RebalanceCallbacks,
	setupPartitions: (assignment: TopicPartition[]) => Promise<void>
): Promise<void> {
	const { consumerGroup, offsetManager, fetchManager, commitOffsets, isRunning, logger } = ctx

	const topics = subscriptions.map(s => s.topic)

	logger.info('eager rebalance: stopping all partitions')

	// Emit partitionsRevoked for all previously-owned partitions
	if (previousAssignment.length > 0) {
		callbacks.onPartitionsRevoked(previousAssignment)
	}

	// Stop fetching without stopping the fetch manager
	fetchManager.setPartitions([])

	// Commit pending offsets before rebalance
	if (commitOffsets) {
		await offsetManager.commitPendingOffsets()
	}

	// Clear consumed offsets
	offsetManager.clearConsumedOffsets()

	// Rejoin group to get new assignment
	const rejoinResult = await consumerGroup.rejoin(topics)

	// Abort if we were stopped while waiting for rejoin
	if (!isRunning()) {
		logger.debug('aborting eager rebalance - consumer is stopping')
		return
	}

	// Update group state
	await updateGroupStateAfterRejoin(ctx)

	// Set up new partitions
	await setupPartitions(rejoinResult.assignment)

	// Emit partitionsAssigned for new assignment
	if (rejoinResult.assignment.length > 0) {
		callbacks.onPartitionsAssigned(rejoinResult.assignment)
	}
}

/**
 * Handle cooperative rebalance for standard run modes (runEach/runBatch)
 */
export async function handleCooperativeRebalance(
	ctx: RebalanceContext,
	subscriptions: TopicSubscription<unknown, unknown>[],
	callbacks: RebalanceCallbacks
): Promise<void> {
	const { consumerGroup, isRunning, logger } = ctx

	const topics = subscriptions.map(s => s.topic)

	// First rejoin to learn what must be revoked
	let rejoinResult = await consumerGroup.rejoin(topics)

	// Abort if we were stopped while waiting for rejoin
	if (!isRunning()) {
		logger.debug('aborting rebalance - consumer is stopping')
		return
	}

	// Update group state
	await updateGroupStateAfterRejoin(ctx)

	if (rejoinResult.protocol === 'cooperative') {
		logger.info('cooperative rebalance: phase 1 - revoking partitions', {
			revokedCount: rejoinResult.revoked.length,
			keptCount: rejoinResult.kept.length,
		})

		// Phase 1: Handle revoked partitions
		await handleCooperativePhase1(ctx, rejoinResult.revoked, callbacks)

		// Phase 2: Rejoin loop until no more rejoin needed
		rejoinResult = await executeCooperativeRejoinLoop(ctx, topics, rejoinResult, callbacks)

		logger.info('cooperative rebalance: phase 2 complete - applying final assignment', {
			assignmentCount: rejoinResult.assignment.length,
			addedCount: rejoinResult.added.length,
		})

		// Final cleanup of revoked partitions
		ctx.fetchManager.removePartitions(rejoinResult.revoked)

		// Add new partitions
		await addNewPartitions(ctx, rejoinResult.added, callbacks)

		// Update group state for kept partitions
		await updateGroupStateAfterRejoin(ctx)
	}
}
