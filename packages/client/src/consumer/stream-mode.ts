/**
 * Stream mode for async iterator consumption
 *
 * Provides async generator functionality for consuming messages
 * one at a time via for-await-of loops.
 */

import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	executeCooperativeRejoinLoop,
	addNewPartitions,
	handleCooperativePhase1,
	updateGroupStateAfterRejoin,
	type RebalanceContext,
	type RebalanceCallbacks,
} from './rebalance-handler.js'
import { StreamRecordProcessor, createFetchCallback, type FetchCallback } from './record-processor.js'
import type { ConsumerGroup } from './consumer-group.js'
import type { OffsetManager } from './offset-manager.js'
import type { FetchManager } from './fetch-manager.js'
import type { TopicSubscription, Message, ConsumeContext, TopicPartition, AutoOffsetReset } from './types.js'
import { DEFAULT_RUN_EACH_OPTIONS } from './types.js'
import type { Logger } from '@/logger.js'
import type { Cluster } from '@/client/cluster.js'

/**
 * Configuration for stream mode
 */
export interface StreamConfig {
	cluster: Cluster
	groupId: string
	autoOffsetReset: AutoOffsetReset
	logger: Logger
}

/**
 * Components needed for stream mode
 */
export interface StreamComponents {
	consumerGroup: ConsumerGroup
	offsetManager: OffsetManager
	fetchManager: FetchManager
	signal: AbortSignal
}

/**
 * Callbacks for stream events
 */
export interface StreamCallbacks {
	onPartitionsAssigned: (partitions: TopicPartition[]) => void
	onPartitionsRevoked: (partitions: TopicPartition[]) => void
	onPartitionsLost: (partitions: TopicPartition[]) => void
	onError: (error: Error) => void
	onRunning: () => void
	onRebalance: () => void
	isRunning: () => boolean
	setupPartitions: (assignment: TopicPartition[]) => Promise<void>
}

/**
 * Handle stream rebalance event
 */
export async function handleStreamRebalance(
	ctx: RebalanceContext,
	subscriptions: TopicSubscription<unknown, unknown>[],
	callbacks: RebalanceCallbacks,
	streamFetchCallback: FetchCallback,
	currentFetchPromise: Promise<void> | null,
	setFetchPromise: (promise: Promise<void>) => void,
	setupPartitions: (assignment: TopicPartition[]) => Promise<void>
): Promise<void> {
	const topics = subscriptions.map(s => s.topic)

	// Rejoin group to get JoinResult with protocol info
	const rejoinResult = await ctx.consumerGroup.rejoin(topics)
	await updateGroupStateAfterRejoin(ctx)

	// Handle cooperative rebalance
	if (rejoinResult.protocol === 'cooperative') {
		ctx.logger.info('stream: cooperative rebalance - phase 1')

		// Phase 1: Handle revoked partitions
		await handleCooperativePhase1(ctx, rejoinResult.revoked, callbacks)

		// Phase 2: Rejoin loop until no more rejoin needed
		const finalResult = await executeCooperativeRejoinLoop(ctx, topics, rejoinResult, callbacks)

		// Final cleanup and add new partitions
		ctx.fetchManager.removePartitions(finalResult.revoked)
		await addNewPartitions(ctx, finalResult.added, callbacks)
		await updateGroupStateAfterRejoin(ctx)
	} else {
		// Eager rebalance - stream mode specific handling
		const currentAssignment = ctx.consumerGroup.currentAssignment
		if (currentAssignment.length > 0) {
			callbacks.onPartitionsRevoked(currentAssignment)
		}

		ctx.fetchManager.stop()
		await currentFetchPromise?.catch(err => {
			ctx.logger.error('fetch error during rebalance', { error: (err as Error).message })
		})

		if (ctx.commitOffsets) {
			await ctx.offsetManager.commitPendingOffsets()
		}
		ctx.offsetManager.clearConsumedOffsets()
		await setupPartitions(rejoinResult.assignment)
		setFetchPromise(ctx.fetchManager.start(streamFetchCallback))

		if (rejoinResult.assignment.length > 0) {
			callbacks.onPartitionsAssigned(rejoinResult.assignment)
		}
	}
}

/**
 * Set up stream event handlers
 */
export function setupStreamEventHandlers(
	components: StreamComponents,
	callbacks: StreamCallbacks,
	setPendingRebalance: (value: boolean) => void
): void {
	const { consumerGroup, offsetManager, fetchManager } = components

	consumerGroup.on('rebalance', () => {
		setPendingRebalance(true)
		callbacks.onRebalance()
	})

	consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
		offsetManager.clearPartitions(partitions)
		fetchManager.removePartitions(partitions)
		callbacks.onPartitionsLost(partitions)
	})

	consumerGroup.on('error', (error: Error) => {
		const err = error instanceof Error ? error : new Error(String(error))
		callbacks.onError(err)

		if (err instanceof KafkaProtocolError && err.errorCode === ErrorCode.FencedInstanceId) {
			// Signal to stop - caller handles this
		}
	})
}

/**
 * Run the stream message loop
 */
export async function* runStreamLoop<V, K>(
	config: StreamConfig,
	components: StreamComponents,
	subscriptions: TopicSubscription<unknown, unknown>[],
	callbacks: StreamCallbacks
): AsyncGenerator<{ message: Message<V, K>; ctx: ConsumeContext }> {
	const { consumerGroup, offsetManager, fetchManager, signal } = components
	const topics = subscriptions.map(s => s.topic)

	// Set up rebalance tracking
	let pendingRebalance = false
	setupStreamEventHandlers(components, callbacks, value => {
		pendingRebalance = value
	})

	// Join group
	const joinResult = await consumerGroup.join(topics)
	await callbacks.setupPartitions(joinResult.assignment)

	if (joinResult.assignment.length > 0) {
		callbacks.onPartitionsAssigned(joinResult.assignment)
	}
	callbacks.onRunning()

	// Message queue for yielding
	const messageQueue: Array<{ message: Message<unknown, unknown>; ctx: ConsumeContext }> = []
	let resolveNext: (() => void) | null = null
	const wakeUp = () => {
		if (resolveNext) {
			resolveNext()
			resolveNext = null
		}
	}

	// Create stream processor and fetch callback using shared infrastructure
	const processor = new StreamRecordProcessor(messageQueue, offsetManager, wakeUp)
	const streamFetchCallback = createFetchCallback(subscriptions, processor, signal, callbacks.isRunning)

	// Track current fetch promise
	let currentFetchPromise: Promise<void> | null = null

	// Start fetch in background
	currentFetchPromise = fetchManager.start(streamFetchCallback)

	// Start auto-commit
	offsetManager.startAutoCommit(DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs)

	// Create rebalance context
	const createRebalanceContext = (): RebalanceContext => ({
		cluster: config.cluster,
		groupId: config.groupId,
		consumerGroup,
		offsetManager,
		fetchManager,
		autoOffsetReset: config.autoOffsetReset,
		commitOffsets: true,
		signal,
		isRunning: callbacks.isRunning,
		logger: config.logger,
	})

	const rebalanceCallbacks: RebalanceCallbacks = {
		onPartitionsRevoked: callbacks.onPartitionsRevoked,
		onPartitionsAssigned: callbacks.onPartitionsAssigned,
	}

	// Yield messages
	while (callbacks.isRunning() && !signal.aborted) {
		// Handle rebalance
		if (pendingRebalance) {
			pendingRebalance = false
			await handleStreamRebalance(
				createRebalanceContext(),
				subscriptions,
				rebalanceCallbacks,
				streamFetchCallback,
				currentFetchPromise,
				newPromise => {
					currentFetchPromise = newPromise
				},
				callbacks.setupPartitions
			)
		}

		// Wait for message
		if (messageQueue.length === 0) {
			await new Promise<void>(resolve => {
				resolveNext = resolve
				setTimeout(resolve, 100)
			})
		}

		// Yield all queued messages
		while (messageQueue.length > 0) {
			const item = messageQueue.shift()!
			yield item as { message: Message<V, K>; ctx: ConsumeContext }
		}
	}

	// Clean up fetch
	fetchManager.stop()
	await currentFetchPromise?.catch(err => {
		config.logger.error('fetch error during cleanup', { error: (err as Error).message })
	})
}
