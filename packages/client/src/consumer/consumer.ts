/**
 * High-level Kafka consumer
 *
 * Main public API for consuming messages from Kafka.
 * Supports single message, batch, and stream modes.
 */

import { EventEmitter } from 'node:events'
import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { CoordinatorNotAvailableError, KafkaProtocolError, NotCoordinatorError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { DecodedRecord, RecordHeader } from '@/protocol/records/index.js'
import type {
	ConsumerConfig,
	ResolvedConsumerConfig,
	ConsumerEvents,
	TopicSubscription,
	SubscriptionInput,
	MsgOf,
	KeyOf,
	Message,
	ConsumeContext,
	MessageHandler,
	BatchHandler,
	RunEachOptions,
	RunBatchOptions,
	TopicPartition,
	ManualAssignment,
	ConsumerTraceFn,
} from './types.js'
import {
	DEFAULT_CONSUMER_CONFIG,
	DEFAULT_RUN_EACH_OPTIONS,
	DEFAULT_RUN_BATCH_OPTIONS,
	normalizeDecoder,
	toTopicSubscription,
	type SubscriptionLike,
} from './types.js'
import { ConsumerGroup } from './consumer-group.js'
import { OffsetManager } from './offset-manager.js'
import { FetchManager } from './fetch-manager.js'
import { noopLogger, type Logger } from '@/logger.js'
import { sleep } from '@/utils/sleep.js'
import { retry } from '@/utils/retry.js'

// Shared empty objects for common cases (optimization)
const EMPTY_BUFFER = Buffer.alloc(0)
const EMPTY_HEADERS: Record<string, Buffer> = Object.freeze({}) as Record<string, Buffer>

/**
 * Convert RecordHeader[] to Record<string, Buffer>
 */
function convertHeaders(headers: RecordHeader[]): Record<string, Buffer> {
	if (headers.length === 0) {
		return EMPTY_HEADERS
	}
	const result: Record<string, Buffer> = {}
	for (const h of headers) {
		if (h.value !== null) {
			result[h.key] = h.value
		}
	}
	return result
}

/**
 * Cooperative rebalance retry limits
 */
const MAX_COOPERATIVE_REJOIN_ATTEMPTS = 5
const REJOIN_BACKOFF_MS = 500

/**
 * Consumer state
 */
type ConsumerState = 'idle' | 'running' | 'stopping'

/**
 * Callback type for processing fetched records
 * Uses DecodedRecord directly to avoid intermediate Message object creation
 */
type FetchCallback = (topic: string, partition: number, records: DecodedRecord[]) => Promise<void>

/**
 * High-level Kafka consumer
 */
export class Consumer extends EventEmitter<ConsumerEvents> {
	private readonly cluster: Cluster
	private readonly config: ResolvedConsumerConfig
	private readonly logger: Logger
	private readonly trace?: ConsumerTraceFn

	private state: ConsumerState = 'idle'
	private commitOffsets = true
	private sessionLost = false
	private consumerGroup: ConsumerGroup | null = null
	private offsetManager: OffsetManager | null = null
	private fetchManager: FetchManager | null = null
	private fetchLoopPromise: Promise<void> | null = null
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runPromiseReject: ((error: Error) => void) | null = null
	private fetchCallback: FetchCallback | null = null
	private rebalanceRunner: Promise<void> | null = null
	private rebalanceRequested = false

	private async getCoordinatorWithRetry(type: 'GROUP' | 'TRANSACTION', key: string): Promise<Broker> {
		const signal = this.abortController?.signal
		try {
			return await retry(() => this.cluster.getCoordinator(type, key), {
				maxAttempts: 10,
				initialDelayMs: 100,
				maxDelayMs: 1_000,
				multiplier: 2,
				jitter: 0,
				signal,
				resolveOnAbort: true,
				shouldRetry: error => {
					if (signal?.aborted) {
						return false
					}
					return error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError
				},
				onRetry: ({ attempt, delayMs, error }) => {
					this.logger.debug('coordinator lookup failed, retrying', {
						type,
						key,
						attempt,
						delayMs,
						error: error instanceof Error ? error.message : String(error),
					})
				},
			})
		} catch (error) {
			if (signal?.aborted) {
				throw error
			}
			if (error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError) {
				throw new CoordinatorNotAvailableError(type, key)
			}
			throw error
		}
	}

	constructor(cluster: Cluster, config: ConsumerConfig) {
		super()
		this.cluster = cluster
		this.logger = cluster.getLogger()?.child({ component: 'consumer', groupId: config.groupId }) ?? noopLogger
		this.trace = config.trace
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
	}

	private startRun(commitOffsets: boolean, externalSignal?: AbortSignal): void {
		if (this.state !== 'idle') {
			throw new Error('Consumer is already running. Call stop() first.')
		}

		this.state = 'running'
		this.abortController = new AbortController()
		this.commitOffsets = commitOffsets
		this.sessionLost = false
		this.rebalanceRequested = false
		this.rebalanceRunner = null

		// Set up external abort signal
		if (externalSignal) {
			externalSignal.addEventListener('abort', () => {
				this.stop()
			})
		}
	}

	private async finalizeRun(options?: { logCleanup?: boolean; logStop?: boolean }): Promise<void> {
		const { logCleanup = true, logStop = true } = options ?? {}

		if (logCleanup) {
			this.logger.debug('consumer run loop ended, cleaning up')
		}
		await this.cleanup()
		this.state = 'idle'

		if (logStop) {
			this.logger.info('consumer stopped')
		}
		this.emit('stopped')
	}

	private getSubscriptionsAndTopics(subscription: SubscriptionInput): {
		subscriptions: TopicSubscription<unknown, unknown>[]
		topics: string[]
	} {
		const subscriptions = this.normalizeSubscription(subscription)
		return {
			subscriptions,
			topics: subscriptions.map(s => s.topic),
		}
	}

	private normalizeAndValidateManualAssignment(assignment: ManualAssignment[], topics: string[]): ManualAssignment[] {
		const normalized = this.normalizeManualAssignment(assignment)

		const subscriptionTopics = new Set(topics)
		for (const tp of normalized) {
			if (!subscriptionTopics.has(tp.topic)) {
				throw new Error(`Manual assignment includes topic "${tp.topic}" that is not in the subscription`)
			}
		}

		return normalized
	}

	private initGroupComponents(partitionConcurrency: number, logger?: Logger): void {
		this.consumerGroup = new ConsumerGroup(this.cluster, this.config, logger)
		this.offsetManager = new OffsetManager(this.cluster, this.config.groupId, this.config.groupInstanceId, logger)
		this.fetchManager = new FetchManager(
			this.cluster,
			this.offsetManager,
			this.config.autoOffsetReset,
			{
				maxBytesPerPartition: this.config.maxBytesPerPartition,
				minBytes: this.config.minBytes,
				maxWaitMs: this.config.maxWaitMs,
				partitionConcurrency,
				isolationLevel: this.config.isolationLevel,
				trace: this.trace,
			},
			logger
		)
	}

	private initManualAssignmentComponents(partitionConcurrency: number, logger?: Logger): void {
		this.offsetManager = new OffsetManager(this.cluster, this.config.groupId, undefined, logger)
		this.fetchManager = new FetchManager(
			this.cluster,
			this.offsetManager,
			this.config.autoOffsetReset,
			{
				maxBytesPerPartition: this.config.maxBytesPerPartition,
				minBytes: this.config.minBytes,
				maxWaitMs: this.config.maxWaitMs,
				partitionConcurrency,
				isolationLevel: this.config.isolationLevel,
				trace: this.trace,
			},
			logger
		)
	}

	private attachGroupEventHandlers(subscriptions: TopicSubscription<unknown, unknown>[]): void {
		const consumerGroup = this.consumerGroup
		if (!consumerGroup) {
			return
		}

		// Handle rebalance
		consumerGroup.on('rebalance', () => {
			this.logger.debug('rebalance event received')
			this.emit('rebalance')
			this.rebalanceRequested = true
			if (this.rebalanceRunner) {
				return
			}

			this.rebalanceRunner = (async () => {
				while (this.rebalanceRequested && this.state === 'running') {
					this.rebalanceRequested = false
					await this.handleRebalance(subscriptions)
				}
			})()
				.catch(error => {
					const err = error instanceof Error ? error : new Error(String(error))
					this.logger.error('rebalance handler failed', { error: err.message })
					if (this.runPromiseReject) {
						this.runPromiseReject(err)
						this.runPromiseReject = null
						this.runPromiseResolve = null
					} else {
						this.emitError(err)
					}
					this.stop()
				})
				.finally(() => {
					this.rebalanceRunner = null
				})
		})

		consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
			this.logger.info('session lost, partitions cannot be committed', { partitionCount: partitions.length })
			this.sessionLost = true
			this.offsetManager?.clearPartitions(partitions)
			this.fetchManager?.removePartitions(partitions)
			this.emit('partitionsLost', partitions)
		})

		consumerGroup.on('error', (error: Error) => {
			const err = error instanceof Error ? error : new Error(String(error))

			if (err instanceof KafkaProtocolError && err.errorCode === ErrorCode.FencedInstanceId) {
				this.emitError(err)
				if (this.runPromiseReject) {
					this.runPromiseReject(err)
					this.runPromiseReject = null
					this.runPromiseResolve = null
				}
				this.stop()
				return
			}

			this.emitError(err)
		})
	}

	private async startFetchLoopAndWait(startFetchLoop: () => Promise<void>): Promise<void> {
		await new Promise<void>((resolve, reject) => {
			this.runPromiseResolve = resolve
			this.runPromiseReject = reject

			this.fetchLoopPromise = startFetchLoop()
				.then(() => {
					if (this.runPromiseResolve) {
						this.runPromiseResolve()
						this.runPromiseResolve = null
						this.runPromiseReject = null
					}
				})
				.catch(error => {
					const err = error instanceof Error ? error : new Error(String(error))
					if (this.runPromiseReject) {
						this.runPromiseReject(err)
						this.runPromiseReject = null
						this.runPromiseResolve = null
					} else {
						this.emitError(err)
					}
				})
		})
	}

	private async startManualAssignmentRun(
		mode: 'each' | 'batch',
		subscriptions: TopicSubscription<unknown, unknown>[],
		topics: string[],
		manualAssignment: ManualAssignment[],
		partitionConcurrency: number,
		startFetchLoop: () => Promise<void>
	): Promise<void> {
		this.logger.info('starting consumer', {
			topics,
			mode,
			assignmentMode: 'manual',
			assignedPartitionCount: manualAssignment.length,
		})

		// Manual assignment mode: no group join/rebalance.
		// Use an OffsetManager without groupInstanceId since we don't have generation/member metadata.
		this.initManualAssignmentComponents(partitionConcurrency, this.logger)

		const offsetManager = this.offsetManager
		const fetchManager = this.fetchManager
		if (!offsetManager || !fetchManager) {
			return
		}

		const assignment: TopicPartition[] = manualAssignment.map(tp => ({ topic: tp.topic, partition: tp.partition }))

		// Coordinator is needed for offset fetch/commit (even without group membership)
		const coordinator = await this.getCoordinatorWithRetry('GROUP', this.config.groupId)
		offsetManager.updateGroupState('', -1, coordinator)

		// Fetch committed offsets for the assigned partitions
		const committedOffsets = await offsetManager.fetchCommittedOffsets(assignment)

		// Resolve starting offsets
		const partitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []
		for (const tp of manualAssignment) {
			const offset =
				tp.offset !== undefined
					? tp.offset
					: await offsetManager.resolveStartingOffset(
							tp.topic,
							tp.partition,
							this.config.autoOffsetReset,
							committedOffsets
						)
			partitionsWithOffsets.push({ topic: tp.topic, partition: tp.partition, offset })
		}

		fetchManager.setPartitions(partitionsWithOffsets)

		if (assignment.length > 0) {
			this.emit('partitionsAssigned', assignment)
		}
		this.emit('running')

		await this.startFetchLoopAndWait(startFetchLoop)
	}

	private async startGroupRun(
		mode: 'each' | 'batch',
		subscriptions: TopicSubscription<unknown, unknown>[],
		topics: string[],
		partitionConcurrency: number,
		startFetchLoop: () => Promise<void>
	): Promise<void> {
		this.logger.info('starting consumer', { topics, mode })

		// Create components
		this.initGroupComponents(partitionConcurrency, this.logger)
		this.attachGroupEventHandlers(subscriptions)

		this.logger.debug('joining group', { topics })
		const joinResult = await this.consumerGroup!.join(topics)
		this.logger.info('group joined', { assignedPartitions: joinResult.assignment.length })
		await this.setupPartitions(subscriptions, joinResult.assignment)

		if (joinResult.assignment.length > 0) {
			this.emit('partitionsAssigned', joinResult.assignment)
		}

		this.emit('running')

		await this.startFetchLoopAndWait(startFetchLoop)
	}

	/**
	 * Check if consumer is running
	 */
	get isRunning(): boolean {
		return this.state === 'running'
	}

	/**
	 * Pause fetching from specific partitions (backpressure control).
	 *
	 * While paused, the consumer remains in the group and continues heartbeating,
	 * but it will not fetch new records from the paused partitions until resumed.
	 */
	pause(partitions: TopicPartition[]): void {
		if (this.state !== 'running' || !this.fetchManager) {
			throw new Error('Consumer is not running')
		}
		this.fetchManager.pausePartitions(partitions)
	}

	/**
	 * Resume fetching from specific partitions after a pause().
	 */
	resume(partitions: TopicPartition[]): void {
		if (this.state !== 'running' || !this.fetchManager) {
			throw new Error('Consumer is not running')
		}
		this.fetchManager.resumePartitions(partitions)
	}

	/**
	 * Run the consumer loop processing one message at a time
	 *
	 * - One consumer → one run. Calling runEach() again while active throws.
	 * - Resolves when stop() is called, options.signal aborts, or a fatal error occurs.
	 */
	async runEach<S extends SubscriptionInput>(
		subscription: S,
		handler: MessageHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunEachOptions
	): Promise<void> {
		const opts = {
			...DEFAULT_RUN_EACH_OPTIONS,
			...options,
		}
		this.startRun(opts.commitOffsets !== false, options?.signal)

		try {
			const manualAssignment = opts.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			if (manualAssignment) {
				const normalized = this.normalizeAndValidateManualAssignment(manualAssignment, topics)
				await this.startManualAssignmentRun(
					'each',
					subscriptions,
					topics,
					normalized,
					opts.partitionConcurrency ?? DEFAULT_RUN_EACH_OPTIONS.partitionConcurrency,
					() => this.startFetchLoopEach(subscriptions, handler as MessageHandler<unknown, unknown>, opts)
				)
				return
			}

			await this.startGroupRun(
				'each',
				subscriptions,
				topics,
				opts.partitionConcurrency ?? DEFAULT_RUN_EACH_OPTIONS.partitionConcurrency,
				() => this.startFetchLoopEach(subscriptions, handler as MessageHandler<unknown, unknown>, opts)
			)
		} catch (error) {
			this.emitError(error)
			throw error
		} finally {
			await this.finalizeRun()
		}
	}

	/**
	 * Run the consumer loop processing messages in batches
	 *
	 * - One consumer → one run. Calling runBatch() again while active throws.
	 * - Resolves when stop() is called, options.signal aborts, or a fatal error occurs.
	 * - Offsets are only marked as consumed after the batch handler completes successfully.
	 */
	async runBatch<S extends SubscriptionInput>(
		subscription: S,
		handler: BatchHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunBatchOptions
	): Promise<void> {
		const opts = {
			...DEFAULT_RUN_BATCH_OPTIONS,
			...options,
		}
		this.startRun(opts.commitOffsets !== false, options?.signal)

		try {
			const manualAssignment = opts.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			if (manualAssignment) {
				const normalized = this.normalizeAndValidateManualAssignment(manualAssignment, topics)
				await this.startManualAssignmentRun(
					'batch',
					subscriptions,
					topics,
					normalized,
					opts.partitionConcurrency ?? DEFAULT_RUN_BATCH_OPTIONS.partitionConcurrency,
					() =>
						this.startFetchLoopBatch(
							subscriptions,
							handler as BatchHandler<unknown, unknown>,
							opts as Required<Pick<RunBatchOptions, 'maxBatchSize' | 'maxBatchWaitMs'>> & RunBatchOptions
						)
				)
				return
			}

			await this.startGroupRun(
				'batch',
				subscriptions,
				topics,
				opts.partitionConcurrency ?? DEFAULT_RUN_BATCH_OPTIONS.partitionConcurrency,
				() => this.startFetchLoopBatch(subscriptions, handler as BatchHandler<unknown, unknown>, opts)
			)
		} catch (error) {
			this.emitError(error)
			throw error
		} finally {
			await this.finalizeRun()
		}
	}

	/**
	 * Async iterator mode
	 *
	 * - Also single-run: calling stream() during run() throws.
	 * - Returns one message at a time.
	 */
	async *stream<S extends SubscriptionInput>(
		subscription: S
	): AsyncIterable<{ message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }> {
		this.startRun(true)

		try {
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			// Create components (no internal component logging by default in stream mode)
			this.initGroupComponents(1, undefined)
			const consumerGroup = this.consumerGroup
			const offsetManager = this.offsetManager
			const fetchManager = this.fetchManager
			const signal = this.abortController!.signal
			if (!consumerGroup || !offsetManager || !fetchManager) {
				return
			}

			// Handle rebalance
			let pendingRebalance = false
			consumerGroup.on('rebalance', () => {
				pendingRebalance = true
				this.emit('rebalance')
			})

			consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
				this.logger.info('session lost, partitions cannot be committed', { partitionCount: partitions.length })
				this.sessionLost = true
				this.offsetManager?.clearPartitions(partitions)
				this.fetchManager?.removePartitions(partitions)
				this.emit('partitionsLost', partitions)
			})

			consumerGroup.on('error', (error: Error) => {
				const err = error instanceof Error ? error : new Error(String(error))
				this.emitError(err)

				if (err instanceof KafkaProtocolError && err.errorCode === ErrorCode.FencedInstanceId) {
					this.stop()
				}
			})

			const joinResult = await consumerGroup.join(topics)
			await this.setupPartitions(subscriptions, joinResult.assignment)

			// Emit partitionsAssigned for initial join
			if (joinResult.assignment.length > 0) {
				this.emit('partitionsAssigned', joinResult.assignment)
			}

			this.emit('running')

			// Message queue for yielding
			const messageQueue: Array<{ message: Message<unknown, unknown>; ctx: ConsumeContext }> = []
			let resolveNext: (() => void) | null = null

			// Pre-compute decoder maps for O(1) lookup
			const decoderMap = new Map<string, (buf: Buffer) => unknown>()
			const keyDecoderMap = new Map<string, (buf: Buffer) => unknown>()
			for (const sub of subscriptions) {
				decoderMap.set(sub.topic, normalizeDecoder(sub.decoder))
				if (sub.keyDecoder) {
					keyDecoderMap.set(sub.topic, normalizeDecoder(sub.keyDecoder))
				}
			}
			const defaultDecoder = (b: Buffer) => b

			// Create fetch callback that can be reused after rebalance
			const streamFetchCallback: FetchCallback = (topic, partition, records) => {
				const decoder = decoderMap.get(topic) ?? defaultDecoder
				const keyDecoder = keyDecoderMap.get(topic) ?? defaultDecoder

				for (const record of records) {
					// Create Message directly from DecodedRecord
					const message: Message<unknown, unknown> = {
						topic,
						partition,
						offset: record.offset,
						timestamp: record.timestamp,
						key: record.key === null ? null : keyDecoder(record.key),
						// Preserve tombstones (null values) without decoding.
						// Note: types do not currently model nullable values in Message<V, K>.
						value: record.value === null ? null : decoder(record.value ?? EMPTY_BUFFER),
						headers: convertHeaders(record.headers),
					}

					const ctx: ConsumeContext = {
						signal,
						topic,
						partition,
						offset: record.offset,
					}

					messageQueue.push({ message, ctx })

					// Mark as consumed for auto-commit
					offsetManager.markConsumed(topic, partition, record.offset)

					// Wake up iterator
					if (resolveNext) {
						resolveNext()
						resolveNext = null
					}
				}
				return Promise.resolve()
			}

			// Track current fetch promise for cleanup
			let currentFetchPromise: Promise<void> | null = null

			// Start fetch in background
			currentFetchPromise = fetchManager.start(streamFetchCallback)

			// Start auto-commit
			offsetManager.startAutoCommit(DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs)

			// Yield messages
			while (this.state === 'running' && !signal.aborted) {
				// Handle rebalance
				if (pendingRebalance) {
					pendingRebalance = false
					await this.handleStreamRebalance(
						subscriptions,
						streamFetchCallback,
						currentFetchPromise,
						newPromise => {
							currentFetchPromise = newPromise
						}
					)
				}

				// Wait for message
				if (messageQueue.length === 0) {
					await new Promise<void>(resolve => {
						resolveNext = resolve
						// Timeout to check for abort
						setTimeout(resolve, 100)
					})
				}

				// Yield all queued messages
				while (messageQueue.length > 0) {
					const item = messageQueue.shift()!
					yield item as { message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }
				}
			}

			// Clean up fetch
			fetchManager.stop()
			await currentFetchPromise?.catch(err => {
				this.logger.error('fetch error during cleanup', { error: (err as Error).message })
			})
		} finally {
			await this.finalizeRun({ logCleanup: false, logStop: false })
		}
	}

	/**
	 * Graceful shutdown
	 *
	 * - Idempotent
	 * - Causes run()/stream() to resolve once fully stopped
	 */
	stop(): void {
		if (this.state === 'idle' || this.state === 'stopping') {
			return
		}

		this.logger.info('stopping consumer')
		this.state = 'stopping'
		this.abortController?.abort()

		// Resolve run promise
		if (this.runPromiseResolve) {
			this.runPromiseResolve()
			this.runPromiseResolve = null
		}
	}

	/**
	 * Normalize subscription input to array
	 *
	 * Accepts topic name strings, TopicSubscription, TopicDefinition, or arrays of any of them.
	 * TopicDefinition instances are converted to TopicSubscription.
	 */
	private normalizeSubscription(input: SubscriptionInput): TopicSubscription<unknown, unknown>[] {
		const items = (Array.isArray(input) ? input : [input]) as Array<string | SubscriptionLike<unknown, unknown>>
		return items.map(item => (typeof item === 'string' ? toTopicSubscription(item) : toTopicSubscription(item)))
	}

	private normalizeManualAssignment(input: ManualAssignment[]): ManualAssignment[] {
		const byKey = new Map<string, ManualAssignment>()
		for (const tp of input) {
			const key = `${tp.topic}:${tp.partition}`
			byKey.set(key, tp)
		}
		return [...byKey.values()]
	}

	/**
	 * Set up partitions after assignment
	 */
	private async setupPartitions(
		_subscriptions: TopicSubscription<unknown, unknown>[],
		assignment: TopicPartition[]
	): Promise<void> {
		const consumerGroup = this.consumerGroup
		const offsetManager = this.offsetManager
		const fetchManager = this.fetchManager

		if (!consumerGroup || !offsetManager || !fetchManager) {
			return
		}

		// Update offset manager with group state
		const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
		offsetManager.updateGroupState(consumerGroup.currentMemberId, consumerGroup.currentGenerationId, coordinator)

		// Fetch committed offsets
		const committedOffsets = await offsetManager.fetchCommittedOffsets(assignment)

		// Resolve starting offsets
		const partitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []
		for (const tp of assignment) {
			const offset = await offsetManager.resolveStartingOffset(
				tp.topic,
				tp.partition,
				this.config.autoOffsetReset,
				committedOffsets
			)

			partitionsWithOffsets.push({ ...tp, offset })
		}

		// Update fetch manager
		fetchManager.setPartitions(partitionsWithOffsets)
	}

	/**
	 * Handle rebalance event
	 */
	private async handleRebalance(subscriptions: TopicSubscription<unknown, unknown>[]): Promise<void> {
		// Don't handle rebalance if we're stopping or stopped
		if (this.state !== 'running') {
			this.logger.debug('ignoring rebalance - consumer is stopping')
			return
		}

		if (!this.consumerGroup || !this.offsetManager || !this.fetchManager) {
			return
		}

		const consumerGroup = this.consumerGroup
		const offsetManager = this.offsetManager
		const fetchManager = this.fetchManager
		const previousAssignment = consumerGroup.currentAssignment

		const topics = subscriptions.map(s => s.topic)

		// Eager rebalance: stop-the-world. Do not wait for rejoin to revoke.
		if (consumerGroup.currentRebalanceProtocol === 'eager') {
			this.logger.info('eager rebalance: stopping all partitions')

			// Emit partitionsRevoked for all previously-owned partitions (if we had any)
			if (previousAssignment.length > 0) {
				this.emit('partitionsRevoked', previousAssignment)
			}

			// Stop fetching without stopping the fetch manager (so run() doesn't resolve)
			fetchManager.setPartitions([])

			// Commit pending offsets before rebalance (best-effort)
			if (this.commitOffsets && !this.sessionLost) {
				await offsetManager.commitPendingOffsets()
			}

			// Clear consumed offsets
			offsetManager.clearConsumedOffsets()

			// Rejoin group to get new assignment
			const rejoinResult = await consumerGroup.rejoin(topics)

			// Abort if we were stopped while waiting for rejoin
			if (this.state !== 'running') {
				this.logger.debug('aborting eager rebalance - consumer is stopping')
				return
			}

			// Rejoined successfully; commits are allowed again for new partitions/generation.
			this.sessionLost = false

			// Ensure offset commits use the latest generation/coordinator after the rejoin.
			const groupCoordinator =
				consumerGroup.getCoordinator() ?? (await this.cluster.getCoordinator('GROUP', this.config.groupId))
			offsetManager.updateGroupState(
				consumerGroup.currentMemberId,
				consumerGroup.currentGenerationId,
				groupCoordinator
			)

			// Set up new partitions
			await this.setupPartitions(subscriptions, rejoinResult.assignment)

			// Emit partitionsAssigned for new assignment
			if (rejoinResult.assignment.length > 0) {
				this.emit('partitionsAssigned', rejoinResult.assignment)
			}

			// Fetch loops will be restarted by FetchManager.setPartitions() inside setupPartitions()
			return
		}

		// Cooperative rebalance (two-phase): rejoin first to learn what must be revoked.
		let rejoinResult = await consumerGroup.rejoin(topics)

		// Abort if we were stopped while waiting for rejoin
		if (this.state !== 'running') {
			this.logger.debug('aborting rebalance - consumer is stopping')
			return
		}

		// Rejoined successfully; commits are allowed again for new partitions/generation.
		this.sessionLost = false

		// Ensure offset commits use the latest generation/coordinator after the rejoin.
		let groupCoordinator =
			consumerGroup.getCoordinator() ?? (await this.cluster.getCoordinator('GROUP', this.config.groupId))
		offsetManager.updateGroupState(
			consumerGroup.currentMemberId,
			consumerGroup.currentGenerationId,
			groupCoordinator
		)

		if (rejoinResult.protocol === 'cooperative') {
			this.logger.info('cooperative rebalance: phase 1 - revoking partitions', {
				revokedCount: rejoinResult.revoked.length,
				keptCount: rejoinResult.kept.length,
			})

			// Emit partitionsRevoked before committing (caller can do custom commits)
			if (rejoinResult.revoked.length > 0) {
				this.emit('partitionsRevoked', rejoinResult.revoked)
			}

			// Phase 1: Stop fetching revoked partitions while kept partitions continue.
			fetchManager.removePartitions(rejoinResult.revoked)

			// Commit offsets for revoked partitions only
			if (this.commitOffsets) {
				await offsetManager.commitPartitions(rejoinResult.revoked)
			}

			// Clear revoked partitions from offset tracking
			offsetManager.clearPartitions(rejoinResult.revoked)

			// Phase 2: Rejoin with revoked partitions excluded from ownership
			// Loop until no more rejoin needed (with backoff and retry limit)
			let rejoinAttempt = 0
			while (rejoinResult.needsRejoin) {
				rejoinAttempt++

				// Check max retry limit
				if (rejoinAttempt > MAX_COOPERATIVE_REJOIN_ATTEMPTS) {
					this.logger.error('max cooperative rejoin attempts exceeded', {
						attempts: rejoinAttempt,
						maxAttempts: MAX_COOPERATIVE_REJOIN_ATTEMPTS,
					})
					throw new Error(`Max cooperative rejoin attempts exceeded (${MAX_COOPERATIVE_REJOIN_ATTEMPTS})`)
				}

				// Backoff before rejoin (linear with cap)
				const backoffMs = Math.min(REJOIN_BACKOFF_MS * rejoinAttempt, 3000)
				this.logger.debug('cooperative rebalance: backoff before rejoin', {
					attempt: rejoinAttempt,
					backoffMs,
				})
				await sleep(backoffMs, { signal: this.abortController?.signal, resolveOnAbort: true })

				// Check if we should stop
				if (this.state !== 'running') {
					this.logger.debug('aborting rejoin - consumer is stopping')
					return
				}

				rejoinResult = await consumerGroup.rejoin(topics, rejoinResult.revoked)
				groupCoordinator = consumerGroup.getCoordinator() ?? groupCoordinator
				offsetManager.updateGroupState(
					consumerGroup.currentMemberId,
					consumerGroup.currentGenerationId,
					groupCoordinator
				)

				// If still needs rejoin, continue the loop
				if (rejoinResult.needsRejoin) {
					// Emit for newly revoked partitions
					if (rejoinResult.revoked.length > 0) {
						this.emit('partitionsRevoked', rejoinResult.revoked)
					}
					// Stop fetching any newly revoked partitions
					fetchManager.removePartitions(rejoinResult.revoked)
					if (this.commitOffsets) {
						await offsetManager.commitPartitions(rejoinResult.revoked)
					}
					offsetManager.clearPartitions(rejoinResult.revoked)
				}
			}

			this.logger.info('cooperative rebalance: phase 2 complete - applying final assignment', {
				assignmentCount: rejoinResult.assignment.length,
				addedCount: rejoinResult.added.length,
			})

			// Final assignment: incrementally update partitions
			// Remove revoked partitions
			fetchManager.removePartitions(rejoinResult.revoked)

			// Add new partitions (need to resolve their offsets first)
			if (rejoinResult.added.length > 0) {
				const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
				offsetManager.updateGroupState(
					consumerGroup.currentMemberId,
					consumerGroup.currentGenerationId,
					coordinator
				)

				// Fetch committed offsets for new partitions
				const committedOffsets = await offsetManager.fetchCommittedOffsets(rejoinResult.added)

				// Resolve starting offsets for new partitions
				const newPartitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []
				for (const tp of rejoinResult.added) {
					const offset = await offsetManager.resolveStartingOffset(
						tp.topic,
						tp.partition,
						this.config.autoOffsetReset,
						committedOffsets
					)
					newPartitionsWithOffsets.push({ ...tp, offset })
				}

				// Add new partitions to fetch manager (will start fetch loops if running)
				fetchManager.addPartitions(newPartitionsWithOffsets)

				// Emit partitionsAssigned for new partitions
				this.emit('partitionsAssigned', rejoinResult.added)
			}

			// Update group state for kept partitions
			const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
			offsetManager.updateGroupState(
				consumerGroup.currentMemberId,
				consumerGroup.currentGenerationId,
				coordinator
			)
		}
	}

	/**
	 * Handle rebalance for stream mode
	 */
	private async handleStreamRebalance(
		subscriptions: TopicSubscription<unknown, unknown>[],
		streamFetchCallback: FetchCallback,
		currentFetchPromise: Promise<void> | null,
		setFetchPromise: (promise: Promise<void>) => void
	): Promise<void> {
		if (!this.consumerGroup || !this.offsetManager || !this.fetchManager) {
			return
		}

		const topics = subscriptions.map(s => s.topic)

		// Rejoin group to get JoinResult with protocol info
		let rejoinResult = await this.consumerGroup.rejoin(topics)
		let groupCoordinator =
			this.consumerGroup.getCoordinator() ?? (await this.cluster.getCoordinator('GROUP', this.config.groupId))
		this.offsetManager.updateGroupState(
			this.consumerGroup.currentMemberId,
			this.consumerGroup.currentGenerationId,
			groupCoordinator
		)

		// Handle cooperative rebalance (two-phase)
		if (rejoinResult.protocol === 'cooperative') {
			this.logger.info('stream: cooperative rebalance - phase 1')

			// Emit partitionsRevoked before committing
			if (rejoinResult.revoked.length > 0) {
				this.emit('partitionsRevoked', rejoinResult.revoked)
			}

			// Phase 1: Stop fetching revoked partitions while kept partitions continue.
			this.fetchManager.removePartitions(rejoinResult.revoked)
			if (this.commitOffsets) {
				await this.offsetManager.commitPartitions(rejoinResult.revoked)
			}
			this.offsetManager.clearPartitions(rejoinResult.revoked)

			// Phase 2: Rejoin with revoked partitions excluded (with backoff and retry limit)
			let rejoinAttempt = 0
			while (rejoinResult.needsRejoin) {
				rejoinAttempt++

				// Check max retry limit
				if (rejoinAttempt > MAX_COOPERATIVE_REJOIN_ATTEMPTS) {
					this.logger.error('stream: max cooperative rejoin attempts exceeded', {
						attempts: rejoinAttempt,
						maxAttempts: MAX_COOPERATIVE_REJOIN_ATTEMPTS,
					})
					throw new Error(`Max cooperative rejoin attempts exceeded (${MAX_COOPERATIVE_REJOIN_ATTEMPTS})`)
				}

				// Backoff before rejoin (linear with cap)
				const backoffMs = Math.min(REJOIN_BACKOFF_MS * rejoinAttempt, 3000)
				this.logger.debug('stream: cooperative rebalance backoff before rejoin', {
					attempt: rejoinAttempt,
					backoffMs,
				})
				await sleep(backoffMs, { signal: this.abortController?.signal, resolveOnAbort: true })

				// Check if we should stop
				if (this.state !== 'running') {
					this.logger.debug('stream: aborting rejoin - consumer is stopping')
					return
				}

				rejoinResult = await this.consumerGroup.rejoin(topics, rejoinResult.revoked)
				groupCoordinator = this.consumerGroup.getCoordinator() ?? groupCoordinator
				this.offsetManager.updateGroupState(
					this.consumerGroup.currentMemberId,
					this.consumerGroup.currentGenerationId,
					groupCoordinator
				)
				if (rejoinResult.needsRejoin) {
					// Emit partitionsRevoked for newly revoked partitions
					if (rejoinResult.revoked.length > 0) {
						this.emit('partitionsRevoked', rejoinResult.revoked)
					}
					this.fetchManager.removePartitions(rejoinResult.revoked)
					if (this.commitOffsets) {
						await this.offsetManager.commitPartitions(rejoinResult.revoked)
					}
					this.offsetManager.clearPartitions(rejoinResult.revoked)
				}
			}

			// Final assignment
			this.fetchManager.removePartitions(rejoinResult.revoked)

			if (rejoinResult.added.length > 0) {
				const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
				this.offsetManager.updateGroupState(
					this.consumerGroup.currentMemberId,
					this.consumerGroup.currentGenerationId,
					coordinator
				)
				const committedOffsets = await this.offsetManager.fetchCommittedOffsets(rejoinResult.added)
				const newPartitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []
				for (const tp of rejoinResult.added) {
					const offset = await this.offsetManager.resolveStartingOffset(
						tp.topic,
						tp.partition,
						this.config.autoOffsetReset,
						committedOffsets
					)
					newPartitionsWithOffsets.push({ ...tp, offset })
				}
				this.fetchManager.addPartitions(newPartitionsWithOffsets)

				// Emit partitionsAssigned for new partitions
				this.emit('partitionsAssigned', rejoinResult.added)
			}

			const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
			this.offsetManager.updateGroupState(
				this.consumerGroup.currentMemberId,
				this.consumerGroup.currentGenerationId,
				coordinator
			)
		} else {
			// Eager rebalance

			// Emit partitionsRevoked for all current partitions
			const currentAssignment = this.consumerGroup.currentAssignment
			if (currentAssignment.length > 0) {
				this.emit('partitionsRevoked', currentAssignment)
			}

			this.fetchManager.stop()
			await currentFetchPromise?.catch(err => {
				this.logger.error('fetch error during rebalance', { error: (err as Error).message })
			})
			if (this.commitOffsets) {
				await this.offsetManager.commitPendingOffsets()
			}
			this.offsetManager.clearConsumedOffsets()
			await this.setupPartitions(subscriptions, rejoinResult.assignment)
			setFetchPromise(this.fetchManager.start(streamFetchCallback))

			// Emit partitionsAssigned for new assignment
			if (rejoinResult.assignment.length > 0) {
				this.emit('partitionsAssigned', rejoinResult.assignment)
			}
		}
	}

	/**
	 * Start the fetch loop for single message processing
	 */
	private async startFetchLoopEach(
		subscriptions: TopicSubscription<unknown, unknown>[],
		handler: MessageHandler<unknown, unknown>,
		opts: RunEachOptions
	): Promise<void> {
		if (!this.fetchManager || !this.offsetManager) {
			return
		}

		// Start auto-commit if enabled
		if (opts.autoCommit !== false && this.commitOffsets) {
			this.offsetManager.startAutoCommit(
				opts.autoCommitIntervalMs ?? DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs
			)
		}

		// Pre-compute decoder map for O(1) lookup (optimization: avoid array.find per record)
		const decoderMap = new Map<string, (buf: Buffer) => unknown>()
		const keyDecoderMap = new Map<string, (buf: Buffer) => unknown>()
		const defaultDecoder = (b: Buffer) => b
		for (const sub of subscriptions) {
			decoderMap.set(sub.topic, normalizeDecoder(sub.decoder))
			if (sub.keyDecoder) {
				keyDecoderMap.set(sub.topic, normalizeDecoder(sub.keyDecoder))
			}
		}

		// Create and store the fetch callback so it can be reused after rebalance
		this.fetchCallback = async (topic, partition, records) => {
			const decoder = decoderMap.get(topic) ?? defaultDecoder
			const keyDecoder = keyDecoderMap.get(topic) ?? defaultDecoder
			const signal = this.abortController!.signal

			// Reuse context object across records in this batch (update offset only)
			const ctx: ConsumeContext = {
				signal,
				topic,
				partition,
				offset: 0n, // Will be updated per record
			}

			for (const record of records) {
				// Stop quickly if the consumer is stopping (e.g., stop() called inside handler)
				if (signal.aborted || this.state !== 'running') {
					break
				}

				try {
					// Create Message directly from DecodedRecord (single object creation)
					const message: Message<unknown, unknown> = {
						topic,
						partition,
						offset: record.offset,
						timestamp: record.timestamp,
						key: record.key === null ? null : keyDecoder(record.key),
						// Preserve tombstones (null values) without decoding.
						// Note: types do not currently model nullable values in Message<V, K>.
						value: record.value === null ? null : decoder(record.value ?? EMPTY_BUFFER),
						headers: convertHeaders(record.headers),
					}

					// Update context offset for this record
					ctx.offset = record.offset

					// Process single message
					await handler(message, ctx)

					// Mark as consumed for auto-commit
					if (this.commitOffsets) {
						this.offsetManager!.markConsumed(topic, partition, record.offset)
					}
				} catch (error) {
					// Reject promise before stopping (stop() would resolve it)
					if (this.runPromiseReject) {
						this.runPromiseReject(error as Error)
						this.runPromiseReject = null
						this.runPromiseResolve = null
					}
					// Stop consumer
					this.stop()
					throw error
				}
			}
		}

		// Process records from fetch
		await this.fetchManager.start(this.fetchCallback)
	}

	/**
	 * Start the fetch loop for batch message processing
	 */
	private async startFetchLoopBatch(
		subscriptions: TopicSubscription<unknown, unknown>[],
		handler: BatchHandler<unknown, unknown>,
		opts: Required<Pick<RunBatchOptions, 'maxBatchSize' | 'maxBatchWaitMs'>> & RunBatchOptions
	): Promise<void> {
		if (!this.fetchManager || !this.offsetManager) {
			return
		}

		// Batch accumulator per partition
		const batches = new Map<string, Array<Message<unknown, unknown>>>()
		const batchTimers = new Map<string, ReturnType<typeof setTimeout>>()

		// Start auto-commit if enabled
		if (opts.autoCommit !== false && this.commitOffsets) {
			this.offsetManager.startAutoCommit(
				opts.autoCommitIntervalMs ?? DEFAULT_RUN_BATCH_OPTIONS.autoCommitIntervalMs
			)
		}

		// Pre-compute decoder map for O(1) lookup (optimization: avoid array.find per record)
		const decoderMap = new Map<string, (buf: Buffer) => unknown>()
		const keyDecoderMap = new Map<string, (buf: Buffer) => unknown>()
		const defaultDecoder = (b: Buffer) => b
		for (const sub of subscriptions) {
			decoderMap.set(sub.topic, normalizeDecoder(sub.decoder))
			if (sub.keyDecoder) {
				keyDecoderMap.set(sub.topic, normalizeDecoder(sub.keyDecoder))
			}
		}

		// Create and store the fetch callback so it can be reused after rebalance
		this.fetchCallback = async (topic, partition, records) => {
			const key = `${topic}:${partition}`
			const decoder = decoderMap.get(topic) ?? defaultDecoder
			const keyDecoder = keyDecoderMap.get(topic) ?? defaultDecoder
			const signal = this.abortController!.signal

			// Create context for this batch (will be used by flush)
			// Note: context is created per callback since it may be captured by timer
			const ctx: ConsumeContext = {
				signal,
				topic,
				partition,
				offset: 0n, // Will be updated to last offset
			}

			for (const record of records) {
				// Stop quickly if the consumer is stopping (e.g., stop() called inside handler)
				if (signal.aborted || this.state !== 'running') {
					break
				}

				try {
					// Create Message directly from DecodedRecord (single object creation)
					const message: Message<unknown, unknown> = {
						topic,
						partition,
						offset: record.offset,
						timestamp: record.timestamp,
						key: record.key === null ? null : keyDecoder(record.key),
						// Preserve tombstones (null values) without decoding.
						// Note: types do not currently model nullable values in Message<V, K>.
						value: record.value === null ? null : decoder(record.value ?? EMPTY_BUFFER),
						headers: convertHeaders(record.headers),
					}

					// Update context to latest offset
					ctx.offset = record.offset

					// Add to batch
					let batch = batches.get(key)
					if (!batch) {
						batch = []
						batches.set(key, batch)
					}
					batch.push(message)

					// Check batch size
					if (batch.length >= opts.maxBatchSize) {
						await this.flushBatch(key, batches, batchTimers, handler, ctx)
					} else if (!batchTimers.has(key)) {
						// Start batch timer
						batchTimers.set(
							key,
							setTimeout(() => {
								void this.flushBatch(key, batches, batchTimers, handler, ctx)
							}, opts.maxBatchWaitMs)
						)
					}
				} catch (error) {
					// Reject promise before stopping (stop() would resolve it)
					if (this.runPromiseReject) {
						this.runPromiseReject(error as Error)
						this.runPromiseReject = null
						this.runPromiseResolve = null
					}
					// Stop consumer
					this.stop()
					throw error
				}
			}
		}

		// Process records from fetch
		await this.fetchManager.start(this.fetchCallback)
	}

	/**
	 * Flush a batch and mark offsets as consumed after handler completes
	 */
	private async flushBatch(
		key: string,
		batches: Map<string, Array<Message<unknown, unknown>>>,
		timers: Map<string, ReturnType<typeof setTimeout>>,
		handler: BatchHandler<unknown, unknown>,
		ctx: ConsumeContext
	): Promise<void> {
		const batch = batches.get(key)
		if (!batch || batch.length === 0) {
			return
		}

		// Clear timer
		const timer = timers.get(key)
		if (timer) {
			clearTimeout(timer)
			timers.delete(key)
		}

		// Clear batch before calling handler (in case handler throws, we don't want to re-process)
		batches.set(key, [])

		// Call handler
		try {
			await handler(batch, ctx)

			// Mark all batch offsets as consumed AFTER handler succeeds
			if (this.commitOffsets) {
				for (const msg of batch) {
					this.offsetManager!.markConsumed(msg.topic, msg.partition, msg.offset)
				}
			}
		} catch (error) {
			// Reject promise before stopping (stop() would resolve it)
			if (this.runPromiseReject) {
				this.runPromiseReject(error as Error)
				this.runPromiseReject = null
				this.runPromiseResolve = null
			}
			// Stop consumer
			this.stop()
			throw error
		}
	}

	/**
	 * Clean up resources
	 */
	private async cleanup(): Promise<void> {
		// Stop fetch manager
		if (this.fetchManager) {
			this.fetchManager.stop()
		}

		// Wait for the fetch loop to fully exit before committing offsets.
		// This ensures the last processed message/batch has been marked as consumed.
		const fetchLoopPromise = this.fetchLoopPromise
		this.fetchLoopPromise = null
		if (fetchLoopPromise) {
			try {
				await fetchLoopPromise
			} catch (error) {
				this.emitError(error)
			}
		}

		// Stop auto-commit and commit pending
		if (this.offsetManager) {
			this.offsetManager.stopAutoCommit()
			if (this.commitOffsets && !this.sessionLost) {
				try {
					await this.offsetManager.commitPendingOffsets()
				} catch (error) {
					this.emitError(error)
				}
			}
		}

		// Leave group
		if (this.consumerGroup) {
			try {
				await this.consumerGroup.stop()
			} catch (error) {
				this.emitError(error)
			}
		}

		// Clear references
		this.consumerGroup = null
		this.offsetManager = null
		this.fetchManager = null
		this.abortController = null
		this.fetchCallback = null
		this.runPromiseReject = null
	}

	private emitError(error: unknown): void {
		const err = error instanceof Error ? error : new Error(String(error))
		if (this.listenerCount('error') > 0) {
			this.emit('error', err)
		} else {
			this.logger.error('consumer error', { error: err.message })
		}
	}
}
