/**
 * High-level Kafka consumer
 *
 * Main public API for consuming messages from Kafka.
 * Supports single message, batch, and stream modes.
 */

import { EventEmitter } from 'node:events'
import type { Cluster } from '@/client/cluster.js'
import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { DecodedRecord, RecordHeader } from '@/protocol/records/index.js'
import type {
	ConsumerConfig,
	ResolvedConsumerConfig,
	ConsumerEvents,
	TopicSubscription,
	SubscriptionInput,
	Message,
	ConsumeContext,
	MessageHandler,
	BatchHandler,
	RunEachOptions,
	RunBatchOptions,
	TopicPartition,
	TopicPartitionOffset,
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

/**
 * Subscription mode: either group-managed (subscribe) or manual (assign)
 */
type SubscriptionMode = 'none' | 'subscribe' | 'assign'

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

	// Subscription state
	private subscriptionMode: SubscriptionMode = 'none'
	private subscriptions: TopicSubscription<unknown, unknown>[] = []
	private manualAssignment: TopicPartitionOffset[] = []

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
	 * Subscribe to topics for group-managed partition assignment.
	 *
	 * When using subscribe, partitions are automatically assigned by the group
	 * coordinator and rebalanced when group membership changes.
	 *
	 * @param subscription - Topics to subscribe to (strings, TopicSubscription, or TopicDefinition)
	 * @throws If already subscribed or assigned
	 */
	subscribe<S extends SubscriptionInput>(subscription: S): void {
		if (this.state !== 'idle') {
			throw new Error('Cannot subscribe while consumer is running. Call stop() first.')
		}
		if (this.subscriptionMode === 'assign') {
			throw new Error('Cannot subscribe after assign(). Use either subscribe() or assign(), not both.')
		}

		this.subscriptionMode = 'subscribe'
		this.subscriptions = this.normalizeSubscription(subscription)
		this.manualAssignment = []

		this.logger.debug('subscribed to topics', { topics: this.subscriptions.map(s => s.topic) })
	}

	/**
	 * Manually assign specific topic partitions to consume from.
	 *
	 * When using assign, there is no group coordination - you directly control
	 * which partitions are consumed. No rebalancing will occur.
	 *
	 * @param partitions - Topic partitions to assign with optional starting offsets
	 * @param options - Optional subscription options for decoders
	 * @throws If already subscribed or assigned
	 */
	assign<S extends SubscriptionInput>(partitions: TopicPartitionOffset[], options?: { subscription?: S }): void {
		if (this.state !== 'idle') {
			throw new Error('Cannot assign while consumer is running. Call stop() first.')
		}
		if (this.subscriptionMode === 'subscribe') {
			throw new Error('Cannot assign after subscribe(). Use either subscribe() or assign(), not both.')
		}

		this.subscriptionMode = 'assign'
		this.manualAssignment = partitions

		// If subscription options provided, use them for decoders
		// Otherwise create basic subscriptions from partition topics
		if (options?.subscription) {
			this.subscriptions = this.normalizeSubscription(options.subscription)
		} else {
			// Create basic subscriptions from partition topics (no decoders, raw Buffer)
			const topics = [...new Set(partitions.map(p => p.topic))]
			this.subscriptions = topics.map(topic => toTopicSubscription(topic))
		}

		this.logger.debug('assigned partitions', {
			partitions: partitions.map(p => `${p.topic}-${p.partition}`),
		})
	}

	/**
	 * Run the consumer loop processing one message at a time
	 *
	 * Call subscribe() or assign() before calling runEach().
	 *
	 * - One consumer → one run. Calling runEach() again while active throws.
	 * - Resolves when stop() is called, options.signal aborts, or a fatal error occurs.
	 */
	async runEach<V = unknown, K = unknown>(handler: MessageHandler<V, K>, options?: RunEachOptions): Promise<void> {
		if (this.state !== 'idle') {
			throw new Error('Consumer is already running. Call stop() first.')
		}
		if (this.subscriptionMode === 'none') {
			throw new Error('No subscription. Call subscribe() or assign() before runEach().')
		}

		this.state = 'running'
		this.abortController = new AbortController()
		this.sessionLost = false

		const opts = {
			...DEFAULT_RUN_EACH_OPTIONS,
			...options,
		}
		this.commitOffsets = opts.commitOffsets !== false

		// Set up external abort signal
		if (options?.signal) {
			options.signal.addEventListener('abort', () => {
				this.stop()
			})
		}

		try {
			const subscriptions = this.subscriptions
			const topics = subscriptions.map(s => s.topic)

			this.logger.info('starting consumer', { topics, mode: 'each', subscriptionMode: this.subscriptionMode })

			// Create offset manager (always needed for offset tracking)
			this.offsetManager = new OffsetManager(
				this.cluster,
				this.config.groupId,
				this.config.groupInstanceId,
				this.logger
			)

			// Create fetch manager
			this.fetchManager = new FetchManager(
				this.cluster,
				this.offsetManager,
				this.config.autoOffsetReset,
				{
					maxBytesPerPartition: this.config.maxBytesPerPartition,
					minBytes: this.config.minBytes,
					maxWaitMs: this.config.maxWaitMs,
					partitionConcurrency: opts.partitionConcurrency ?? 1,
					isolationLevel: this.config.isolationLevel,
					trace: this.trace,
				},
				this.logger
			)

			let initialAssignment: TopicPartition[]

			if (this.subscriptionMode === 'subscribe') {
				// Group-managed mode: create consumer group and join
				this.consumerGroup = new ConsumerGroup(this.cluster, this.config, this.logger)

				// Handle rebalance
				this.consumerGroup.on('rebalance', () => {
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

				this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
					this.logger.info('session lost, partitions cannot be committed', {
						partitionCount: partitions.length,
					})
					this.sessionLost = true
					this.offsetManager?.clearPartitions(partitions)
					this.fetchManager?.removePartitions(partitions)
					this.emit('partitionsLost', partitions)
				})

				this.consumerGroup.on('error', (error: Error) => {
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

				this.logger.debug('joining group', { topics })
				const joinResult = await this.consumerGroup.join(topics)
				this.logger.info('group joined', { assignedPartitions: joinResult.assignment.length })
				await this.setupPartitions(subscriptions, joinResult.assignment)
				initialAssignment = joinResult.assignment
			} else {
				// Manual assignment mode: use assigned partitions directly
				this.logger.debug('using manual assignment', {
					partitions: this.manualAssignment.map(p => `${p.topic}-${p.partition}`),
				})
				await this.setupManualPartitions(this.manualAssignment)
				initialAssignment = this.manualAssignment.map(p => ({ topic: p.topic, partition: p.partition }))
			}

			if (initialAssignment.length > 0) {
				this.emit('partitionsAssigned', initialAssignment)
			}

			this.emit('running')

			await new Promise<void>((resolve, reject) => {
				this.runPromiseResolve = resolve
				this.runPromiseReject = reject

				this.fetchLoopPromise = this.startFetchLoopEach(
					subscriptions,
					handler as MessageHandler<unknown, unknown>,
					opts
				)
					.then(() => {
						// If the fetch loop ends, ensure the run promise resolves.
						if (this.runPromiseResolve) {
							this.runPromiseResolve()
							this.runPromiseResolve = null
							this.runPromiseReject = null
						}
					})
					.catch(error => {
						// Surface unexpected fetch loop failures to the run() caller.
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
		} finally {
			this.logger.debug('consumer run loop ended, cleaning up')
			await this.cleanup()
			this.state = 'idle'
			this.logger.info('consumer stopped')
			this.emit('stopped')
		}
	}

	/**
	 * Run the consumer loop processing messages in batches
	 *
	 * Call subscribe() or assign() before calling runBatch().
	 *
	 * - One consumer → one run. Calling runBatch() again while active throws.
	 * - Resolves when stop() is called, options.signal aborts, or a fatal error occurs.
	 * - Offsets are only marked as consumed after the batch handler completes successfully.
	 */
	async runBatch<V = unknown, K = unknown>(handler: BatchHandler<V, K>, options?: RunBatchOptions): Promise<void> {
		if (this.state !== 'idle') {
			throw new Error('Consumer is already running. Call stop() first.')
		}
		if (this.subscriptionMode === 'none') {
			throw new Error('No subscription. Call subscribe() or assign() before runBatch().')
		}

		this.state = 'running'
		this.abortController = new AbortController()
		this.sessionLost = false

		const opts = {
			...DEFAULT_RUN_BATCH_OPTIONS,
			...options,
		}
		this.commitOffsets = opts.commitOffsets !== false

		// Set up external abort signal
		if (options?.signal) {
			options.signal.addEventListener('abort', () => {
				this.stop()
			})
		}

		try {
			const subscriptions = this.subscriptions
			const topics = subscriptions.map(s => s.topic)

			this.logger.info('starting consumer', { topics, mode: 'batch', subscriptionMode: this.subscriptionMode })

			// Create offset manager (always needed for offset tracking)
			this.offsetManager = new OffsetManager(
				this.cluster,
				this.config.groupId,
				this.config.groupInstanceId,
				this.logger
			)

			// Create fetch manager
			this.fetchManager = new FetchManager(
				this.cluster,
				this.offsetManager,
				this.config.autoOffsetReset,
				{
					maxBytesPerPartition: this.config.maxBytesPerPartition,
					minBytes: this.config.minBytes,
					maxWaitMs: this.config.maxWaitMs,
					partitionConcurrency: opts.partitionConcurrency ?? 1,
					isolationLevel: this.config.isolationLevel,
				},
				this.logger
			)

			let initialAssignment: TopicPartition[]

			if (this.subscriptionMode === 'subscribe') {
				// Group-managed mode: create consumer group and join
				this.consumerGroup = new ConsumerGroup(this.cluster, this.config, this.logger)

				// Handle rebalance
				this.consumerGroup.on('rebalance', () => {
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

				this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
					this.logger.info('session lost, partitions cannot be committed', {
						partitionCount: partitions.length,
					})
					this.sessionLost = true
					this.offsetManager?.clearPartitions(partitions)
					this.fetchManager?.removePartitions(partitions)
					this.emit('partitionsLost', partitions)
				})

				this.consumerGroup.on('error', (error: Error) => {
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

				this.logger.debug('joining group', { topics })
				const joinResult = await this.consumerGroup.join(topics)
				this.logger.info('group joined', { assignedPartitions: joinResult.assignment.length })
				await this.setupPartitions(subscriptions, joinResult.assignment)
				initialAssignment = joinResult.assignment
			} else {
				// Manual assignment mode: use assigned partitions directly
				this.logger.debug('using manual assignment', {
					partitions: this.manualAssignment.map(p => `${p.topic}-${p.partition}`),
				})
				await this.setupManualPartitions(this.manualAssignment)
				initialAssignment = this.manualAssignment.map(p => ({ topic: p.topic, partition: p.partition }))
			}

			if (initialAssignment.length > 0) {
				this.emit('partitionsAssigned', initialAssignment)
			}

			this.emit('running')

			await new Promise<void>((resolve, reject) => {
				this.runPromiseResolve = resolve
				this.runPromiseReject = reject
				this.fetchLoopPromise = this.startFetchLoopBatch(
					subscriptions,
					handler as BatchHandler<unknown, unknown>,
					opts
				)
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
		} finally {
			this.logger.debug('consumer run loop ended, cleaning up')
			await this.cleanup()
			this.state = 'idle'
			this.logger.info('consumer stopped')
			this.emit('stopped')
		}
	}

	/**
	 * Async iterator mode
	 *
	 * Call subscribe() or assign() before calling stream().
	 *
	 * - Also single-run: calling stream() during run() throws.
	 * - Returns one message at a time.
	 */
	async *stream<V = unknown, K = unknown>(): AsyncIterable<{ message: Message<V, K>; ctx: ConsumeContext }> {
		if (this.state !== 'idle') {
			throw new Error('Consumer is already running. Call stop() first.')
		}
		if (this.subscriptionMode === 'none') {
			throw new Error('No subscription. Call subscribe() or assign() before stream().')
		}

		this.state = 'running'
		this.abortController = new AbortController()
		this.commitOffsets = true
		this.sessionLost = false

		try {
			const subscriptions = this.subscriptions
			const topics = subscriptions.map(s => s.topic)

			// Create offset manager (always needed for offset tracking)
			this.offsetManager = new OffsetManager(this.cluster, this.config.groupId, this.config.groupInstanceId)

			// Create fetch manager
			this.fetchManager = new FetchManager(this.cluster, this.offsetManager, this.config.autoOffsetReset, {
				maxBytesPerPartition: this.config.maxBytesPerPartition,
				minBytes: this.config.minBytes,
				maxWaitMs: this.config.maxWaitMs,
				partitionConcurrency: 1, // Stream mode uses single concurrency
				isolationLevel: this.config.isolationLevel,
			})

			let initialAssignment: TopicPartition[]
			let pendingRebalance = false

			if (this.subscriptionMode === 'subscribe') {
				// Group-managed mode: create consumer group and join
				this.consumerGroup = new ConsumerGroup(this.cluster, this.config)

				// Handle rebalance
				this.consumerGroup.on('rebalance', () => {
					pendingRebalance = true
					this.emit('rebalance')
				})

				this.consumerGroup.on('sessionLost', (partitions: TopicPartition[]) => {
					this.logger.info('session lost, partitions cannot be committed', {
						partitionCount: partitions.length,
					})
					this.sessionLost = true
					this.offsetManager?.clearPartitions(partitions)
					this.fetchManager?.removePartitions(partitions)
					this.emit('partitionsLost', partitions)
				})

				this.consumerGroup.on('error', (error: Error) => {
					const err = error instanceof Error ? error : new Error(String(error))
					this.emitError(err)

					if (err instanceof KafkaProtocolError && err.errorCode === ErrorCode.FencedInstanceId) {
						this.stop()
					}
				})

				const joinResult = await this.consumerGroup.join(topics)
				await this.setupPartitions(subscriptions, joinResult.assignment)
				initialAssignment = joinResult.assignment
			} else {
				// Manual assignment mode: use assigned partitions directly
				await this.setupManualPartitions(this.manualAssignment)
				initialAssignment = this.manualAssignment.map(p => ({ topic: p.topic, partition: p.partition }))
			}

			// Emit partitionsAssigned for initial assignment
			if (initialAssignment.length > 0) {
				this.emit('partitionsAssigned', initialAssignment)
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
						value: decoder(record.value ?? EMPTY_BUFFER),
						headers: convertHeaders(record.headers),
					}

					const ctx: ConsumeContext = {
						signal: this.abortController!.signal,
						topic,
						partition,
						offset: record.offset,
					}

					messageQueue.push({ message, ctx })

					// Mark as consumed for auto-commit
					this.offsetManager!.markConsumed(topic, partition, record.offset)

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
			currentFetchPromise = this.fetchManager.start(streamFetchCallback)

			// Start auto-commit
			if (this.offsetManager) {
				this.offsetManager.startAutoCommit(DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs)
			}

			// Yield messages
			while (this.state === 'running' && !this.abortController.signal.aborted) {
				// Handle rebalance (only in subscribe mode)
				if (pendingRebalance && this.subscriptionMode === 'subscribe') {
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
					yield item as { message: Message<V, K>; ctx: ConsumeContext }
				}
			}

			// Clean up fetch
			this.fetchManager.stop()
			await currentFetchPromise?.catch(err => {
				this.logger.error('fetch error during cleanup', { error: (err as Error).message })
			})
		} finally {
			await this.cleanup()
			this.state = 'idle'
			this.emit('stopped')
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
	 * Set up partitions for manual assignment mode (no group coordination)
	 */
	private async setupManualPartitions(partitions: TopicPartitionOffset[]): Promise<void> {
		const offsetManager = this.offsetManager
		const fetchManager = this.fetchManager

		if (!offsetManager || !fetchManager) {
			return
		}

		// Set up coordinator for offset commits.
		// For simple consumers (manual assignment), we use empty memberId and generationId=-1.
		// This allows offset commits to work without group membership.
		const coordinator = await this.cluster.getCoordinator('GROUP', this.config.groupId)
		offsetManager.updateGroupState('', -1, coordinator)

		const partitionsWithOffsets: Array<TopicPartition & { offset: bigint }> = []

		for (const tp of partitions) {
			// If offset is provided (>= 0), use it directly
			// Otherwise resolve based on autoOffsetReset
			let offset: bigint
			if (tp.offset >= 0n) {
				offset = tp.offset
			} else {
				// Resolve starting offset (earliest/latest based on config)
				offset = await offsetManager.resolveStartingOffset(
					tp.topic,
					tp.partition,
					this.config.autoOffsetReset,
					new Map() // No committed offsets in manual mode
				)
			}

			partitionsWithOffsets.push({ topic: tp.topic, partition: tp.partition, offset })
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
						value: decoder(record.value ?? EMPTY_BUFFER),
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
						value: decoder(record.value ?? EMPTY_BUFFER),
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
