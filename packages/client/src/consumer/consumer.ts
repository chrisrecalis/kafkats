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
import {
	EachRecordProcessor,
	BatchRecordProcessor,
	StreamRecordProcessor,
	createFetchCallback,
	type RecordProcessor,
	type FetchCallback,
} from './record-processor.js'
import {
	ManualPartitionProvider,
	GroupPartitionProvider,
	type PartitionProvider,
	type PartitionProviderCallbacks,
} from './partition-provider.js'
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
	toTopicSubscription,
	type SubscriptionLike,
} from './types.js'
import { ConsumerGroup } from './consumer-group.js'
import { OffsetManager } from './offset-manager.js'
import { FetchManager } from './fetch-manager.js'
import { noopLogger, type Logger } from '@/logger.js'

/**
 * Consumer state
 */
type ConsumerState = 'idle' | 'running' | 'stopping'

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
	private partitionProvider: PartitionProvider | null = null
	private fetchLoopPromise: Promise<void> | null = null
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runPromiseReject: ((error: Error) => void) | null = null
	private fetchCallback: FetchCallback | null = null

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
			this.logger.debug('consumer stopped')
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

	private initComponents(partitionConcurrency: number, useConsumerGroup: boolean, logger?: Logger): void {
		if (useConsumerGroup) {
			this.consumerGroup = new ConsumerGroup(this.cluster, this.config, logger)
		}
		this.offsetManager = new OffsetManager(
			this.cluster,
			this.config.groupId,
			useConsumerGroup ? this.config.groupInstanceId : undefined,
			logger
		)
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

	/**
	 * Create callbacks for partition provider events
	 */
	private createProviderCallbacks(manualOffsets?: Map<string, bigint>): PartitionProviderCallbacks {
		return {
			onPartitionsAssigned: async partitions => {
				const withOffsets = await this.resolvePartitionOffsets(partitions, manualOffsets)
				this.fetchManager!.setPartitions(withOffsets)
				this.emit('partitionsAssigned', partitions)
			},
			onPartitionsRevoked: async partitions => {
				this.fetchManager!.removePartitions(partitions)
				if (this.commitOffsets && !this.sessionLost) {
					await this.offsetManager!.commitPendingOffsets()
				}
				this.offsetManager!.clearPartitions(partitions)
				this.emit('partitionsRevoked', partitions)
			},
			onPartitionsLost: partitions => {
				this.logger.debug('session lost, partitions cannot be committed', {
					partitionCount: partitions.length,
				})
				this.sessionLost = true
				this.offsetManager!.clearPartitions(partitions)
				this.fetchManager!.removePartitions(partitions)
				this.emit('partitionsLost', partitions)
			},
			onError: error => {
				this.emitError(error)
				if (error instanceof KafkaProtocolError && error.errorCode === ErrorCode.FencedInstanceId) {
					if (this.runPromiseReject) {
						this.runPromiseReject(error)
						this.runPromiseReject = null
						this.runPromiseResolve = null
					}
					this.stop()
				}
			},
			resolveOffsets: partitions => this.resolvePartitionOffsets(partitions, manualOffsets),
		}
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
	 * Unified run mode implementation using PartitionProvider
	 */
	private async runMode(
		subscription: SubscriptionInput,
		opts: RunEachOptions | RunBatchOptions,
		concurrency: number,
		startFetchLoop: (subscriptions: TopicSubscription<unknown, unknown>[]) => Promise<void>
	): Promise<void> {
		this.startRun(opts.commitOffsets !== false, opts.signal)

		try {
			const manualAssignment = opts.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			// Create components
			const useConsumerGroup = !manualAssignment
			this.initComponents(concurrency, useConsumerGroup, this.logger)

			// Create partition provider
			let manualOffsets: Map<string, bigint> | undefined
			if (manualAssignment) {
				const normalized = this.normalizeAndValidateManualAssignment(manualAssignment, topics)
				manualOffsets = new Map()
				for (const tp of normalized) {
					if (tp.offset !== undefined) {
						manualOffsets.set(`${tp.topic}:${tp.partition}`, tp.offset)
					}
				}
				this.partitionProvider = new ManualPartitionProvider({
					assignment: normalized,
					cluster: this.cluster,
					groupId: this.config.groupId,
					offsetManager: this.offsetManager!,
					signal: this.abortController?.signal,
				})
				this.logger.debug('starting consumer', {
					topics,
					assignmentMode: 'manual',
					assignedPartitionCount: normalized.length,
				})
			} else {
				this.partitionProvider = new GroupPartitionProvider({
					consumerGroup: this.consumerGroup!,
					cluster: this.cluster,
					groupId: this.config.groupId,
					autoOffsetReset: this.config.autoOffsetReset,
					commitOffsets: this.commitOffsets && !this.sessionLost,
					offsetManager: this.offsetManager!,
					fetchManager: this.fetchManager!,
					logger: this.logger,
					signal: this.abortController?.signal,
					isRunning: () => this.state === 'running',
				})
				this.logger.debug('starting consumer', { topics })
			}

			// Create callbacks and start provider
			const callbacks = this.createProviderCallbacks(manualOffsets)
			await this.partitionProvider.start(topics, callbacks)

			this.emit('running')

			// Start fetch loop
			await this.startFetchLoopAndWait(() => startFetchLoop(subscriptions))
		} catch (error) {
			this.emitError(error)
			throw error
		} finally {
			await this.finalizeRun()
		}
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
		const opts = { ...DEFAULT_RUN_EACH_OPTIONS, ...options }
		const concurrency = opts.partitionConcurrency ?? DEFAULT_RUN_EACH_OPTIONS.partitionConcurrency
		await this.runMode(subscription, opts, concurrency, subscriptions => {
			const processor = new EachRecordProcessor(
				handler as MessageHandler<unknown, unknown>,
				this.offsetManager!,
				this.commitOffsets,
				this.createProcessorErrorHandler()
			)
			return this.startFetchLoop(
				subscriptions,
				processor,
				opts.autoCommitIntervalMs ?? DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs,
				opts.autoCommit !== false
			)
		})
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
		const opts = { ...DEFAULT_RUN_BATCH_OPTIONS, ...options }
		const concurrency = opts.partitionConcurrency ?? DEFAULT_RUN_BATCH_OPTIONS.partitionConcurrency
		await this.runMode(subscription, opts, concurrency, subscriptions => {
			const processor = new BatchRecordProcessor(
				handler as BatchHandler<unknown, unknown>,
				this.offsetManager!,
				this.commitOffsets,
				this.createProcessorErrorHandler()
			)
			return this.startFetchLoop(
				subscriptions,
				processor,
				opts.autoCommitIntervalMs ?? DEFAULT_RUN_BATCH_OPTIONS.autoCommitIntervalMs,
				opts.autoCommit !== false
			)
		})
	}

	/**
	 * Async iterator mode
	 *
	 * - Also single-run: calling stream() during run() throws.
	 * - Returns one message at a time.
	 * - Uses push-pull adapter pattern: fetch loop pushes to queue, generator yields from queue.
	 */
	async *stream<S extends SubscriptionInput>(
		subscription: S
	): AsyncIterable<{ message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }> {
		this.startRun(true)

		try {
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			// Create components
			this.initComponents(1, true, this.logger)
			const consumerGroup = this.consumerGroup
			const offsetManager = this.offsetManager
			const fetchManager = this.fetchManager
			const signal = this.abortController!.signal
			if (!consumerGroup || !offsetManager || !fetchManager) {
				return
			}

			// Create partition provider (same as runMode)
			this.partitionProvider = new GroupPartitionProvider({
				consumerGroup,
				cluster: this.cluster,
				groupId: this.config.groupId,
				autoOffsetReset: this.config.autoOffsetReset,
				commitOffsets: this.commitOffsets,
				offsetManager,
				fetchManager,
				logger: this.logger,
				signal,
				isRunning: () => this.state === 'running',
			})

			// Start partition provider
			const callbacks = this.createProviderCallbacks()
			await this.partitionProvider.start(topics, callbacks)
			this.emit('running')

			// Push-pull adapter: queue bridges fetch callbacks (push) and generator (pull)
			const messageQueue: Array<{ message: Message<unknown, unknown>; ctx: ConsumeContext }> = []
			let resolveNext: (() => void) | null = null
			const wakeUp = () => {
				if (resolveNext) {
					resolveNext()
					resolveNext = null
				}
			}

			// Create stream processor that pushes to queue
			const processor = new StreamRecordProcessor(messageQueue, offsetManager, wakeUp)
			this.fetchCallback = createFetchCallback(subscriptions, processor, signal, () => this.state === 'running')

			// Start auto-commit and fetch loop in background
			offsetManager.startAutoCommit(DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs)
			this.fetchLoopPromise = fetchManager.start(this.fetchCallback)

			// Track fetch errors
			let fetchError: Error | null = null
			this.fetchLoopPromise.catch(err => {
				fetchError = err instanceof Error ? err : new Error(String(err))
				wakeUp()
			})

			// Yield messages from queue
			const onAbort = () => wakeUp()
			signal.addEventListener('abort', onAbort)
			try {
				while (this.state === 'running' && !signal.aborted) {
					if (fetchError) throw fetchError

					// Wait for messages
					if (messageQueue.length === 0) {
						await new Promise<void>(resolve => {
							resolveNext = resolve
							setTimeout(resolve, 100) // Periodic check for errors/abort
						})
					}

					// Yield all queued messages
					while (messageQueue.length > 0) {
						const item = messageQueue.shift()!
						yield item as { message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }
					}
				}

				if (fetchError) throw fetchError
			} finally {
				signal.removeEventListener('abort', onAbort)
			}
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

		this.logger.debug('stopping consumer')
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
	 * Resolve starting offsets for partitions
	 */
	private async resolvePartitionOffsets(
		assignment: TopicPartition[],
		manualOffsets?: Map<string, bigint>
	): Promise<Array<TopicPartition & { offset: bigint }>> {
		const offsetManager = this.offsetManager
		if (!offsetManager) {
			return []
		}

		const committedOffsets = await offsetManager.fetchCommittedOffsets(assignment)
		const result: Array<TopicPartition & { offset: bigint }> = []

		for (const tp of assignment) {
			const key = `${tp.topic}:${tp.partition}`
			const manualOffset = manualOffsets?.get(key)
			const offset =
				manualOffset !== undefined
					? manualOffset
					: await offsetManager.resolveStartingOffset(
							tp.topic,
							tp.partition,
							this.config.autoOffsetReset,
							committedOffsets
						)
			result.push({ ...tp, offset })
		}

		return result
	}

	/**
	 * Create error handler for record processors
	 */
	private createProcessorErrorHandler(): (error: Error) => void {
		return (error: Error) => {
			if (this.runPromiseReject) {
				this.runPromiseReject(error)
				this.runPromiseReject = null
				this.runPromiseResolve = null
			}
			this.stop()
		}
	}

	/**
	 * Unified fetch loop for all processing modes
	 */
	private async startFetchLoop(
		subscriptions: TopicSubscription<unknown, unknown>[],
		processor: RecordProcessor,
		autoCommitIntervalMs: number,
		autoCommit: boolean
	): Promise<void> {
		if (!this.fetchManager || !this.offsetManager) {
			return
		}

		// Start auto-commit if enabled
		if (autoCommit && this.commitOffsets) {
			this.offsetManager.startAutoCommit(autoCommitIntervalMs)
		}

		// Create fetch callback using shared infrastructure
		this.fetchCallback = createFetchCallback(
			subscriptions,
			processor,
			this.abortController!.signal,
			() => this.state === 'running'
		)

		// Process records from fetch
		await this.fetchManager.start(this.fetchCallback)
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

		// Stop partition provider (handles leaving group for GroupPartitionProvider)
		if (this.partitionProvider) {
			try {
				await this.partitionProvider.stop()
			} catch (error) {
				this.emitError(error)
			}
		}

		// Clear references
		this.consumerGroup = null
		this.offsetManager = null
		this.fetchManager = null
		this.partitionProvider = null
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
