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
import { pmapVoid } from '@/utils/pmap.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { buildDecoderMaps, decodeRecord } from './message-decoder.js'
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
	StreamOptions,
	TopicPartition,
	ManualAssignment,
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
import { PartitionTracker } from './partition-tracker.js'
import { noopLogger, type Logger } from '@/logger.js'

type ConsumerState = 'idle' | 'running' | 'stopping'

type InternalBatchHandler = (
	topic: string,
	partition: number,
	records: DecodedRecord[],
	decoders: ReturnType<typeof buildDecoderMaps>,
	ctx: { signal: AbortSignal; offsetManager: OffsetManager; commitOffsets: boolean }
) => Promise<void>

export class Consumer extends EventEmitter<ConsumerEvents> {
	private readonly cluster: Cluster
	private readonly config: ResolvedConsumerConfig
	private readonly logger: Logger

	private state: ConsumerState = 'idle'
	private commitOffsets = true
	private sessionLost = false
	private consumerGroup: ConsumerGroup | null = null
	private offsetManager: OffsetManager | null = null
	private fetchManager: FetchManager | null = null
	private partitionTracker: PartitionTracker | null = null
	private partitionProvider: PartitionProvider | null = null
	private fetchLoopPromise: Promise<void> | null = null
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runPromiseReject: ((error: Error) => void) | null = null

	constructor(cluster: Cluster, config: ConsumerConfig) {
		super()
		this.cluster = cluster
		this.logger = cluster.getLogger()?.child({ component: 'consumer', groupId: config.groupId }) ?? noopLogger
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
		this.partitionTracker = new PartitionTracker({ logger })

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
			},
			logger
		)
	}

	private createProviderCallbacks(): PartitionProviderCallbacks {
		return {
			// eslint-disable-next-line @typescript-eslint/require-await
			onPartitionsAssigned: async partitionsWithOffsets => {
				this.sessionLost = false
				const partitions = partitionsWithOffsets.map(p => ({ topic: p.topic, partition: p.partition }))
				this.offsetManager!.addAssignedPartitions(partitions)
				this.partitionTracker!.assign(partitions)
				this.fetchManager!.addPartitions(partitionsWithOffsets)
				this.emit('partitionsAssigned', partitions)
			},
			onPartitionsRevoked: async partitions => {
				if (this.partitionTracker) {
					await this.partitionTracker.revoke(partitions)
				}
				this.offsetManager!.removeAssignedPartitions(partitions)
				this.fetchManager!.removePartitions(partitions)
				if (this.commitOffsets && !this.sessionLost) {
					await this.offsetManager!.commitPendingOffsets()
				}
				this.offsetManager!.clearPartitions(partitions)
				this.emit('partitionsRevoked', partitions)
			},
			onPartitionsLost: partitions => {
				this.sessionLost = true
				this.offsetManager!.removeAssignedPartitions(partitions)
				for (const tp of partitions) {
					this.partitionTracker?.endProcessing(tp.topic, tp.partition)
				}
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
	 * Whether the consumer is currently running.
	 */
	get isRunning(): boolean {
		return this.state === 'running'
	}

	/**
	 * Pause fetching from specific partitions.
	 *
	 * Useful for backpressure control when processing cannot keep up with incoming messages.
	 * The consumer remains in the group and continues heartbeating while paused.
	 *
	 * @param partitions - The partitions to pause
	 * @throws If the consumer is not running
	 */
	pause(partitions: TopicPartition[]): void {
		if (this.state !== 'running' || !this.fetchManager) {
			throw new Error('Consumer is not running')
		}
		this.fetchManager.pausePartitions(partitions)
	}

	/**
	 * Resume fetching from previously paused partitions.
	 *
	 * @param partitions - The partitions to resume
	 * @throws If the consumer is not running
	 */
	resume(partitions: TopicPartition[]): void {
		if (this.state !== 'running' || !this.fetchManager) {
			throw new Error('Consumer is not running')
		}
		this.fetchManager.resumePartitions(partitions)
	}

	/**
	 * Seek to a specific offset for a partition.
	 *
	 * Repositions the fetch position for the specified partition to the given offset.
	 * The next fetch will start at this offset.
	 *
	 * Common use cases:
	 * - Replaying messages: seek to an earlier offset
	 * - Skipping ahead: seek to a later offset
	 * - Resetting to a known position after handling errors
	 *
	 * Note: This only affects the fetch position, not the committed offset.
	 * If you want to persist the position, you'll need to commit after seeking.
	 *
	 * @param topic - Topic name
	 * @param partition - Partition index
	 * @param offset - The offset to seek to (will fetch from this offset next)
	 * @throws If consumer is not running
	 */
	seek(topic: string, partition: number, offset: bigint): void {
		if (this.state !== 'running' || !this.fetchManager) {
			throw new Error('Consumer is not running')
		}
		this.fetchManager.seekPartition(topic, partition, offset)
	}

	/**
	 * Internal batch handler type for poll-based processing
	 */
	private async runPollLoop(
		subscriptions: TopicSubscription<unknown, unknown>[],
		batchHandler: InternalBatchHandler,
		concurrency: number,
		autoCommitIntervalMs: number,
		autoCommit: boolean
	): Promise<void> {
		const fetchManager = this.fetchManager!
		const offsetManager = this.offsetManager!
		const partitionTracker = this.partitionTracker!
		const partitionProvider = this.partitionProvider!
		const signal = this.abortController!.signal
		const decoders = buildDecoderMaps(subscriptions)

		if (autoCommit && this.commitOffsets) {
			offsetManager.startAutoCommit(autoCommitIntervalMs)
		}

		const handlerCtx = { signal, offsetManager, commitOffsets: this.commitOffsets }

		while (this.state === 'running' && !signal.aborted) {
			const batches = await fetchManager.poll()

			// Handle any pending rebalance after poll returns but before processing
			// This ensures revoked partitions are removed before we check assignments
			await partitionProvider.checkAndHandleRebalance()
			if (this.state !== 'running' || signal.aborted) break

			if (batches.length === 0) {
				continue
			}

			try {
				await pmapVoid(
					batches,
					async batch => {
						// Skip batches from partitions that were revoked during rebalance
						// Check both FetchManager (has partition state) and PartitionTracker (assigned + not revoking)
						if (!fetchManager.isPartitionAssigned(batch.topic, batch.partition)) {
							return
						}

						// Mark partition as processing - returns false if partition is not assigned or being revoked
						if (!partitionTracker.startProcessing(batch.topic, batch.partition)) {
							return
						}

						try {
							await batchHandler(batch.topic, batch.partition, batch.records, decoders, handlerCtx)
						} finally {
							// Mark processing complete - this unblocks any pending revoke() wait
							partitionTracker.endProcessing(batch.topic, batch.partition)
						}
					},
					concurrency,
					signal
				)
			} catch (error) {
				this.emitError(error)
				throw error
			}
		}
	}

	/**
	 * Unified run mode implementation using PartitionProvider
	 */
	private async run(
		subscription: SubscriptionInput,
		opts: RunEachOptions | RunBatchOptions,
		concurrency: number,
		batchHandler: InternalBatchHandler,
		autoCommitIntervalMs: number,
		autoCommit: boolean
	): Promise<void> {
		this.startRun(opts.commitOffsets !== false, opts.signal)

		try {
			const manualAssignment = opts.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)

			const useConsumerGroup = !manualAssignment
			this.initComponents(concurrency, useConsumerGroup, this.logger)

			if (manualAssignment) {
				const normalized = this.normalizeAndValidateManualAssignment(manualAssignment, topics)
				this.partitionProvider = new ManualPartitionProvider({
					assignment: normalized,
					cluster: this.cluster,
					groupId: this.config.groupId,
					offsetManager: this.offsetManager!,
					autoOffsetReset: this.config.autoOffsetReset,
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
					offsetManager: this.offsetManager!,
					logger: this.logger,
					signal: this.abortController?.signal,
					isRunning: () => this.state === 'running',
				})
				this.logger.debug('starting consumer', { topics })
			}

			const callbacks = this.createProviderCallbacks()
			await this.partitionProvider.start(topics, callbacks)

			this.emit('running')

			await this.startFetchLoopAndWait(() =>
				this.runPollLoop(subscriptions, batchHandler, concurrency, autoCommitIntervalMs, autoCommit)
			)
		} catch (error) {
			this.emitError(error)
			throw error
		} finally {
			await this.finalizeRun()
		}
	}

	/**
	 * Consume messages one at a time.
	 *
	 * Starts the consumer and invokes the handler for each message. The promise resolves
	 * when {@link stop} is called, the abort signal fires, or a fatal error occurs.
	 *
	 * @param subscription - Topic(s) to consume from
	 * @param handler - Async function called for each message
	 * @param options - Optional configuration for offsets, concurrency, and assignment
	 * @throws If the consumer is already running
	 *
	 * @example
	 * ```ts
	 * await consumer.runEach('my-topic', async (message, ctx) => {
	 *   console.log(message.value)
	 * })
	 * ```
	 */
	async runEach<S extends SubscriptionInput>(
		subscription: S,
		handler: MessageHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunEachOptions
	): Promise<void> {
		const opts = { ...DEFAULT_RUN_EACH_OPTIONS, ...options }
		const concurrency = opts.partitionConcurrency ?? DEFAULT_RUN_EACH_OPTIONS.partitionConcurrency

		await this.run(
			subscription,
			opts,
			concurrency,
			async (topic, partition, records, decoders, ctx) => {
				for (const record of records) {
					if (ctx.signal.aborted) return

					const message = decodeRecord(topic, partition, record, decoders)
					const consumeCtx: ConsumeContext = {
						signal: ctx.signal,
						topic,
						partition,
						offset: record.offset,
					}

					await handler(message as Message<MsgOf<S>, KeyOf<S>>, consumeCtx)

					if (ctx.commitOffsets) {
						ctx.offsetManager.markConsumed(topic, partition, record.offset)
					}
				}
			},
			opts.autoCommitIntervalMs ?? DEFAULT_RUN_EACH_OPTIONS.autoCommitIntervalMs,
			opts.autoCommit !== false
		)
	}

	/**
	 * Consume messages in batches.
	 *
	 * Starts the consumer and invokes the handler with batches of messages from each partition.
	 * Offsets are committed only after the handler completes successfully, providing at-least-once
	 * semantics. The promise resolves when {@link stop} is called, the abort signal fires, or
	 * a fatal error occurs.
	 *
	 * @param subscription - Topic(s) to consume from
	 * @param handler - Async function called for each batch of messages
	 * @param options - Optional configuration for offsets, concurrency, and assignment
	 * @throws If the consumer is already running
	 *
	 * @example
	 * ```ts
	 * await consumer.runBatch('my-topic', async (messages, ctx) => {
	 *   for (const msg of messages) {
	 *     console.log(msg.value)
	 *   }
	 * })
	 * ```
	 */
	async runBatch<S extends SubscriptionInput>(
		subscription: S,
		handler: BatchHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunBatchOptions
	): Promise<void> {
		const opts = { ...DEFAULT_RUN_BATCH_OPTIONS, ...options }
		const concurrency = opts.partitionConcurrency ?? DEFAULT_RUN_BATCH_OPTIONS.partitionConcurrency

		await this.run(
			subscription,
			opts,
			concurrency,
			async (topic, partition, records, decoders, ctx) => {
				if (records.length === 0) return

				const messages = records.map(r => decodeRecord(topic, partition, r, decoders))
				const lastRecord = records[records.length - 1]!
				const consumeCtx: ConsumeContext = {
					signal: ctx.signal,
					topic,
					partition,
					offset: lastRecord.offset,
				}

				await handler(messages as Message<MsgOf<S>, KeyOf<S>>[], consumeCtx)

				if (ctx.commitOffsets) {
					for (const record of records) {
						ctx.offsetManager.markConsumed(topic, partition, record.offset)
					}
				}
			},
			opts.autoCommitIntervalMs ?? DEFAULT_RUN_BATCH_OPTIONS.autoCommitIntervalMs,
			opts.autoCommit !== false
		)
	}

	/**
	 * Consume messages as an async iterable.
	 *
	 * Returns an async iterator that yields messages one at a time. This is useful for
	 * integrating with `for await...of` loops and stream processing pipelines.
	 * The iterator completes when you break out of the loop or call {@link stop}.
	 *
	 * @param subscription - Topic(s) to consume from
	 * @param options - Optional configuration for offsets and assignment
	 * @throws If the consumer is already running
	 *
	 * @example
	 * ```ts
	 * for await (const { message, ctx } of consumer.stream('my-topic')) {
	 *   console.log(message.value)
	 *   if (shouldStop) break
	 * }
	 * ```
	 */
	async *stream<S extends SubscriptionInput>(
		subscription: S,
		options?: StreamOptions
	): AsyncIterable<{ message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }> {
		const commitOffsets = options?.commitOffsets !== false
		const autoCommitIntervalMs: number = options?.autoCommitIntervalMs ?? 5000
		this.startRun(commitOffsets, options?.signal)

		try {
			const manualAssignment = options?.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)
			const decoders = buildDecoderMaps(subscriptions)

			const useConsumerGroup = !manualAssignment
			this.initComponents(1, useConsumerGroup, this.logger)

			const offsetManager = this.offsetManager!
			const fetchManager = this.fetchManager!
			const signal = this.abortController!.signal

			if (manualAssignment) {
				const normalized = this.normalizeAndValidateManualAssignment(manualAssignment, topics)
				this.partitionProvider = new ManualPartitionProvider({
					assignment: normalized,
					cluster: this.cluster,
					groupId: this.config.groupId,
					offsetManager,
					autoOffsetReset: this.config.autoOffsetReset,
					signal,
				})
				this.logger.debug('starting consumer stream', {
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
					offsetManager,
					logger: this.logger,
					signal,
					isRunning: () => this.state === 'running',
				})
				this.logger.debug('starting consumer stream', { topics })
			}

			const callbacks = this.createProviderCallbacks()
			await this.partitionProvider.start(topics, callbacks)
			this.emit('running')

			if (this.commitOffsets) {
				offsetManager.startAutoCommit(autoCommitIntervalMs)
			}

			const partitionTracker = this.partitionTracker!
			const partitionProvider = this.partitionProvider

			while (this.state === 'running' && !signal.aborted) {
				const batches = await fetchManager.poll()

				// Handle any pending rebalance after poll returns but before processing
				// This ensures revoked partitions are removed before we check assignments
				await partitionProvider.checkAndHandleRebalance()
				if (this.state !== 'running' || signal.aborted) break

				if (batches.length === 0) {
					continue
				}

				for (const { topic, partition, records } of batches) {
					// Skip batches from partitions that were revoked during rebalance
					if (!fetchManager.isPartitionAssigned(topic, partition)) {
						continue
					}

					// Mark partition as processing - returns false if partition is not assigned or being revoked
					if (!partitionTracker.startProcessing(topic, partition)) {
						continue
					}

					try {
						for (const record of records) {
							const message = decodeRecord(topic, partition, record, decoders)
							const ctx: ConsumeContext = {
								signal,
								topic,
								partition,
								offset: record.offset,
							}

							yield { message, ctx } as { message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }

							// Mark consumed after yield returns - user has received message
							if (this.commitOffsets) {
								offsetManager.markConsumed(topic, partition, record.offset)
							}

							if (this.state !== 'running' || signal.aborted) {
								return
							}
						}
					} finally {
						// Mark processing complete - this unblocks any pending revoke() wait
						partitionTracker.endProcessing(topic, partition)
					}
				}
			}
		} finally {
			await this.finalizeRun({ logCleanup: false, logStop: false })
		}
	}

	/**
	 * Stop the consumer gracefully.
	 *
	 * Signals the consumer to stop fetching and exit the run loop. Any pending offsets
	 * are committed before shutdown completes. This method is idempotent and safe to
	 * call multiple times.
	 */
	stop(): void {
		if (this.state === 'idle' || this.state === 'stopping') {
			return
		}

		this.logger.debug('stopping consumer')
		this.state = 'stopping'
		this.abortController?.abort()

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

	private async cleanup(): Promise<void> {
		if (this.fetchManager) {
			this.fetchManager.stop()
		}

		// Wait for fetch loop to exit before committing - ensures last message was marked consumed
		const fetchLoopPromise = this.fetchLoopPromise
		this.fetchLoopPromise = null
		if (fetchLoopPromise) {
			try {
				await fetchLoopPromise
			} catch (error) {
				this.emitError(error)
			}
		}

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

		// Handles leaving consumer group
		if (this.partitionProvider) {
			try {
				await this.partitionProvider.stop()
			} catch (error) {
				this.emitError(error)
			}
		}

		this.consumerGroup = null
		this.offsetManager = null
		this.fetchManager = null
		this.partitionTracker?.clear()
		this.partitionTracker = null
		this.partitionProvider = null
		this.abortController = null
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
