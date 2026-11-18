/**
 * High-level Kafka consumer
 *
 * Main public API for consuming messages from Kafka.
 * Supports single message, batch, and stream modes.
 */

import { EventEmitter } from 'node:events'
import type { Cluster } from '@/client/cluster.js'
import { CommitFailedError, KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode, isGenerationLostErrorCode } from '@/protocol/messages/error-codes.js'
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
	BatchContext,
	MessageHandler,
	BatchHandler,
	RunEachOptions,
	RunBatchOptions,
	StreamOptions,
	TopicOf,
	TopicPartition,
	ManualAssignment,
	PartitionBatch,
	ConsumerMetrics,
	CommitOffset,
	CommittedOffset,
	ConsumerGroupMetadata,
} from './types.js'
import { ProcessOperation, diagnostics, register, trace } from '@/diagnostics.js'
import {
	DEFAULT_CONSUMER_CONFIG,
	DEFAULT_RUN_EACH_OPTIONS,
	DEFAULT_RUN_BATCH_OPTIONS,
	toTopicSubscription,
	type SubscriptionLike,
} from './types.js'
import { ConsumerGroup } from './consumer-group.js'
import { OffsetManager, type OffsetRequestOptions } from './offset-manager.js'
import { FetchManager } from './fetch-manager.js'
import { SHUTDOWN_DEADLINE_MS, withTimeout, DeadlineExceededError } from '@/utils/timeout.js'
import { PartitionTracker } from './partition-tracker.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey } from '@/utils/topic-partition.js'
import { validatePriorityStrategy, type PriorityStrategy } from './priority.js'

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
	/**
	 * Batches drained from the fetch buffer that the run loop has not handed to a handler yet. The FetchManager
	 * appends them as it drains them and counts them as not delivered (see FetchManagerConfig.undelivered); a batch
	 * leaves this list just before its handler is invoked.
	 */
	private readonly undelivered: PartitionBatch[] = []
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runPromiseReject: ((error: Error) => void) | null = null
	private shutdownTimer: ReturnType<typeof setTimeout> | null = null

	constructor(cluster: Cluster, config: ConsumerConfig) {
		super()
		this.cluster = cluster
		this.logger = cluster.getLogger()?.child({ component: 'consumer', groupId: config.groupId }) ?? noopLogger
		register({ type: 'consumer', clientId: cluster.clientId, groupId: config.groupId }, this)
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
			checkCrcs: config.checkCrcs ?? DEFAULT_CONSUMER_CONFIG.checkCrcs,
			partitionAssignmentStrategy:
				config.partitionAssignmentStrategy ?? DEFAULT_CONSUMER_CONFIG.partitionAssignmentStrategy,
			defaultApiTimeoutMs: config.defaultApiTimeoutMs ?? DEFAULT_CONSUMER_CONFIG.defaultApiTimeoutMs,
			onBeforeRebalance: config.onBeforeRebalance,
			onPartitionsRevoked: config.onPartitionsRevoked,
			onPartitionsAssigned: config.onPartitionsAssigned,
			onPartitionsLost: config.onPartitionsLost,
		}
		if (!Number.isInteger(this.config.maxBytesPerPartition) || this.config.maxBytesPerPartition <= 0) {
			throw new Error('maxBytesPerPartition must be a positive integer')
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
		this.undelivered.length = 0

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

	private createConsumeContext(
		signal: AbortSignal,
		topic: string,
		partition: number,
		offset: bigint
	): ConsumeContext {
		const consumerGroup = this.consumerGroup

		if (!consumerGroup) {
			return { signal, topic, partition, offset, groupId: this.config.groupId }
		}

		return {
			signal,
			topic,
			partition,
			offset,
			groupId: this.config.groupId,
			consumerGroupMetadata: {
				groupId: this.config.groupId,
				generationId: consumerGroup.currentGenerationId,
				memberId: consumerGroup.currentMemberId,
				groupInstanceId: this.config.groupInstanceId ?? null,
			},
		}
	}

	private initComponents(
		partitionConcurrency: number,
		useConsumerGroup: boolean,
		logger?: Logger,
		priority?: PriorityStrategy
	): void {
		this.partitionTracker = new PartitionTracker({ logger })

		if (useConsumerGroup) {
			this.consumerGroup = new ConsumerGroup(this.cluster, this.config, logger)
		}
		this.offsetManager = new OffsetManager(
			this.cluster,
			this.config.groupId,
			useConsumerGroup ? this.config.groupInstanceId : undefined,
			logger,
			this.config.defaultApiTimeoutMs,
			this.config.isolationLevel
		)
		// Wire auto-commit failures back to the consumer so generation/coordinator
		// errors are surfaced as 'error' events and trigger a rejoin instead of
		// silently looping with an ever-growing consumedOffsets map.
		this.offsetManager.setCommitErrorHandler(error => this.handleAutoCommitError(error))
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
				checkCrcs: this.config.checkCrcs,
				priority,
				onError: error => this.emitError(error),
				onFetchPosition: (position, recordCount) => {
					this.emit('fetchPosition', position, recordCount)
				},
				onOffsetOutOfRange: position => this.emit('offsetOutOfRange', position),
				onOffsetReset: reset => this.emit('offsetReset', reset),
				undelivered: this.undelivered,
			},
			logger
		)
	}

	private createProviderCallbacks(): PartitionProviderCallbacks {
		const offsetManager = this.offsetManager!
		const fetchManager = this.fetchManager!
		const partitionTracker = this.partitionTracker!
		// Partitions reported lost (or revoked) already are not assigned or revoked (again).
		const stillAssigned = <T extends TopicPartition>(partitions: T[]) =>
			partitions.filter(tp => offsetManager.isPartitionAssigned(tp.topic, tp.partition))

		return {
			onRebalancePending: () => this.emit('rebalancePending'),
			onRebalance: async () => {
				// User hook awaited inside the rebalance protocol so transactional pipelines
				// can commit before partitions are revoked. Synchronous notification fires
				// after, so listeners see "rebalance starting" only once the hook has run.
				if (this.config.onBeforeRebalance) {
					await this.config.onBeforeRebalance()
				}
				this.emit('rebalance')
			},
			onPartitionsAssigned: async assigned => {
				this.sessionLost = false
				const partitions = assigned.map(({ topic, partition }) => ({ topic, partition }))
				offsetManager.addAssignedPartitions(partitions)
				partitionTracker.assign(partitions)
				// Held, not fetched, until the hook returned: what it pauses or seeks takes effect before any fetch.
				fetchManager.addPartitions(assigned, { held: true })
				await this.config.onPartitionsAssigned?.(assigned)
				// Lost while the hook ran (the session expired meanwhile): reported lost, not assigned.
				const kept = stillAssigned(assigned)
				if (kept.length === 0) return
				fetchManager.releaseHeld(kept)
				this.emit('partitionsAssigned', kept)
			},
			onPartitionsRevoked: async revoked => {
				await partitionTracker.revoke(revoked)
				let partitions = stillAssigned(revoked)
				if (partitions.length === 0) return
				await this.config.onPartitionsRevoked?.(partitions)
				// Lost while the hook ran (the session expired meanwhile): reported lost, not revoked as well.
				partitions = stillAssigned(partitions)
				if (partitions.length === 0) return
				// Commit the final positions BEFORE removing the partitions from the
				// assigned set. commitPendingOffsets() skips offsets for unassigned
				// partitions, so removing first would silently drop the last-consumed
				// offsets of the partitions being revoked and make the next owner
				// reprocess them.
				if (this.commitOffsets && !this.sessionLost) {
					// A deleted topic (the leader's metadata watch asks for this rebalance once metadata stops showing
					// it): its offsets cannot be committed (UNKNOWN_TOPIC_OR_PARTITION) and nobody will read it again.
					// Dropped, so that the rebalance goes on (Java: "Will continue to join group").
					offsetManager.clearPartitions(
						partitions.filter(tp => !this.cluster.getMetadata()?.topics.has(tp.topic))
					)
					try {
						await offsetManager.commitPendingOffsets()
					} catch (error) {
						// The broker may already be past the commit window (RebalanceInProgress
						// once the group left PreparingRebalance) or the generation may be gone.
						// Tolerate those: the revoke must still proceed so this consumer stops
						// fetching partitions it no longer owns; the offsets stay uncommitted and
						// the next owner may reprocess (at-least-once). Other errors are fatal.
						const code = error instanceof KafkaProtocolError ? error.errorCode : null
						const tolerable =
							code === ErrorCode.RebalanceInProgress || (code !== null && isGenerationLostErrorCode(code))
						if (!tolerable) {
							throw error
						}
						this.logger.warn('offset commit during revoke failed; continuing rebalance', {
							error: (error as Error).message,
						})
					}
				}
				offsetManager.removeAssignedPartitions(partitions)
				fetchManager.removePartitions(partitions)
				offsetManager.clearPartitions(partitions)
				this.emit('partitionsRevoked', partitions)
			},
			onPartitionsLost: partitions => {
				this.sessionLost = true
				offsetManager.removeAssignedPartitions(partitions)
				partitionTracker.release(partitions)
				offsetManager.clearPartitions(partitions)
				fetchManager.removePartitions(partitions)
				// Invoked synchronously, so its synchronous part runs before any other code learns of the loss.
				const handled = (async () => {
					await this.config.onPartitionsLost?.(partitions)
				})()
				this.emit('partitionsLost', partitions)
				return handled
			},
			onRebalanceComplete: () => this.emit('rebalanceComplete'),
			onOffsetReset: reset => this.emit('offsetReset', reset),
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
	 * Whether the group is rebalancing: from the moment it asked for a rebalance (`rebalancePending` event), while
	 * the running handlers finish and partitions move, until the rebalance finished (`rebalanceComplete` event).
	 */
	get rebalancePending(): boolean {
		return this.partitionProvider?.isRebalancing() ?? false
	}

	/**
	 * The group metadata of the consumer's last completed join: what offsets committed in a transaction for its
	 * assignment must carry (`producer` `sendOffsets({ consumerGroupMetadata })`). In `onPartitionsAssigned` it is
	 * already that of the assignment.
	 *
	 * Undefined with a manual assignment, before the first join, and from the moment the group rejected this
	 * member's session or generation until the next join completed.
	 */
	groupMetadata(): ConsumerGroupMetadata | undefined {
		return this.consumerGroup?.groupMetadata
	}

	/**
	 * Commit offsets of partitions assigned to this consumer, with optional metadata, for example from outside a
	 * handler or in `onPartitionsRevoked`. Each offset is the next one to read (the last processed offset + 1).
	 * Works with `commitOffsets: false`; offsets of partitions not assigned to this consumer are skipped.
	 *
	 * A moved, loading or slow coordinator is retried until `timeoutMs` (default `defaultApiTimeoutMs`) passed,
	 * then it rejects with `TimeoutError`. It rejects with `RebalanceInProgressError` while the group rebalances
	 * (commit again once it is done), and with `CommitFailedError` when this member's generation is gone; then the
	 * consumer rejoins the group. It also rejects with `CommitFailedError`, without committing anything, once the
	 * consumer rejoined or a partition of the call was revoked or lost while it was retried: the offsets were meant
	 * for an assignment that is gone. Aborting `signal` rejects with its reason.
	 *
	 * @throws If the consumer is not running
	 */
	async commit(offsets: readonly CommitOffset[], options?: OffsetRequestOptions): Promise<void> {
		const offsetManager = this.runningOffsetManager()
		const join = this.consumerGroup?.groupMetadata
		try {
			await offsetManager.commitExplicit(offsets, options)
		} catch (error) {
			// The coordinator rejected the generation this member still holds: rejoin. A join that changed meanwhile
			// (rejoined, or reset by a loss, which rejoins by itself) needs no other rebalance.
			if (
				error instanceof CommitFailedError &&
				join !== undefined &&
				this.consumerGroup?.groupMetadata === join
			) {
				this.consumerGroup.emit('rebalance')
			}
			throw error
		}
	}

	/**
	 * The committed offsets of partitions, with the metadata committed with them, keyed by `topic:partition`;
	 * `null` for a partition without one. Offsets of an open transaction are waited for, up to `timeoutMs`
	 * (default `defaultApiTimeoutMs`). Aborting `signal` rejects with its reason.
	 *
	 * @throws If the consumer is not running
	 */
	async committed(
		partitions: readonly TopicPartition[],
		options?: OffsetRequestOptions
	): Promise<Map<string, CommittedOffset | null>> {
		const committed = await this.runningOffsetManager().fetchCommittedOffsets(partitions, options)
		return new Map(
			partitions.map(({ topic, partition }) => {
				const key = tpKey(topic, partition)
				return [key, committed.get(key) ?? null]
			})
		)
	}

	private runningOffsetManager(): OffsetManager {
		if (this.state !== 'running' || !this.offsetManager) {
			throw new Error('Consumer is not running')
		}
		return this.offsetManager
	}

	/**
	 * Pause fetching from specific partitions.
	 *
	 * Useful for backpressure control when processing cannot keep up with incoming messages.
	 * The consumer remains in the group and continues heartbeating while paused.
	 *
	 * Once it returns, no record of these partitions fetched before is delivered, except to a handler of
	 * the partition that already runs; resuming delivers them again. A pause does not outlive the
	 * partition's assignment: under the eager protocol every rebalance assigns partitions anew, unpaused
	 * (pause them again in `onPartitionsAssigned`).
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
	 * Where this consumer stands right now, per assigned partition: what only the consumer knows
	 * between two commits. (Lag of the group's committed offsets is a broker-side measure.)
	 */
	metrics(): ConsumerMetrics {
		return { partitions: this.fetchManager?.partitionMetrics() ?? [] }
	}

	/** Call the application's handler as one traced operation (see `diagnostics.process`). */
	private tracedProcess(
		topic: string,
		partition: number,
		messages: Array<Message<unknown, unknown>>,
		batch: boolean,
		handle: () => Promise<void>
	): Promise<void> {
		return trace(
			diagnostics.process,
			() => new ProcessOperation(this.cluster.clientId, this.config.groupId, topic, partition, messages, batch),
			processing => (processing ? processing.run(handle) : handle())
		)
	}

	/**
	 * The first offset of the earliest batch of an assigned partition fetched but not delivered yet, else the
	 * fetch position (which moves past transaction markers and aborted records). Undefined when the partition is
	 * not assigned to this consumer.
	 *
	 * Delivery counts whole batches: a batch is delivered once its handler is called (`runBatch`) or its first
	 * record is handed out (`runEach`, `stream`), so while a batch is processed this is past its last record. It
	 * is an offset to commit only while no batch of the partition is being processed (in `onPartitionsRevoked`,
	 * for example); with `runEach` and `stream`, commit `ctx.offset + 1n` of the last processed record instead.
	 */
	nextOffset(topic: string, partition: number): bigint | undefined {
		return this.fetchManager?.nextOffset(topic, partition)
	}

	/**
	 * How many records of an assigned partition can be read beyond {@link nextOffset}: the partition's end as of
	 * its last fetch response (the last stable offset under 'read_committed', else the high watermark) minus
	 * `nextOffset`, at least 0. Undefined when the partition is not assigned, before its first fetch response,
	 * and while it is paused (its end is not followed then).
	 */
	currentLag(topic: string, partition: number): bigint | undefined {
		return this.fetchManager?.currentLag(topic, partition)
	}

	/**
	 * The offset the next fetch of an assigned partition starts at.
	 *
	 * Inside a `partitionsAssigned` listener this is the position the partition starts from (the
	 * committed offset, or the one resolved by `autoOffsetReset`). Returns undefined when the
	 * partition is not assigned to this consumer.
	 */
	position(topic: string, partition: number): bigint | undefined {
		return this.fetchManager?.getFetchPosition(topic, partition)
	}

	/**
	 * Commit the position a partition starts from, for partitions that have no committed offset.
	 *
	 * Without it, such a partition is resolved by `autoOffsetReset` again after a restart, and with
	 * `'latest'` that skips whatever arrived in between. Call it from a `partitionsAssigned`
	 * listener, optionally after `seek()`. Partitions that are not assigned are ignored.
	 *
	 * @throws If records of a partition were already fetched: its position is then past records
	 * no handler has seen, and committing it would skip them.
	 */
	async commitStartPosition(partitions: Array<{ topic: string; partition: number }>): Promise<void> {
		if (!this.offsetManager || !this.fetchManager) return
		const positions: Array<{ topic: string; partition: number; offset: bigint }> = []
		for (const { topic, partition } of partitions) {
			const offset = this.position(topic, partition)
			if (offset === undefined) continue
			if (this.fetchManager.hasFetched(topic, partition)) {
				throw new Error(
					`commitStartPosition: ${topic}-${partition} has already fetched records; ` +
						'its position is no longer where it started'
				)
			}
			positions.push({ topic, partition, offset })
		}
		await this.offsetManager.commitPositions(positions)
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
	 * Records of the partition fetched but not delivered yet are dropped: once it returns, no record from
	 * before the seek is delivered, except to a handler of the partition that already runs.
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
		const maxConcurrency = Math.max(1, Math.floor(concurrency)) || 1
		// One handler per partition, keyed by topic:partition. FetchBuffer keeps records for
		// these partitions buffered so a slow partition cannot occupy a free slot.
		const active = new Map<string, Promise<void>>()
		const pending = this.undelivered
		let handlerError: Error | null = null

		const runHandler = async (batch: PartitionBatch, key: string): Promise<void> => {
			try {
				await batchHandler(batch.topic, batch.partition, batch.records, decoders, handlerCtx)
			} catch (cause) {
				handlerError ??= cause instanceof Error ? cause : new Error(String(cause))
			} finally {
				partitionTracker.endProcessing(batch.topic, batch.partition)
				active.delete(key)
				fetchManager.wakePoll()
			}
		}

		// Start handlers for pending batches whose partition is idle, up to maxConcurrency. A batch leaves
		// `pending` before its handler is invoked, so the handler's own pause() or nextOffset() treats it as
		// delivered. Batches voided since they were drained are dropped: their partition was revoked, sought,
		// or paused (the pause rewound the partition to them, so they are fetched again once it is resumed).
		const admitPending = (): void => {
			for (let index = 0; index < pending.length;) {
				if (active.size >= maxConcurrency || partitionProvider.hasPendingRebalance()) return
				const batch = pending[index]!
				const key = tpKey(batch.topic, batch.partition)
				if (active.has(key)) {
					index++
					continue
				}
				pending.splice(index, 1)
				if (
					fetchManager.isBatchAssigned(batch) &&
					partitionTracker.startProcessing(batch.topic, batch.partition)
				) {
					active.set(key, runHandler(batch, key))
				}
			}
		}

		const waitForAllHandlers = () => Promise.all(active.values())

		try {
			while (this.state === 'running' && !signal.aborted && !handlerError) {
				if (partitionProvider.hasPendingRebalance()) {
					// Stop admission and let active handlers finish before changing the assignment.
					await waitForAllHandlers()
					if (handlerError) break
					await partitionProvider.checkAndHandleRebalance()
					continue
				}

				admitPending()
				if (handlerError || partitionProvider.hasPendingRebalance()) continue

				if (pending.length > 0 || active.size >= maxConcurrency) {
					// Leftover pending batches belong to active partitions, so a handler is running.
					await Promise.race(active.values())
					continue
				}

				// The drained batches land in `pending` (this.undelivered).
				await fetchManager.poll(active)
			}
		} finally {
			await waitForAllHandlers()
		}

		if (handlerError) throw handlerError
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
			if (opts.priority) validatePriorityStrategy(opts.priority, topics)

			const useConsumerGroup = !manualAssignment
			this.initComponents(concurrency, useConsumerGroup, this.logger, opts.priority)

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
	async runEach<const S extends SubscriptionInput>(
		subscription: S,
		handler: MessageHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunEachOptions<NoInfer<TopicOf<S>>>
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
					const consumeCtx = this.createConsumeContext(ctx.signal, topic, partition, record.offset)

					await this.tracedProcess(topic, partition, [message], false, () =>
						handler(message as Message<MsgOf<S>, KeyOf<S>>, consumeCtx)
					)

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
	async runBatch<const S extends SubscriptionInput>(
		subscription: S,
		handler: BatchHandler<MsgOf<S>, KeyOf<S>>,
		options?: RunBatchOptions<NoInfer<TopicOf<S>>>
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
				let resolved = 0
				const resolveOffset = (offset: bigint): void => {
					if (!ctx.commitOffsets) return
					while (resolved < records.length && records[resolved]!.offset <= offset) {
						ctx.offsetManager.markConsumed(topic, partition, records[resolved]!.offset)
						resolved++
					}
				}
				const batchCtx: BatchContext = {
					...this.createConsumeContext(ctx.signal, topic, partition, lastRecord.offset),
					resolveOffset,
				}

				await this.tracedProcess(topic, partition, messages, true, () =>
					handler(messages as Message<MsgOf<S>, KeyOf<S>>[], batchCtx)
				)

				// A handler of a stopped run may have returned early: only what it resolved itself is marked.
				if (!ctx.signal.aborted) resolveOffset(lastRecord.offset)
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
	async *stream<const S extends SubscriptionInput>(
		subscription: S,
		options?: StreamOptions<NoInfer<TopicOf<S>>>
	): AsyncIterable<{ message: Message<MsgOf<S>, KeyOf<S>>; ctx: ConsumeContext }> {
		const commitOffsets = options?.commitOffsets !== false
		const autoCommitIntervalMs: number = options?.autoCommitIntervalMs ?? 5000
		this.startRun(commitOffsets, options?.signal)

		try {
			const manualAssignment = options?.assignment
			const { subscriptions, topics } = this.getSubscriptionsAndTopics(subscription)
			if (options?.priority) validatePriorityStrategy(options.priority, topics)
			const decoders = buildDecoderMaps(subscriptions)

			const useConsumerGroup = !manualAssignment
			this.initComponents(1, useConsumerGroup, this.logger, options?.priority)

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

			// The FetchManager appends the batches it drains to `pending` (this.undelivered).
			const pending = this.undelivered
			while (this.state === 'running' && !signal.aborted) {
				await fetchManager.poll()

				// Handle any pending rebalance after poll returns but before processing
				// This ensures revoked partitions are removed before we check assignments
				await partitionProvider.checkAndHandleRebalance()
				if (this.state !== 'running' || signal.aborted) break

				// A batch leaves `pending` when its records are handed out (see runPollLoop). Batches voided
				// meanwhile are skipped: their partition was revoked (an eager rebalance also voids the batches
				// of a partition it re-assigns), sought or paused.
				for (let batch = pending.shift(); batch !== undefined; batch = pending.shift()) {
					const { topic, partition, records } = batch
					if (!fetchManager.isBatchAssigned(batch)) {
						continue
					}

					// Mark partition as processing - returns false if partition is not assigned or being revoked
					if (!partitionTracker.startProcessing(topic, partition)) {
						continue
					}

					try {
						for (const record of records) {
							const message = decodeRecord(topic, partition, record, decoders)
							const ctx = this.createConsumeContext(signal, topic, partition, record.offset)

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

		this.armShutdownDeadline()

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
	 * Keep shutdown bounded when the run loop does not wind down in time because it is parked at an
	 * unresponsive coordinator (e.g. in a JoinGroup): force-close the coordinator connection.
	 *
	 * Application code that is still working on a record is not that case. The member has to keep
	 * its session until that code returns, or its final commit and LeaveGroup would fail.
	 */
	private armShutdownDeadline(): void {
		this.clearShutdownTimer()
		this.shutdownTimer = setTimeout(() => {
			if (this.partitionTracker?.hasInFlightProcessing) this.armShutdownDeadline()
			else this.partitionProvider?.interrupt?.()
		}, SHUTDOWN_DEADLINE_MS)
		this.shutdownTimer.unref?.()
	}

	private clearShutdownTimer(): void {
		if (this.shutdownTimer) {
			clearTimeout(this.shutdownTimer)
			this.shutdownTimer = null
		}
	}

	private async cleanup(): Promise<void> {
		this.clearShutdownTimer()
		if (this.fetchManager) {
			this.fetchManager.stop()
		}

		let commitTimedOut = false

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
					// Best effort within the shutdown budget (healthy broker: returns immediately).
					await withTimeout(
						this.offsetManager.commitPendingOffsets(),
						SHUTDOWN_DEADLINE_MS,
						'final offset commit'
					)
				} catch (error) {
					this.emitError(error)
					commitTimedOut = error instanceof DeadlineExceededError
				}
			}
		}

		// Handles leaving consumer group. If the final commit timed out the broker is unresponsive:
		// skip LeaveGroup (the session expires broker-side) and just close the connection.
		if (this.partitionProvider) {
			try {
				await this.partitionProvider.stop(commitTimedOut ? 0 : SHUTDOWN_DEADLINE_MS)
			} catch (error) {
				this.emitError(error)
			}
		}

		this.consumerGroup = null
		this.offsetManager = null
		this.fetchManager = null
		this.undelivered.length = 0
		this.partitionTracker?.clear()
		this.partitionTracker = null
		this.partitionProvider = null
		this.abortController = null
		this.runPromiseReject = null
	}

	private handleAutoCommitError(error: Error): void {
		const code = error instanceof KafkaProtocolError ? error.errorCode : null

		// RebalanceInProgress is expected during a rebalance — the consumed offsets are
		// retained so the revoke-time commit during the rejoin can retry them. Wiping
		// them here would make the next owner resume from a stale committed offset.
		if (code === ErrorCode.RebalanceInProgress) {
			this.consumerGroup?.emit('rebalance')
			return
		}

		this.emitError(error)

		// Generation lost: stale offsets can't be committed under this generation;
		// drop them and rejoin so the new generation resumes from the broker's
		// last committed offset (otherwise consumedOffsets grows unboundedly).
		if (code !== null && isGenerationLostErrorCode(code)) {
			this.offsetManager?.clearConsumedOffsets()
			this.consumerGroup?.emit('rebalance')
		}
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
