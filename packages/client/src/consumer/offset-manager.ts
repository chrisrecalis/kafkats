/**
 * Offset manager for consumer
 *
 * Handles:
 * - Tracking consumed offsets
 * - Auto-commit loop
 * - Committing explicit offsets (with metadata) for the application
 * - Fetching committed offsets
 * - Resolving starting offsets (earliest/latest/committed)
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	CommitFailedError,
	isConnectionFailure,
	isKafkaError,
	KafkaProtocolError,
	NoOffsetForPartitionError,
	RebalanceInProgressError,
	shouldRefreshMetadata,
	TimeoutError,
} from '@/client/errors.js'
import { ISOLATION_LEVEL } from '@/protocol/messages/requests/fetch.js'
import { OFFSET_TIMESTAMP } from '@/protocol/messages/requests/list-offsets.js'
import type { OffsetCommitPartition, OffsetCommitTopic } from '@/protocol/messages/requests/offset-commit.js'
import type { AutoOffsetReset, CommitOffset, CommittedOffset, IsolationLevel, TopicPartition } from './types.js'
import { diagnostics, trace, type CommitDiagnostics } from '@/diagnostics.js'
import { noopLogger, type Logger } from '@/logger.js'
import { parseKey, tpKey } from '@/utils/topic-partition.js'
import { abortable } from '@/utils/abortable.js'
import { retry } from '@/utils/retry.js'
import { sleep } from '@/utils/sleep.js'

/** Bounds of a coordinator request made on the application's behalf. */
export interface OffsetRequestOptions {
	/** Time budget of the call, retries included (default: the consumer's `defaultApiTimeoutMs`). */
	timeoutMs?: number
	/** Aborting it rejects the call with the signal's reason. */
	signal?: AbortSignal
}

/** Backoff between attempts of an explicit commit: 100 ms, doubling up to 1 s. */
const COMMIT_RETRY_INITIAL_DELAY_MS = 100
const COMMIT_RETRY_MAX_DELAY_MS = 1_000

/** OffsetCommit errors a later attempt can overcome: the coordinator moved, is loading or was slow. */
const RETRIABLE_COMMIT_ERRORS: ReadonlySet<ErrorCode> = new Set([
	ErrorCode.CoordinatorLoadInProgress,
	ErrorCode.NotCoordinator,
	ErrorCode.CoordinatorNotAvailable,
	ErrorCode.RequestTimedOut,
])

/** OffsetCommit errors that mean this member's generation is gone. */
const GENERATION_LOST_COMMIT_ERRORS: ReadonlySet<ErrorCode> = new Set([
	ErrorCode.IllegalGeneration,
	ErrorCode.UnknownMemberId,
	ErrorCode.FencedInstanceId,
])

/**
 * Offset manager
 */
export class OffsetManager {
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly groupInstanceId?: string
	private readonly logger: Logger
	private readonly apiTimeoutMs: number
	/** Isolation level of ListOffsets requests, as sent on the wire. */
	private readonly isolationLevel: number

	// Current state (set by consumer group on join/rejoin)
	private memberId: string = ''
	private generationId: number = -1
	private coordinator: Broker | null = null

	// Consumed offsets (next offset to commit = consumed + 1)
	private readonly consumedOffsets: Map<string, bigint> = new Map()

	// Currently assigned partitions (Java client model protection)
	// Prevents markConsumed for revoked partitions during rebalance race
	private readonly assignedPartitions: Set<string> = new Set()

	// Auto-commit state
	private autoCommitTimer: ReturnType<typeof setTimeout> | null = null
	private abortController: AbortController | null = null

	// Optional callback so the Consumer can react to commit failures
	// (e.g. trigger rejoin on IllegalGeneration).
	private onCommitError: ((error: Error) => void) | null = null

	/**
	 * @param isolationLevel - The consumer's: under 'read_committed', the 'latest' offset is the last stable offset
	 *   (the end of the committed records), as the consumer can read up to there.
	 */
	constructor(
		cluster: Cluster,
		groupId: string,
		groupInstanceId?: string,
		logger?: Logger,
		apiTimeoutMs = 60000,
		isolationLevel: IsolationLevel = 'read_uncommitted'
	) {
		this.cluster = cluster
		this.groupId = groupId
		this.groupInstanceId = groupInstanceId
		this.apiTimeoutMs = apiTimeoutMs
		this.isolationLevel =
			isolationLevel === 'read_committed' ? ISOLATION_LEVEL.READ_COMMITTED : ISOLATION_LEVEL.READ_UNCOMMITTED
		this.logger = logger?.child({ component: 'offset-manager', groupId }) ?? noopLogger
	}

	setCommitErrorHandler(handler: ((error: Error) => void) | null): void {
		this.onCommitError = handler
	}

	/**
	 * Update group state after join/rejoin
	 */
	updateGroupState(memberId: string, generationId: number, coordinator: Broker): void {
		this.memberId = memberId
		this.generationId = generationId
		this.coordinator = coordinator
	}

	/**
	 * Clear consumed offsets (e.g., after eager rebalance)
	 */
	clearConsumedOffsets(): void {
		this.consumedOffsets.clear()
	}

	/**
	 * Clear consumed offsets for specific partitions only (for cooperative rebalance)
	 */
	clearPartitions(partitions: TopicPartition[]): void {
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			this.consumedOffsets.delete(key)
		}
	}

	/**
	 * Add partitions to the assigned set (Java client model).
	 * Called after rebalance when new partitions are assigned.
	 * Additive: does not clear existing partitions (important for cooperative rebalance
	 * where onPartitionsAssigned only receives newly added partitions, not kept ones).
	 */
	addAssignedPartitions(partitions: TopicPartition[]): void {
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			this.assignedPartitions.add(key)
		}
		this.logger.debug('assigned partitions added', {
			added: partitions.length,
			total: this.assignedPartitions.size,
		})
	}

	/**
	 * Remove partitions from the assigned set (for cooperative rebalance revoke).
	 * Called when partitions are revoked during incremental rebalance.
	 */
	removeAssignedPartitions(partitions: TopicPartition[]): void {
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			this.assignedPartitions.delete(key)
		}
		this.logger.debug('partitions removed from assignment', {
			removedCount: partitions.length,
			remainingCount: this.assignedPartitions.size,
		})
	}

	/**
	 * Check if a partition is currently assigned
	 */
	isPartitionAssigned(topic: string, partition: number): boolean {
		const key = tpKey(topic, partition)
		return this.assignedPartitions.has(key)
	}

	/**
	 * Mark a message as consumed
	 * The committed offset will be the next offset (consumed + 1)
	 *
	 * Protected by partition assignment check (Java client model):
	 * If the partition was revoked during rebalance, this is a no-op to prevent
	 * committing offsets for partitions we no longer own.
	 */
	markConsumed(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)

		// Protect against race condition: handler completes after partition revoked
		if (!this.assignedPartitions.has(key)) {
			this.logger.debug('ignoring markConsumed for unassigned partition', {
				topic,
				partition,
				offset: offset.toString(),
			})
			return
		}

		const current = this.consumedOffsets.get(key) ?? -1n
		if (offset > current) {
			this.consumedOffsets.set(key, offset)
		}
	}

	/**
	 * Get the number of pending offset commits
	 */
	get pendingCommitCount(): number {
		return this.consumedOffsets.size
	}

	/**
	 * Start auto-commit loop
	 */
	startAutoCommit(intervalMs: number): void {
		if (this.autoCommitTimer) {
			return
		}

		this.logger.debug('starting auto-commit', { intervalMs })
		this.abortController = new AbortController()

		const runAutoCommit = () => {
			if (this.abortController?.signal.aborted) {
				return
			}

			this.commitPendingOffsets()
				.then(() => {
					this.logger.debug('auto-commit completed', { success: true })
				})
				.catch((error: Error) => {
					this.logger.error('auto-commit failed', { error: error.message })
					if (this.onCommitError) {
						try {
							this.onCommitError(error)
						} catch (handlerError) {
							this.logger.error('auto-commit error handler threw', {
								error: (handlerError as Error).message,
							})
						}
					}
				})
				.finally(() => {
					// Schedule next commit
					if (!this.abortController?.signal.aborted) {
						this.autoCommitTimer = setTimeout(runAutoCommit, intervalMs)
					}
				})
		}

		// Start first commit after interval
		this.autoCommitTimer = setTimeout(runAutoCommit, intervalMs)
	}

	/**
	 * Stop auto-commit loop
	 */
	stopAutoCommit(): void {
		this.abortController?.abort()
		if (this.autoCommitTimer) {
			clearTimeout(this.autoCommitTimer)
			this.autoCommitTimer = null
		}
	}

	/**
	 * Commit explicit positions (the next offset to read) of assigned partitions.
	 *
	 * Unlike {@link markConsumed}, this needs no record to have been consumed: it is how a
	 * position chosen by `autoOffsetReset` is made durable before anything was processed.
	 */
	async commitPositions(positions: Array<{ topic: string; partition: number; offset: bigint }>): Promise<void> {
		for (const { topic, partition, offset } of positions) {
			const key = tpKey(topic, partition)
			if (!this.assignedPartitions.has(key)) continue
			// Stored as "last consumed offset"; a position of 0 is "nothing consumed" (-1).
			const consumed = offset - 1n
			const current = this.consumedOffsets.get(key)
			if (current === undefined || consumed > current) {
				this.consumedOffsets.set(key, consumed)
			}
		}
		await this.commitPendingOffsets()
	}

	/**
	 * Commit all pending offsets
	 * Filters to only assigned partitions (Java client model protection)
	 */
	async commitPendingOffsets(): Promise<void> {
		const coordinator = this.coordinator
		if (this.consumedOffsets.size === 0 || !coordinator) {
			return
		}

		this.logger.debug('committing offsets', { count: this.consumedOffsets.size })

		const pending = Array.from(this.consumedOffsets, ([key, consumed]) => ({
			...parseKey(key),
			offset: consumed + 1n,
		}))
		const topics = commitTopics(this.assignedOnly(pending))
		if (topics.length === 0) {
			this.logger.debug('no assigned partitions to commit')
			return
		}

		await trace(
			diagnostics.commit,
			() => this.commitDiagnostics(topics),
			() => this.sendOffsetCommit(coordinator, topics)
		)
	}

	/**
	 * Commit the given offsets of assigned partitions, with their metadata and leader epochs, under this member's
	 * generation (-1 without a group). Unlike {@link commitPendingOffsets}, no record needs to have been consumed.
	 * Partitions not assigned to this member are skipped.
	 *
	 * A moved, loading or slow coordinator and connection failures are retried with backoff (100 ms doubling to 1 s)
	 * until `timeoutMs` passed, then the call rejects with a `TimeoutError` (cause: the last failure). Other failures
	 * are not retried: `RebalanceInProgressError` while the group rebalances, `CommitFailedError` when this member's
	 * generation is gone, `KafkaProtocolError` otherwise. Aborting `signal` rejects with its reason.
	 *
	 * Once committed, the consumed offsets the commit covers are dropped, so no later commit moves the partition back.
	 */
	async commitExplicit(offsets: readonly CommitOffset[], options: OffsetRequestOptions = {}): Promise<void> {
		options.signal?.throwIfAborted()
		const coordinator = this.coordinator
		if (!coordinator) {
			throw new Error('Cannot commit offsets before the group coordinator is known')
		}
		const topics = commitTopics(this.assignedOnly(offsets))
		if (topics.length === 0) {
			return
		}

		await trace(
			diagnostics.commit,
			() => this.commitDiagnostics(topics),
			() => this.commitUntilDone(coordinator, topics, options.timeoutMs ?? this.apiTimeoutMs, options.signal)
		)
	}

	/** The retry loop of {@link commitExplicit}. */
	private async commitUntilDone(
		coordinator: Broker,
		topics: OffsetCommitTopic[],
		timeoutMs: number,
		signal: AbortSignal | undefined
	): Promise<void> {
		// Aborted by the deadline or by the caller's signal; the caller's is checked first when telling them apart.
		const stop = new AbortController()
		const timer = setTimeout(() => stop.abort(), timeoutMs)
		const onAbort = () => stop.abort()
		signal?.addEventListener('abort', onAbort, { once: true })
		let lastError: unknown
		try {
			let delayMs = COMMIT_RETRY_INITIAL_DELAY_MS
			for (;;) {
				try {
					// The current coordinator: sendOffsetCommit re-discovers a moved one before it throws.
					return await abortable(this.sendOffsetCommit(this.coordinator ?? coordinator, topics), stop.signal)
				} catch (error) {
					if (stop.signal.aborted || !isRetriableCommitError(error)) throw error
					lastError = error
					this.logger.debug('retrying offset commit', { delayMs, error: (error as Error).message })
					await sleep(delayMs, { signal: stop.signal })
					delayMs = Math.min(2 * delayMs, COMMIT_RETRY_MAX_DELAY_MS)
				}
			}
		} catch (error) {
			signal?.throwIfAborted()
			if (stop.signal.aborted) {
				const timeout = new TimeoutError(`OffsetCommit for group ${this.groupId} did not succeed`, timeoutMs)
				if (lastError !== undefined) timeout.cause = lastError
				throw timeout
			}
			throw this.commitError(error)
		} finally {
			clearTimeout(timer)
			signal?.removeEventListener('abort', onAbort)
		}
	}

	/** The typed error of a commit failure that is not retried. */
	private commitError(error: unknown): unknown {
		if (!(error instanceof KafkaProtocolError)) return error
		if (error.errorCode === ErrorCode.RebalanceInProgress) return new RebalanceInProgressError(this.groupId)
		if (GENERATION_LOST_COMMIT_ERRORS.has(error.errorCode)) return new CommitFailedError(this.groupId, error)
		return error
	}

	/** The offsets of partitions assigned to this member: a commit for another partition would overwrite its owner's. */
	private assignedOnly(offsets: Iterable<CommitOffset>): CommitOffset[] {
		const assigned: CommitOffset[] = []
		for (const offset of offsets) {
			if (this.assignedPartitions.has(tpKey(offset.topic, offset.partition))) {
				assigned.push(offset)
			} else {
				this.logger.debug('skipping commit for unassigned partition', {
					topic: offset.topic,
					partition: offset.partition,
				})
			}
		}
		return assigned
	}

	private commitDiagnostics(topics: OffsetCommitTopic[]): CommitDiagnostics {
		return {
			clientId: this.cluster.clientId,
			groupId: this.groupId,
			offsets: topics.flatMap(topic =>
				topic.partitions.map(p => ({
					topic: topic.name,
					partition: p.partitionIndex,
					offset: p.committedOffset,
				}))
			),
		}
	}

	/**
	 * Send one OffsetCommit request and settle the pending offsets by its per-partition results: a committed
	 * partition's consumed offset is dropped unless it moved past the committed offset meanwhile.
	 */
	private async sendOffsetCommit(coordinator: Broker, topics: OffsetCommitTopic[]): Promise<void> {
		let response: Awaited<ReturnType<Broker['offsetCommit']>>
		try {
			response = await coordinator.offsetCommit({
				groupId: this.groupId,
				generationId: this.generationId,
				memberId: this.memberId,
				groupInstanceId: this.groupInstanceId ?? null,
				topics,
			})
		} catch (error) {
			if (isConnectionFailure(error)) {
				// The connection re-dials by itself; additionally re-resolve the coordinator in case it
				// moved to another node. Offsets stay pending for the next attempt.
				this.cluster.invalidateCoordinator('GROUP', this.groupId)
				this.coordinator = await this.cluster
					.getCoordinator('GROUP', this.groupId)
					.catch(() => this.coordinator)
			}
			throw error
		}

		const committed = new Map<string, bigint>()
		for (const topic of topics) {
			for (const partition of topic.partitions) {
				committed.set(tpKey(topic.name, partition.partitionIndex), partition.committedOffset)
			}
		}

		// Clear successes before throwing so a failed sibling doesn't poison healthy
		// partitions on the next retry (stale generation → forever-failing commits).
		let firstError: KafkaProtocolError | null = null

		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				const key = tpKey(topic.name, partition.partitionIndex)
				if (partition.errorCode === ErrorCode.None) {
					// The committed offset is the next one to read: it covers consumed offsets below it.
					const consumed = this.consumedOffsets.get(key)
					const offset = committed.get(key)
					if (consumed !== undefined && offset !== undefined && consumed < offset) {
						this.consumedOffsets.delete(key)
					}
					continue
				}
				this.logger.error('offset commit error', {
					topic: topic.name,
					partition: partition.partitionIndex,
					errorCode: partition.errorCode,
				})
				firstError ??= new KafkaProtocolError(
					partition.errorCode,
					`OffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
				)
			}
		}

		if (firstError) {
			// Re-discover the coordinator before the next attempt, else commits retry forever against
			// the stale one. The caller (auto-commit loop / revoke-time commit) drives re-attempts.
			if (
				firstError.errorCode === ErrorCode.NotCoordinator ||
				firstError.errorCode === ErrorCode.CoordinatorNotAvailable
			) {
				this.cluster.invalidateCoordinator('GROUP', this.groupId)
				this.coordinator = await this.cluster
					.getCoordinator('GROUP', this.groupId)
					.catch(() => this.coordinator)
			}
			throw firstError
		}

		this.logger.debug('offsets committed successfully')
	}

	/**
	 * Fetch the committed offsets of partitions, keyed by `topic:partition`; partitions without one are absent.
	 *
	 * Only stable offsets are read: while a transaction holds pending offsets, the fetch is retried, like other
	 * retriable errors and coordinator moves, with backoff until `timeoutMs` is spent; then the last error is thrown.
	 * Aborting `signal` rejects with its reason.
	 */
	async fetchCommittedOffsets(
		partitions: readonly TopicPartition[],
		options: OffsetRequestOptions = {}
	): Promise<Map<string, CommittedOffset>> {
		options.signal?.throwIfAborted()
		if (partitions.length === 0 || !this.coordinator) {
			return new Map()
		}

		this.logger.debug('fetching committed offsets', { partitionCount: partitions.length })

		// Build request
		const topicMap = new Map<string, number[]>()
		for (const tp of partitions) {
			if (!topicMap.has(tp.topic)) {
				topicMap.set(tp.topic, [])
			}
			topicMap.get(tp.topic)!.push(tp.partition)
		}

		const topics = Array.from(topicMap.entries()).map(([name, partitions]) => ({
			name,
			partitions: partitions.map(partitionIndex => ({ partitionIndex })),
		}))

		// Retriable top-level codes (coordinator loading/moved) must not fatally fail consumer
		// start/rebalance — retry with backoff and re-discover the coordinator.
		const fetching = retry(
			async () => {
				if (!this.coordinator) {
					throw new KafkaProtocolError(ErrorCode.CoordinatorNotAvailable, 'OffsetFetch: no coordinator')
				}
				const res = await this.coordinator.offsetFetch({
					groupId: this.groupId,
					topics,
					requireStable: true,
				})
				if (res.errorCode !== ErrorCode.None) {
					throw new KafkaProtocolError(res.errorCode, 'OffsetFetch failed')
				}
				// Partition-level errors must never be treated as "no committed offset" —
				// falling through to autoOffsetReset would silently lose the consumer's
				// position. Retriable codes are retried by the surrounding retry();
				// non-retriable ones propagate to the caller.
				for (const topic of res.topics) {
					for (const partition of topic.partitions) {
						if (partition.errorCode !== ErrorCode.None) {
							throw new KafkaProtocolError(
								partition.errorCode,
								`OffsetFetch failed for ${topic.name}-${partition.partitionIndex}`
							)
						}
					}
				}
				return res
			},
			{
				// Time-bounded by timeoutMs; maxAttempts is only a non-firing safety net.
				maxAttempts: 1_000,
				maxElapsedMs: options.timeoutMs ?? this.apiTimeoutMs,
				initialDelayMs: 100,
				maxDelayMs: 1_000,
				multiplier: 2,
				jitter: 0,
				signal: options.signal,
				shouldRetry: error => isKafkaError(error) && error.retriable,
				onRetry: async ({ error }) => {
					const errorCode = isKafkaError(error) ? error.errorCode : undefined
					if (errorCode === ErrorCode.NotCoordinator || errorCode === ErrorCode.CoordinatorNotAvailable) {
						this.cluster.invalidateCoordinator('GROUP', this.groupId)
						const refreshed = await this.cluster.getCoordinator('GROUP', this.groupId).catch(() => null)
						if (refreshed) this.coordinator = refreshed
					}
				},
			}
		)
		const response = await abortable(fetching, options.signal)

		// Build result map (partition-level errors were rejected above)
		const result = new Map<string, CommittedOffset>()
		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				// -1 means no committed offset
				if (partition.committedOffset < 0n) continue
				// The coordinator stores a commit without metadata as '', so '' reads back as none.
				const committed: CommittedOffset = {
					offset: partition.committedOffset,
					metadata: partition.metadata || null,
				}
				if (partition.committedLeaderEpoch >= 0) committed.leaderEpoch = partition.committedLeaderEpoch
				result.set(tpKey(topic.name, partition.partitionIndex), committed)
			}
		}

		this.logger.debug('committed offsets fetched', { count: result.size })
		return result
	}

	/**
	 * Resolve starting offset for a partition
	 *
	 * @param topic - Topic name
	 * @param partition - Partition index
	 * @param autoOffsetReset - Strategy when no committed offset exists: 'earliest', 'latest', or 'none'
	 * @param committedOffsets - Map of committed offsets (from fetchCommittedOffsets)
	 * @returns Starting offset
	 * @throws NoOffsetForPartitionError - No committed offset and the strategy is 'none'
	 */
	async resolveStartingOffset(
		topic: string,
		partition: number,
		autoOffsetReset: AutoOffsetReset,
		committedOffsets: ReadonlyMap<string, CommittedOffset>
	): Promise<bigint> {
		const committed = committedOffsets.get(tpKey(topic, partition))
		if (committed !== undefined) {
			return committed.offset
		}

		// No committed offset - apply reset strategy
		switch (autoOffsetReset) {
			case 'earliest':
				return this.getEarliestOffset(topic, partition)
			case 'latest':
				return this.getLatestOffset(topic, partition)
			case 'none':
				throw new NoOffsetForPartitionError(topic, partition)
		}
	}

	/**
	 * Get earliest offset for a partition
	 */
	async getEarliestOffset(topic: string, partition: number): Promise<bigint> {
		return this.listOffset(topic, partition, OFFSET_TIMESTAMP.EARLIEST)
	}

	/**
	 * Get latest offset for a partition: the last stable offset under 'read_committed', else the high watermark
	 */
	async getLatestOffset(topic: string, partition: number): Promise<bigint> {
		return this.listOffset(topic, partition, OFFSET_TIMESTAMP.LATEST)
	}

	/**
	 * List offset for a specific timestamp, at the consumer's isolation level
	 */
	private async listOffset(topic: string, partition: number, timestamp: bigint): Promise<bigint> {
		return retry(
			async () => {
				const leader = await this.cluster.getLeaderForPartition(topic, partition)

				const response = await leader.listOffsets({
					isolationLevel: this.isolationLevel,
					topics: [
						{
							name: topic,
							partitions: [
								{
									partitionIndex: partition,
									timestamp,
								},
							],
						},
					],
				})

				const topicResponse = response.topics.find(t => t.name === topic)
				const partitionResponse = topicResponse?.partitions.find(p => p.partitionIndex === partition)

				if (!partitionResponse) {
					throw new Error(`No offset response for ${topic}-${partition}`)
				}

				if (partitionResponse.errorCode !== ErrorCode.None) {
					throw new KafkaProtocolError(
						partitionResponse.errorCode,
						`ListOffsets failed for ${topic}-${partition}`
					)
				}

				return partitionResponse.offset
			},
			{
				maxAttempts: 5,
				initialDelayMs: 100,
				maxDelayMs: 2_000,
				multiplier: 2,
				jitter: 0,
				shouldRetry: error => isKafkaError(error) && error.retriable,
				onRetry: async ({ attempt, delayMs, error }) => {
					const errorCode = isKafkaError(error) ? error.errorCode : undefined
					if (errorCode !== undefined && shouldRefreshMetadata(errorCode)) {
						this.logger.debug('refreshing metadata due to listOffsets error', {
							topic,
							partition,
							errorCode,
							attempt,
						})
						await this.cluster.refreshMetadata([topic]).catch(() => {})
					}

					this.logger.debug('retrying listOffsets after error', {
						topic,
						partition,
						attempt,
						delayMs,
						error: error instanceof Error ? error.message : String(error),
					})
				},
			}
		)
	}
}

function isRetriableCommitError(error: unknown): boolean {
	return (
		isConnectionFailure(error) ||
		(error instanceof KafkaProtocolError && RETRIABLE_COMMIT_ERRORS.has(error.errorCode))
	)
}

/** The topics of an OffsetCommit request; of several offsets for one partition, the last is committed. */
function commitTopics(offsets: Iterable<CommitOffset>): OffsetCommitTopic[] {
	const topics = new Map<string, Map<number, OffsetCommitPartition>>()
	for (const { topic, partition, offset, metadata, leaderEpoch } of offsets) {
		let partitions = topics.get(topic)
		if (!partitions) {
			partitions = new Map()
			topics.set(topic, partitions)
		}
		partitions.set(partition, {
			partitionIndex: partition,
			committedOffset: offset,
			committedLeaderEpoch: leaderEpoch,
			committedMetadata: metadata ?? null,
		})
	}
	return Array.from(topics, ([name, partitions]) => ({ name, partitions: Array.from(partitions.values()) }))
}
