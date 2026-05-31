/**
 * ShareConsumer implements Kafka Share Groups (KIP-932), GA in Kafka 4.2.
 *
 * This is a separate consumer from the classic Consumer group implementation and uses:
 * - ShareGroupHeartbeat for membership
 * - ShareFetch for record acquisition (v1 = Kafka 4.1, v2 = Kafka 4.2 adds acquireMode and renew)
 * - ShareAcknowledge for per-record acknowledgements (v1 = Kafka 4.1, v2 = Kafka 4.2 adds renew)
 */

import { EventEmitter } from 'node:events'
import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import {
	BrokerNotAvailableError,
	CoordinatorNotAvailableError,
	KafkaError,
	KafkaFeatureDisabledError,
	KafkaFeatureUnsupportedError,
	KafkaProtocolError,
	LeaderNotAvailableError,
	NotCoordinatorError,
	UnsupportedVersionError,
} from '@/client/errors.js'
import { NetworkError } from '@/network/errors.js'
import { ErrorCode, getErrorName } from '@/protocol/messages/error-codes.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { decodeRecordBatchFrom, decodeRecordBatchFromSync } from '@/protocol/records/index.js'

import { normalizeDecoder } from '@/topic.js'

import type {
	ConsumeContext,
	ShareConsumerConfig,
	ShareConsumerEvents,
	ShareKeyOf,
	ShareMessage,
	ShareMessageHandler,
	ShareMsgOf,
	ShareRunEachOptions,
	ShareSubscriptionInput,
	SubscriptionLike,
	TopicPartition,
	TopicSubscription,
} from './types.js'
import { DEFAULT_SHARE_CONSUMER_CONFIG, DEFAULT_SHARE_RUN_EACH_OPTIONS } from './types.js'

import type { ShareGroupHeartbeatRequest } from '@/protocol/messages/requests/share-group-heartbeat.js'
import type { ShareFetchRequest } from '@/protocol/messages/requests/share-fetch.js'
import type { ShareAcknowledgeRequest } from '@/protocol/messages/requests/share-acknowledge.js'
import type { ShareFetchResponse } from '@/protocol/messages/responses/share-fetch.js'
import type { ShareAcknowledgeResponse } from '@/protocol/messages/responses/share-acknowledge.js'

import { noopLogger, type Logger } from '@/logger.js'
import { retry } from '@/utils/retry.js'
import { sleep } from '@/utils/sleep.js'
import { Decoder } from '@/protocol/primitives/decoder.js'

import type { ShareAcknowledgeRequestWithoutEpoch } from './ack-manager.js'
import { AckManager, ACK_GAP, ACK_ACCEPT, ACK_RELEASE, ACK_REJECT, ACK_RENEW } from './ack-manager.js'
import {
	SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED,
	SHARE_ACQUIRE_MODE_RECORD_LIMIT,
	type ShareAcquireMode,
} from '@/protocol/messages/requests/share-fetch.js'
import {
	convertHeaders,
	isControlBatch,
	nextShareSessionEpoch,
	randomKafkaUuid,
	SHARE_SESSION_INITIAL_EPOCH,
	toShareTopicSubscription,
	toTopicPartitionKey,
} from './share-consumer-helpers.js'
import { pmap } from '@/utils/pmap.js'

const EMPTY_BUFFER = Buffer.alloc(0)

// stream() back-pressure: with the fetch loop at concurrency 1, parking the handler once this many
// records are buffered keeps the loop from fetching (and broker-locking) unboundedly ahead of a slow
// async-iterator consumer. 1 most faithfully matches the pull model.
const STREAM_QUEUE_HIGH_WATER_MARK = 1

function isShareSessionError(code: ErrorCode): boolean {
	return (
		code === ErrorCode.ShareSessionNotFound ||
		code === ErrorCode.InvalidShareSessionEpoch ||
		code === ErrorCode.ShareSessionLimitReached
	)
}

// Transient infrastructure errors the fetch/heartbeat loops ride through (refresh metadata or
// re-resolve the coordinator and retry) instead of treating as fatal: a broker dying mid-request
// (socket closed / connect failed / request timeout), an unavailable broker or leader, a moved
// coordinator, or any retriable protocol error. Non-retriable errors (auth, feature disabled) fall
// through to fatal and stop the consumer.
function isTransientShareError(error: unknown): boolean {
	if (error instanceof NetworkError) return true
	if (error instanceof BrokerNotAvailableError || error instanceof LeaderNotAvailableError) return true
	if (error instanceof KafkaError) return error.retriable
	return false
}

// Per-partition ShareFetch errorCode classification. Most fetch errors are recoverable: skip the
// partition this round (its acquired records redeliver after the lock timeout) and, for the
// metadata-class errors, refresh metadata so the next round re-resolves the leader. Only a small set
// is fatal. Mirrors the Java ShareFetchCollector.handleInitializeErrors behaviour.
const FETCH_PARTITION_REFRESH_ERRORS = new Set<ErrorCode>([
	ErrorCode.NotLeaderOrFollower,
	ErrorCode.ReplicaNotAvailable,
	ErrorCode.KafkaStorageError,
	ErrorCode.FencedLeaderEpoch,
	ErrorCode.OffsetNotAvailable,
	ErrorCode.UnknownTopicOrPartition,
	ErrorCode.UnknownTopicId,
	ErrorCode.InconsistentTopicId,
])
const FETCH_PARTITION_SKIP_ONLY_ERRORS = new Set<ErrorCode>([
	ErrorCode.UnknownLeaderEpoch,
	ErrorCode.UnknownServerError,
])
function classifyFetchPartitionError(code: ErrorCode): 'refresh' | 'skip' | 'fatal' {
	if (FETCH_PARTITION_REFRESH_ERRORS.has(code)) return 'refresh'
	if (FETCH_PARTITION_SKIP_ONLY_ERRORS.has(code)) return 'skip'
	return 'fatal'
}

/**
 * Find a share-session-level error (ShareSessionNotFound / InvalidShareSessionEpoch) anywhere in a
 * ShareFetch/ShareAcknowledge response — at the top level OR in any partition's `errorCode` or
 * `acknowledgeErrorCode`. Such an error means the broker rejected the request without advancing its
 * session epoch, so the client must reset rather than advance. A per-partition session error must be
 * caught here (before advancing) because some callers (e.g. prefetch) discard the response entirely.
 */
function findShareSessionError(
	topLevel: ErrorCode,
	topics: Array<{ partitions: Array<{ errorCode: ErrorCode; acknowledgeErrorCode?: ErrorCode }> }>
): ErrorCode | null {
	if (isShareSessionError(topLevel)) {
		return topLevel
	}
	for (const topic of topics) {
		for (const partition of topic.partitions) {
			if (isShareSessionError(partition.errorCode)) {
				return partition.errorCode
			}
			if (partition.acknowledgeErrorCode !== undefined && isShareSessionError(partition.acknowledgeErrorCode)) {
				return partition.acknowledgeErrorCode
			}
		}
	}
	return null
}

class ShareMessageAlreadyHandledError extends Error {
	constructor(topic: string, partition: number, offset: bigint) {
		super(`Share message ${topic}[${partition}]@${offset} already handled`)
		this.name = 'ShareMessageAlreadyHandledError'
	}
}

type ShareConsumerState = 'idle' | 'running' | 'stopping'

type ShareFetchWorkItem = {
	topicName: string
	topicId: string
	partitionIndex: number
	record: DecodedRecord
	deliveryCount?: number
	decoder: (b: Buffer) => unknown
	keyDecoder: (b: Buffer) => unknown
}

export class ShareConsumer extends EventEmitter<ShareConsumerEvents> {
	private readonly cluster: Cluster
	private readonly logger: Logger
	private readonly config: Required<
		Pick<ShareConsumerConfig, 'groupId' | 'maxWaitMs' | 'minBytes' | 'maxBytes' | 'maxRecords' | 'batchSize'>
	> &
		Pick<ShareConsumerConfig, 'rackId'>
	private readonly acquireModeWire: ShareAcquireMode

	private state: ShareConsumerState = 'idle'
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runCleanupPromise: Promise<void> | null = null
	private runCleanupResolve: (() => void) | null = null

	private coordinator: Broker | null = null
	private memberId: string = randomKafkaUuid()
	private memberEpoch: number = 0
	private shareSessionEpochByBrokerId: Map<number, number> = new Map()
	// Partitions currently registered in each broker's share session (topicId -> partition set).
	// Used to compute forgottenTopicsData when partitions leave the assignment, since omitting a
	// partition from an incremental ShareFetch does NOT remove it from the broker's session.
	private shareSessionPartitionsByBrokerId: Map<number, Map<string, Set<number>>> = new Map()
	private shareSessionQueueByBrokerId: Map<number, Promise<void>> = new Map()
	private heartbeatIntervalMs: number = 1000
	private assignment: TopicPartition[] = []
	// Warm-up prefetch is done once per run (on the first assignment). prefetchAssignedPartitions
	// discards the records it acquires, so re-running it on every rebalance would needlessly redeliver.
	private didInitialPrefetch = false
	// Latest broker-reported acquisition-lock timeout (ms); how long the app has to process/renew a
	// record before its lock expires and it is redelivered. Surfaced via acquisitionLockTimeoutMs.
	private lastAcquisitionLockTimeoutMs = 0
	// Set when a ShareFetch task hit a transient error (dead broker / stale leader) so the fetch
	// loop refreshes metadata and re-resolves leaders before the next round.
	private fetchMetadataStale = false
	private topicIdByName: Map<string, string> = new Map()
	private topicNameById: Map<string, string> = new Map()
	private subscribedTopics: string[] = []

	constructor(cluster: Cluster, config: ShareConsumerConfig) {
		super()
		this.cluster = cluster
		this.logger = cluster.getLogger()?.child({ component: 'share-consumer', groupId: config.groupId }) ?? noopLogger
		this.config = {
			groupId: config.groupId,
			rackId: config.rackId,
			maxWaitMs: config.maxWaitMs ?? DEFAULT_SHARE_CONSUMER_CONFIG.maxWaitMs,
			minBytes: config.minBytes ?? DEFAULT_SHARE_CONSUMER_CONFIG.minBytes,
			maxBytes: config.maxBytes ?? DEFAULT_SHARE_CONSUMER_CONFIG.maxBytes,
			maxRecords: config.maxRecords ?? DEFAULT_SHARE_CONSUMER_CONFIG.maxRecords,
			batchSize: config.batchSize ?? DEFAULT_SHARE_CONSUMER_CONFIG.batchSize,
		}
		const acquireMode = config.acquireMode ?? DEFAULT_SHARE_CONSUMER_CONFIG.acquireMode
		this.acquireModeWire =
			acquireMode === 'record_limit' ? SHARE_ACQUIRE_MODE_RECORD_LIMIT : SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED
		this.validateConfig()
	}

	private validateConfig(): void {
		const c = this.config
		if (!c.groupId) {
			throw new Error('ShareConsumer requires a non-empty groupId')
		}
		if (!Number.isInteger(c.maxRecords) || c.maxRecords <= 0) {
			throw new Error(`ShareConsumer maxRecords must be a positive integer (got ${c.maxRecords})`)
		}
		if (!Number.isInteger(c.batchSize) || c.batchSize <= 0) {
			throw new Error(`ShareConsumer batchSize must be a positive integer (got ${c.batchSize})`)
		}
		if (c.minBytes < 0) {
			throw new Error(`ShareConsumer minBytes must be >= 0 (got ${c.minBytes})`)
		}
		if (c.maxBytes < c.minBytes) {
			throw new Error(`ShareConsumer maxBytes (${c.maxBytes}) must be >= minBytes (${c.minBytes})`)
		}
		if (c.maxWaitMs < 0) {
			throw new Error(`ShareConsumer maxWaitMs must be >= 0 (got ${c.maxWaitMs})`)
		}
	}

	get isRunning(): boolean {
		return this.state === 'running'
	}

	/** Latest broker-reported acquisition-lock timeout in ms (how long before an unacked record is
	 * redelivered), or 0 before the first fetch. Use it to decide when to call message.renew(). */
	get acquisitionLockTimeoutMs(): number {
		return this.lastAcquisitionLockTimeoutMs
	}

	/**
	 * Stop the consumer and wait for cleanup to complete.
	 *
	 * Returns a promise that resolves when all internal processes have finished,
	 * including flushing pending acknowledgements and leaving the group.
	 */
	stop(): Promise<void> {
		if (this.state === 'idle') {
			return Promise.resolve()
		}

		if (this.state === 'stopping') {
			return this.runCleanupPromise ?? Promise.resolve()
		}

		this.logger.info('stopping share consumer')
		this.state = 'stopping'
		this.abortController?.abort()

		if (this.runPromiseResolve) {
			this.runPromiseResolve()
			this.runPromiseResolve = null
		}

		return this.runCleanupPromise ?? Promise.resolve()
	}

	private startRun(externalSignal?: AbortSignal): void {
		if (this.state !== 'idle') {
			throw new Error('ShareConsumer is already running. Call stop() first.')
		}

		this.state = 'running'
		this.abortController = new AbortController()
		this.coordinator = null
		this.memberId = randomKafkaUuid()
		this.memberEpoch = 0
		this.shareSessionEpochByBrokerId.clear()
		this.shareSessionPartitionsByBrokerId.clear()
		this.shareSessionQueueByBrokerId.clear()
		this.heartbeatIntervalMs = 1000
		this.assignment = []
		this.didInitialPrefetch = false
		this.topicIdByName.clear()
		this.topicNameById.clear()
		this.subscribedTopics = []

		// Create cleanup promise that resolves when finalizeRun completes
		this.runCleanupPromise = new Promise<void>(resolve => {
			this.runCleanupResolve = resolve
		})

		if (externalSignal) {
			externalSignal.addEventListener('abort', () => {
				this.stop()
			})
		}
	}

	private async leaveGroup(): Promise<void> {
		// ShareGroupHeartbeatRequest: MemberEpoch = -1 leaves the group.
		// Best-effort only; ignore failures during shutdown.
		if (!this.coordinator || !this.memberId) {
			return
		}

		try {
			await this.coordinator.shareGroupHeartbeat({
				groupId: this.config.groupId,
				memberId: this.memberId,
				memberEpoch: -1,
				rackId: null,
				subscribedTopicNames: null,
			})
		} catch (error) {
			// Ignore errors during leave - we're stopping anyway.
			this.logger.debug('share group leave heartbeat failed', { error: (error as Error).message })
		}
	}

	private async finalizeRun(): Promise<void> {
		await this.leaveGroup()
		this.coordinator = null
		this.abortController = null
		this.runPromiseResolve = null
		this.state = 'idle'
		this.emit('stopped')
		this.logger.info('share consumer stopped')

		// Resolve cleanup promise so stop() callers know cleanup is complete
		if (this.runCleanupResolve) {
			this.runCleanupResolve()
			this.runCleanupResolve = null
		}
		this.runCleanupPromise = null
	}

	private emitError(error: unknown): void {
		const err = error instanceof Error ? error : new Error(String(error))
		if (this.listenerCount('error') > 0) {
			this.emit('error', err)
		} else {
			this.logger.error('share consumer error', { error: err.message })
		}
	}

	private getShareSessionEpoch(brokerId: number): number {
		return this.shareSessionEpochByBrokerId.get(brokerId) ?? SHARE_SESSION_INITIAL_EPOCH
	}

	private resetShareSessionEpoch(brokerId: number): void {
		this.shareSessionEpochByBrokerId.set(brokerId, SHARE_SESSION_INITIAL_EPOCH)
		// Drop the tracked session partition set: the next fetch (epoch 0) is a full fetch that
		// re-declares the whole set, so nothing needs to be (or can be) forgotten against the old one.
		this.shareSessionPartitionsByBrokerId.delete(brokerId)
	}

	private enqueueShareSessionOp<T>(broker: Broker, op: (epoch: number) => Promise<T>): Promise<T> {
		const brokerId = broker.nodeId

		const previous = this.shareSessionQueueByBrokerId.get(brokerId) ?? Promise.resolve()
		const start = previous.catch(() => {})

		const run = start.then(async () => {
			const epoch = this.getShareSessionEpoch(brokerId)
			try {
				return await op(epoch)
			} catch (error) {
				this.resetShareSessionEpoch(brokerId)
				throw error
			}
		})

		this.shareSessionQueueByBrokerId.set(
			brokerId,
			run.then(
				() => {},
				() => {}
			)
		)

		return run
	}

	private async shareFetch(
		broker: Broker,
		topicPartitions: Array<{ topicId: string; partitions: number[] }>,
		overrides?: Partial<Pick<ShareFetchRequest, 'maxWaitMs' | 'minBytes'>>
	): Promise<ShareFetchResponse> {
		return this.enqueueShareSessionOp(broker, async epoch => {
			// Current partition set for this broker, keyed by topicId.
			const currentByTopic = new Map<string, Set<number>>()
			for (const tp of topicPartitions) {
				currentByTopic.set(tp.topicId, new Set(tp.partitions))
			}

			// On an incremental fetch (epoch > 0), any partition that was in the broker's session
			// but is no longer in the current set must be explicitly forgotten — omitting it does
			// not remove it from the session. A full fetch (epoch 0) re-declares the whole set.
			const forgottenTopicsData =
				epoch === SHARE_SESSION_INITIAL_EPOCH ? [] : this.computeForgottenTopics(broker.nodeId, currentByTopic)

			const request: ShareFetchRequest = {
				groupId: this.config.groupId,
				memberId: this.memberId,
				shareSessionEpoch: epoch,
				maxWaitMs: overrides?.maxWaitMs ?? this.config.maxWaitMs,
				minBytes: overrides?.minBytes ?? this.config.minBytes,
				maxBytes: this.config.maxBytes,
				maxRecords: this.config.maxRecords,
				batchSize: this.config.batchSize,
				acquireMode: this.acquireModeWire,
				topics: topicPartitions.map(tp => ({
					topicId: tp.topicId,
					partitions: tp.partitions.map(p => ({
						partitionIndex: p,
						acknowledgementBatches: [],
					})),
				})),
				forgottenTopicsData,
			}

			let response: ShareFetchResponse
			try {
				response = await broker.shareFetch(request)
			} catch (e) {
				if (e instanceof UnsupportedVersionError) {
					throw new KafkaFeatureUnsupportedError('share-groups', 'broker does not support ShareFetch')
				}
				throw e
			}

			const sessionError = findShareSessionError(response.errorCode, response.topics)
			if (sessionError !== null) {
				this.resetShareSessionEpoch(broker.nodeId)
				throw new KafkaProtocolError(sessionError, response.errorMessage ?? 'ShareFetch failed')
			}

			this.shareSessionEpochByBrokerId.set(broker.nodeId, nextShareSessionEpoch(epoch))
			if (response.acquisitionLockTimeoutMs > 0) {
				this.lastAcquisitionLockTimeoutMs = response.acquisitionLockTimeoutMs
			}
			// Record the partition set the broker's session now holds, so the next fetch can
			// forget any partitions that drop out of the assignment.
			this.shareSessionPartitionsByBrokerId.set(broker.nodeId, currentByTopic)
			return response
		})
	}

	/**
	 * Partitions previously registered in the broker's share session that are absent from the
	 * current request, grouped by topic, so they can be sent in forgottenTopicsData.
	 */
	private computeForgottenTopics(
		brokerId: number,
		currentByTopic: Map<string, Set<number>>
	): ShareFetchRequest['forgottenTopicsData'] {
		const previousByTopic = this.shareSessionPartitionsByBrokerId.get(brokerId)
		if (!previousByTopic) {
			return []
		}

		const forgotten: NonNullable<ShareFetchRequest['forgottenTopicsData']> = []
		for (const [topicId, previousPartitions] of previousByTopic) {
			const currentPartitions = currentByTopic.get(topicId)
			const partitions = [...previousPartitions].filter(p => !currentPartitions?.has(p))
			if (partitions.length > 0) {
				forgotten.push({ topicId, partitions })
			}
		}
		return forgotten
	}

	/**
	 * Issue a ShareFetch to one leader, swallowing share-session errors (returning null so the
	 * caller retries with a fresh session). An empty `topicPartitions` produces a forget-only fetch
	 * (topics: [] plus forgottenTopicsData for the broker's stale partitions) and returns immediately.
	 */
	private buildShareFetchTask(
		leaderId: number,
		topicPartitions: Array<{ topicId: string; partitions: number[] }>
	): Promise<{ broker: Broker; response: ShareFetchResponse } | null> {
		return (async () => {
			const broker = await this.cluster.getBroker(leaderId)
			try {
				const overrides = topicPartitions.length === 0 ? { maxWaitMs: 0, minBytes: 0 } : undefined
				const response = await this.shareFetch(broker, topicPartitions, overrides)
				return { broker, response }
			} catch (error) {
				if (error instanceof KafkaProtocolError && isShareSessionError(error.errorCode)) {
					this.logger.debug('share session error, retrying with a new session', {
						leaderId,
						errorCode: error.errorCode,
						error: error.message,
					})
					return null
				}
				// A broker dying mid-fetch (or a stale leader) must not sink the whole round's
				// Promise.all and kill the consumer. Drop this leader for now, flag metadata stale so
				// the loop re-resolves leaders, and let the records be re-fetched next round.
				if (isTransientShareError(error)) {
					this.fetchMetadataStale = true
					this.logger.debug('share fetch to leader failed, retrying next round', {
						leaderId,
						error: (error as Error).message,
					})
					return null
				}
				throw error
			}
		})()
	}

	private async prefetchAssignedPartitions(partitions: TopicPartition[]): Promise<void> {
		const signal = this.abortController!.signal
		if (signal.aborted || this.state !== 'running' || partitions.length === 0) {
			return
		}

		const partitionsByLeader = new Map<number, Map<string, number[]>>()

		for (const tp of partitions) {
			const topicId = this.topicIdByName.get(tp.topic)
			if (!topicId) continue

			const leader = await this.cluster.getLeaderForPartition(tp.topic, tp.partition)
			const byTopic = partitionsByLeader.get(leader.nodeId) ?? new Map<string, number[]>()
			const ids = byTopic.get(topicId) ?? []
			ids.push(tp.partition)
			byTopic.set(topicId, ids)
			partitionsByLeader.set(leader.nodeId, byTopic)
		}

		await Promise.all(
			[...partitionsByLeader.entries()].map(async ([leaderId, byTopicId]) => {
				const broker = await this.cluster.getBroker(leaderId)
				const topicPartitions = [...byTopicId.entries()].map(([topicId, ids]) => ({ topicId, partitions: ids }))
				await this.shareFetch(broker, topicPartitions, { maxWaitMs: 0, minBytes: 0 })
			})
		)
	}

	private async shareAcknowledge(
		broker: Broker,
		requestWithoutEpoch: ShareAcknowledgeRequestWithoutEpoch
	): Promise<ShareAcknowledgeResponse> {
		return this.enqueueShareSessionOp(broker, async epoch => {
			const request: ShareAcknowledgeRequest = {
				...requestWithoutEpoch,
				shareSessionEpoch: epoch,
			}

			let response: ShareAcknowledgeResponse
			try {
				response = await broker.shareAcknowledge(request)
			} catch (e) {
				if (e instanceof UnsupportedVersionError) {
					throw new KafkaFeatureUnsupportedError('share-groups', 'broker does not support ShareAcknowledge')
				}
				throw e
			}

			const sessionError = findShareSessionError(response.errorCode, response.topics)
			if (sessionError !== null) {
				this.resetShareSessionEpoch(broker.nodeId)
				throw new KafkaProtocolError(sessionError, response.errorMessage ?? 'ShareAcknowledge failed')
			}

			this.shareSessionEpochByBrokerId.set(broker.nodeId, nextShareSessionEpoch(epoch))
			return response
		})
	}

	private async getCoordinatorWithRetry(type: 'GROUP' | 'SHARE', key: string): Promise<Broker> {
		const signal = this.abortController?.signal
		try {
			return await retry(() => this.cluster.getCoordinator(type, key), {
				maxAttempts: 10,
				initialDelayMs: 100,
				maxDelayMs: 1000,
				multiplier: 2,
				jitter: 0,
				signal,
				resolveOnAbort: true,
				shouldRetry: error => {
					if (signal?.aborted) return false
					return error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError
				},
				onRetry: ({ attempt, delayMs, error }) => {
					this.logger.debug('share coordinator lookup failed, retrying', {
						type,
						key,
						attempt,
						delayMs,
						error: error instanceof Error ? error.message : String(error),
					})
				},
			})
		} catch (error) {
			if (signal?.aborted) throw error
			if (error instanceof CoordinatorNotAvailableError || error instanceof NotCoordinatorError) {
				throw new CoordinatorNotAvailableError(type, key)
			}
			throw error
		}
	}

	private normalizeSubscription(input: ShareSubscriptionInput): TopicSubscription<unknown, unknown>[] {
		const items = (Array.isArray(input) ? input : [input]) as Array<string | SubscriptionLike<unknown, unknown>>
		return items.map(item =>
			typeof item === 'string' ? toShareTopicSubscription(item) : toShareTopicSubscription(item)
		)
	}

	private getSubscriptionsAndTopics(subscription: ShareSubscriptionInput): {
		subscriptions: TopicSubscription<unknown, unknown>[]
		topics: string[]
	} {
		const subscriptions = this.normalizeSubscription(subscription)
		return { subscriptions, topics: subscriptions.map(s => s.topic) }
	}

	private async refreshTopicIdMaps(topics: string[]): Promise<void> {
		const md = await this.cluster.refreshMetadata(topics.length > 0 ? topics : undefined)
		this.topicIdByName.clear()
		this.topicNameById.clear()
		for (const [name, meta] of md.topics) {
			this.topicIdByName.set(name, meta.topicId)
			this.topicNameById.set(meta.topicId, name)
		}
	}

	private async ensureCoordinator(): Promise<void> {
		// Share group membership is coordinated by the group coordinator (KIP-932).
		this.coordinator = await this.getCoordinatorWithRetry('GROUP', this.config.groupId)
	}

	private async joinGroup(subscribedTopicNames: string[]): Promise<void> {
		const signal = this.abortController!.signal

		while (!signal.aborted && this.state === 'running') {
			try {
				// Share group membership is managed via ShareGroupHeartbeat, so always include
				// the subscription to ensure the coordinator can compute assignments.
				await this.shareHeartbeat(subscribedTopicNames)

				// ShareGroupHeartbeat may return no assignment during group stabilization; keep heartbeating
				// until we get a non-empty assignment so fetch can start immediately.
				if (this.assignment.length > 0 || subscribedTopicNames.length === 0) {
					return
				}
			} catch (error) {
				if (signal.aborted || this.state !== 'running') return
				if (error instanceof NotCoordinatorError || error instanceof CoordinatorNotAvailableError) {
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				if (error instanceof KafkaProtocolError && error.errorCode === ErrorCode.CoordinatorLoadInProgress) {
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				// Re-resolve the coordinator and retry on a transient failure (e.g. the coordinator
				// broker died) instead of failing the join.
				if (isTransientShareError(error)) {
					this.cluster.invalidateCoordinator('GROUP', this.config.groupId)
					this.coordinator = null
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				throw error
			}

			// Heartbeat interval is advisory; cap it to keep startup responsive.
			const intervalMs = Math.min(this.heartbeatIntervalMs, 1000)
			await sleep(Math.max(intervalMs, 200), { signal, resolveOnAbort: true })
		}
	}

	private async shareHeartbeat(subscribedTopicNames: string[] | null): Promise<void> {
		if (!this.coordinator) {
			await this.ensureCoordinator()
		}
		const coordinator = this.coordinator!

		const request: ShareGroupHeartbeatRequest = {
			groupId: this.config.groupId,
			memberId: this.memberId,
			memberEpoch: this.memberEpoch,
			rackId: this.config.rackId ?? null,
			subscribedTopicNames,
		}

		let response: Awaited<ReturnType<Broker['shareGroupHeartbeat']>>
		try {
			response = await coordinator.shareGroupHeartbeat(request)
		} catch (e) {
			if (e instanceof UnsupportedVersionError) {
				throw new KafkaFeatureUnsupportedError('share-groups', 'broker does not support ShareGroupHeartbeat')
			}
			throw e
		}

		if (response.errorCode !== ErrorCode.None) {
			if (
				response.errorCode === ErrorCode.NotCoordinator ||
				response.errorCode === ErrorCode.CoordinatorNotAvailable
			) {
				this.cluster.invalidateCoordinator('GROUP', this.config.groupId)
				this.coordinator = null
				throw new NotCoordinatorError('GROUP', this.config.groupId)
			}

			// Fencing is recoverable, not fatal: the coordinator advanced past this member's epoch (or
			// forgot it). Abandon the assignment and rejoin with epoch 0 on the next heartbeat.
			if (
				response.errorCode === ErrorCode.FencedMemberEpoch ||
				response.errorCode === ErrorCode.UnknownMemberId
			) {
				this.logger.info('share group member fenced; abandoning assignment and rejoining', {
					errorCode: response.errorCode,
					memberId: this.memberId,
				})
				this.transitionToFenced()
				return
			}

			const message = response.errorMessage ?? `ShareGroupHeartbeat failed for ${this.config.groupId}`
			// Share Groups are feature-flagged. When the broker supports the Share APIs but the
			// share.version feature is not enabled, ShareGroupHeartbeat returns InvalidRequest.
			if (response.errorCode === ErrorCode.InvalidRequest) {
				throw new KafkaFeatureDisabledError(
					'share-groups',
					`${message}. Ensure the broker is configured with share groups enabled (e.g. share.version=1).`
				)
			}

			throw new KafkaProtocolError(response.errorCode, message)
		}

		if (response.memberId && response.memberId !== this.memberId) {
			this.memberId = response.memberId
			this.shareSessionEpochByBrokerId.clear()
			this.shareSessionPartitionsByBrokerId.clear()
			this.shareSessionQueueByBrokerId.clear()
		}

		this.memberEpoch = response.memberEpoch
		this.heartbeatIntervalMs = response.heartbeatIntervalMs

		if (response.assignment) {
			await this.applyAssignment(response.assignment)
		}

		// Heartbeat interval is advisory; the loop uses it for pacing.
		// No further action needed here.
	}

	// Reset to a freshly-joining member after a fence (FencedMemberEpoch / UnknownMemberId): rejoin
	// epoch is 0, the memberId is kept (it is this member's lifetime id), and the per-broker share
	// sessions are wiped so the next ShareFetch starts fresh full sessions. The broker already
	// dropped this member's acquisition locks, so its in-flight records redeliver to the group after
	// the lock timeout — we do not ack/release them here (those acks would fail anyway).
	private transitionToFenced(): void {
		this.memberEpoch = 0
		this.shareSessionEpochByBrokerId.clear()
		this.shareSessionPartitionsByBrokerId.clear()
		this.shareSessionQueueByBrokerId.clear()
		const revoked = this.assignment
		this.assignment = []
		this.didInitialPrefetch = false
		if (revoked.length > 0) {
			this.emit('partitionsRevoked', revoked)
		}
	}

	private async applyAssignment(raw: Array<{ topicId: string; partitions: number[] }>): Promise<void> {
		if (this.topicNameById.size === 0) {
			await this.refreshTopicIdMaps([])
		}

		const next: TopicPartition[] = []
		for (const t of raw) {
			let topicName = this.topicNameById.get(t.topicId)
			if (!topicName) {
				await this.refreshTopicIdMaps([])
				topicName = this.topicNameById.get(t.topicId)
			}
			if (!topicName) {
				this.logger.warn('received assignment for unknown topicId', { topicId: t.topicId })
				continue
			}
			for (const p of t.partitions) {
				next.push({ topic: topicName, partition: p })
			}
		}

		const prevKeys = new Set(this.assignment.map(toTopicPartitionKey))
		const nextKeys = new Set(next.map(toTopicPartitionKey))

		const revoked: TopicPartition[] = this.assignment.filter(tp => !nextKeys.has(toTopicPartitionKey(tp)))
		const assigned: TopicPartition[] = next.filter(tp => !prevKeys.has(toTopicPartitionKey(tp)))

		this.assignment = next

		// Share groups effectively start from "now" when a member first begins fetching. On the first
		// assignment, proactively issue a non-blocking ShareFetch to establish the starting point before
		// emitting partitionsAssigned (so callers can safely produce after this event). Only once per
		// run: prefetchAssignedPartitions discards what it acquires, so repeating it per-rebalance would
		// needlessly redeliver records.
		if (!this.didInitialPrefetch && next.length > 0) {
			await this.prefetchAssignedPartitions(next)
			this.didInitialPrefetch = true
		}

		if (revoked.length > 0) {
			this.emit('partitionsRevoked', revoked)
		}
		if (assigned.length > 0) {
			this.emit('partitionsAssigned', assigned)
		}
	}

	private async decodeRecords(data: Buffer): Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)

		type Batch = { attributes: number; records: DecodedRecord[] }
		const batches: Batch[] = []

		let needsAsync = false
		while (decoder.remaining() > 0) {
			try {
				const batch = decodeRecordBatchFromSync(decoder, { assumeSequentialOffsets: true })
				batches.push({ attributes: batch.attributes, records: batch.records })
			} catch (e) {
				if ((e as Error).message.includes('cannot decode compressed')) {
					needsAsync = true
					break
				}
				// A failure with no batch decoded is genuine corruption (the broker returns
				// at least one complete batch); surface it instead of silently abandoning
				// acquired records (which would otherwise stay locked until the lock expires).
				// A failure after >=1 batch is the expected maxBytes-truncated trailing batch.
				if (batches.length === 0) {
					throw new Error(`Failed to decode share-fetch record batch (corrupt data): ${(e as Error).message}`)
				}
				break
			}
		}

		if (needsAsync) {
			return this.decodeRecordsAsync(data)
		}

		const out: DecodedRecord[] = []
		for (const b of batches) {
			if (isControlBatch(b.attributes)) continue
			out.push(...b.records)
		}
		return out
	}

	private async decodeRecordsAsync(data: Buffer): Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)
		const out: DecodedRecord[] = []
		let decoded = 0
		while (decoder.remaining() > 0) {
			try {
				const batch = await decodeRecordBatchFrom(decoder, { assumeSequentialOffsets: true })
				decoded++
				if (isControlBatch(batch.attributes)) continue
				out.push(...batch.records)
			} catch (e) {
				// See decodeRecords: surface genuine corruption (no batch decoded) instead of
				// silently abandoning acquired records; a later failure is the truncated tail.
				if (decoded === 0) {
					throw new Error(`Failed to decode share-fetch record batch (corrupt data): ${(e as Error).message}`)
				}
				break
			}
		}
		return out
	}

	private async collectShareFetchWorkItems(
		broker: Broker,
		response: ShareFetchResponse,
		subscriptionByTopic: Map<string, TopicSubscription<unknown, unknown>>,
		ackManager: AckManager
	): Promise<ShareFetchWorkItem[]> {
		const signal = this.abortController!.signal

		if (response.errorCode !== ErrorCode.None) {
			throw new KafkaProtocolError(response.errorCode, response.errorMessage ?? 'ShareFetch failed')
		}

		const items: ShareFetchWorkItem[] = []

		for (const topicResponse of response.topics) {
			const topicName = this.topicNameById.get(topicResponse.topicId)
			if (!topicName) {
				continue
			}
			const sub = subscriptionByTopic.get(topicName)
			if (!sub) continue

			const decoder = normalizeDecoder(sub.decoder)
			const keyDecoder = sub.keyDecoder ? normalizeDecoder(sub.keyDecoder) : (b: Buffer) => b

			for (const partitionResponse of topicResponse.partitions) {
				if (signal.aborted || this.state !== 'running') break

				if (partitionResponse.errorCode !== ErrorCode.None) {
					const cls = classifyFetchPartitionError(partitionResponse.errorCode)
					if (cls === 'refresh') {
						this.logger.debug('share fetch partition error, refreshing metadata and skipping', {
							topic: topicName,
							partition: partitionResponse.partitionIndex,
							errorCode: getErrorName(partitionResponse.errorCode),
						})
						this.fetchMetadataStale = true
						continue
					}
					if (cls === 'skip') {
						this.logger.warn('share fetch partition error, skipping this round', {
							topic: topicName,
							partition: partitionResponse.partitionIndex,
							errorCode: getErrorName(partitionResponse.errorCode),
						})
						continue
					}
					throw new KafkaProtocolError(
						partitionResponse.errorCode,
						partitionResponse.errorMessage ??
							`ShareFetch failed for ${topicName}-${partitionResponse.partitionIndex}`
					)
				}

				if (partitionResponse.acknowledgeErrorCode !== ErrorCode.None) {
					// Best-effort metadata refresh on leadership/unknown topic errors
					if (
						partitionResponse.acknowledgeErrorCode === ErrorCode.NotLeaderOrFollower ||
						partitionResponse.acknowledgeErrorCode === ErrorCode.UnknownTopicOrPartition ||
						partitionResponse.acknowledgeErrorCode === ErrorCode.UnknownTopicId
					) {
						await this.cluster.refreshMetadata([topicName])
						await this.refreshTopicIdMaps([])
						continue
					}

					if (isShareSessionError(partitionResponse.acknowledgeErrorCode)) {
						this.resetShareSessionEpoch(broker.nodeId)
					}

					throw new KafkaProtocolError(
						partitionResponse.acknowledgeErrorCode,
						partitionResponse.acknowledgeErrorMessage ??
							`ShareFetch acknowledgement failed for ${topicName}-${partitionResponse.partitionIndex}`
					)
				}

				const acquiredRecords = partitionResponse.acquiredRecords
				const recordsData = partitionResponse.recordsData

				// Nothing acquired and no data: nothing to deliver or gap-ack.
				if (acquiredRecords.length === 0 && (!recordsData || recordsData.length === 0)) {
					continue
				}

				const records = recordsData && recordsData.length > 0 ? await this.decodeRecords(recordsData) : []
				const recordOffsets = new Set<bigint>()
				for (const r of records) recordOffsets.add(r.offset)

				// Gap-ack every acquired offset that has no delivered record (a compaction hole or a
				// control-batch offset, which decodeRecords drops). Without this the share-partition
				// start offset stalls and those offsets redeliver forever. Fire-and-forget: the fetch
				// round's flushAll sends them. Runs even when records is empty (control-only response).
				// A gap-ack can legitimately fail (lock expired, leader moved); the offset just
				// redelivers, so swallow the rejection rather than letting it escape as unhandled.
				for (const range of acquiredRecords) {
					for (let off = range.firstOffset; off <= range.lastOffset; off++) {
						if (!recordOffsets.has(off)) {
							void ackManager
								.enqueue(
									topicName,
									topicResponse.topicId,
									partitionResponse.partitionIndex,
									off,
									ACK_GAP
								)
								.catch(() => {})
						}
					}
				}

				for (const record of records) {
					if (signal.aborted || this.state !== 'running') break

					const range = acquiredRecords.find(
						r => record.offset >= r.firstOffset && record.offset <= r.lastOffset
					)
					// Deliver only records the broker actually acquired (locked) for this member. A
					// ShareFetch can return batch data spanning already-acked records (e.g. when the
					// share-partition start offset is held back by a gap); records outside every acquired
					// range are not held by this member and must be skipped, not reprocessed.
					if (!range) continue

					items.push({
						topicName,
						topicId: topicResponse.topicId,
						partitionIndex: partitionResponse.partitionIndex,
						record,
						deliveryCount: range.deliveryCount,
						decoder,
						keyDecoder,
					})
				}
			}
		}

		return items
	}

	private async processShareFetchWorkItem(
		item: ShareFetchWorkItem,
		handler: ShareMessageHandler<unknown, unknown>,
		ackManager: AckManager,
		autoAckOnSuccess: boolean
	): Promise<number> {
		const signal = this.abortController!.signal
		if (signal.aborted || this.state !== 'running') {
			return 0
		}

		let handled = 0 // 0=none, 1=acked, 2=released, 3=rejected
		const { topicName, topicId, partitionIndex, record } = item

		const message: ShareMessage<unknown, unknown> = {
			topic: topicName,
			partition: partitionIndex,
			offset: record.offset,
			timestamp: record.timestamp,
			key: record.key === null ? null : item.keyDecoder(record.key),
			value: record.value === null ? null : item.decoder(record.value ?? EMPTY_BUFFER),
			headers: convertHeaders(record.headers),
			ack: async () => {
				if (handled !== 0) {
					throw new ShareMessageAlreadyHandledError(topicName, partitionIndex, record.offset)
				}
				handled = 1
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_ACCEPT)
			},
			release: async () => {
				if (handled !== 0) {
					throw new ShareMessageAlreadyHandledError(topicName, partitionIndex, record.offset)
				}
				handled = 2
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_RELEASE)
			},
			reject: async () => {
				if (handled !== 0) {
					throw new ShareMessageAlreadyHandledError(topicName, partitionIndex, record.offset)
				}
				handled = 3
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_REJECT)
			},
			renew: async () => {
				if (handled !== 0) {
					throw new ShareMessageAlreadyHandledError(topicName, partitionIndex, record.offset)
				}
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_RENEW)
			},
		}

		if (item.deliveryCount !== undefined) {
			message.deliveryCount = item.deliveryCount
		}

		await handler(message, {
			signal,
			topic: topicName,
			partition: partitionIndex,
			offset: record.offset,
		})

		if (handled === 0 && autoAckOnSuccess) {
			handled = 1
			try {
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_ACCEPT)
			} catch (error) {
				// A failed auto-ack (e.g. the acquisition lock expired) must not kill the consumer: the
				// record is left to redeliver after the lock times out. Surface it in the log rather than
				// throwing, which would tear down the fetch loop.
				this.logger.warn('auto-acknowledge failed; record will be redelivered', {
					topic: topicName,
					partition: partitionIndex,
					offset: record.offset.toString(),
					error: (error as Error).message,
				})
			}
		}

		return 1
	}

	private async heartbeatLoop(subscribedTopicNames: string[]): Promise<void> {
		const signal = this.abortController!.signal

		while (!signal.aborted && this.state === 'running') {
			try {
				await this.shareHeartbeat(subscribedTopicNames)
			} catch (error) {
				if (signal.aborted || this.state !== 'running') return
				// Coordinator errors are retried by letting the next iteration re-resolve.
				if (error instanceof NotCoordinatorError || error instanceof CoordinatorNotAvailableError) {
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				if (error instanceof KafkaProtocolError && error.errorCode === ErrorCode.CoordinatorLoadInProgress) {
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				// The coordinator broker may have died mid-heartbeat: drop it so the next iteration
				// re-resolves a live coordinator, rather than tearing down the consumer.
				if (isTransientShareError(error)) {
					this.cluster.invalidateCoordinator('GROUP', this.config.groupId)
					this.coordinator = null
					await sleep(250, { signal, resolveOnAbort: true })
					continue
				}
				throw error
			}

			// Use the broker-provided interval as guidance, but cap it to keep
			// membership/assignment changes responsive (especially during startup).
			const intervalMs = Math.min(this.heartbeatIntervalMs, 1000)
			await sleep(Math.max(intervalMs, 200), { signal, resolveOnAbort: true })
		}
	}

	private async fetchLoop(
		subscriptions: TopicSubscription<unknown, unknown>[],
		handler: ShareMessageHandler<unknown, unknown>,
		options: ShareRunEachOptions,
		autoAckOnSuccess: boolean
	): Promise<void> {
		const signal = this.abortController!.signal

		const opts = { ...DEFAULT_SHARE_RUN_EACH_OPTIONS, ...options }
		const ackManager = new AckManager(
			this.config.groupId,
			() => this.memberId,
			(broker, request) => this.shareAcknowledge(broker, request),
			(topic, partition) => this.cluster.getLeaderForPartition(topic, partition),
			topics => this.cluster.refreshMetadata(topics).then(() => {}),
			brokerId => this.resetShareSessionEpoch(brokerId),
			this.logger,
			opts.ackBatchSize
		)

		const subscriptionByTopic = new Map<string, TopicSubscription<unknown, unknown>>()
		for (const s of subscriptions) {
			subscriptionByTopic.set(s.topic, s)
		}

		try {
			while (!signal.aborted && this.state === 'running') {
				try {
					const partitionsByLeader = new Map<number, Map<string, number[]>>()

					for (const tp of this.assignment) {
						const topicId = this.topicIdByName.get(tp.topic)
						if (!topicId) continue

						const leader = await this.cluster.getLeaderForPartition(tp.topic, tp.partition)
						const byTopic = partitionsByLeader.get(leader.nodeId) ?? new Map<string, number[]>()
						const partitions = byTopic.get(topicId) ?? []
						partitions.push(tp.partition)
						byTopic.set(topicId, partitions)
						partitionsByLeader.set(leader.nodeId, byTopic)
					}

					// Brokers whose share session still holds partitions that are no longer assigned to
					// them (e.g. the last assigned partition on a broker was revoked or moved away). They
					// are absent from partitionsByLeader, so without an explicit forget-only ShareFetch the
					// broker would retain the stale partitions (and their acquisition locks) until the
					// session times out. Send an empty fetch so forgottenTopicsData drops them now.
					const forgetOnlyBrokerIds: number[] = []
					for (const [brokerId, byTopic] of this.shareSessionPartitionsByBrokerId) {
						if (!partitionsByLeader.has(brokerId) && byTopic.size > 0) {
							forgetOnlyBrokerIds.push(brokerId)
						}
					}

					if (partitionsByLeader.size === 0 && forgetOnlyBrokerIds.length === 0) {
						await sleep(opts.idleBackoffMs, { signal, resolveOnAbort: true })
						continue
					}

					const fetchTasks: Array<Promise<{ broker: Broker; response: ShareFetchResponse } | null>> = []

					for (const [leaderId, byTopicId] of partitionsByLeader) {
						const topicPartitions = [...byTopicId.entries()].map(([topicId, partitions]) => ({
							topicId,
							partitions,
						}))
						fetchTasks.push(this.buildShareFetchTask(leaderId, topicPartitions))
					}

					// Empty topicPartitions => topics:[] with forgottenTopicsData listing the stale set.
					for (const brokerId of forgetOnlyBrokerIds) {
						fetchTasks.push(this.buildShareFetchTask(brokerId, []))
					}

					const fetchResults = await Promise.all(fetchTasks)

					// A leader died/moved or a topic id changed this round: refresh metadata AND the topic-id
					// maps so the next round re-resolves leaders and topic ids instead of retrying stale ones.
					if (this.fetchMetadataStale) {
						this.fetchMetadataStale = false
						await this.refreshTopicIdMaps(this.subscribedTopics).catch(() => {})
					}

					const workItems: ShareFetchWorkItem[] = []
					for (const result of fetchResults) {
						if (!result || signal.aborted || this.state !== 'running') {
							continue
						}
						workItems.push(
							...(await this.collectShareFetchWorkItems(
								result.broker,
								result.response,
								subscriptionByTopic,
								ackManager
							))
						)
					}

					const delivered = await pmap(
						workItems,
						item => this.processShareFetchWorkItem(item, handler, ackManager, autoAckOnSuccess),
						opts.concurrency,
						signal
					)
					const totalDelivered = delivered.reduce((a, b) => a + b, 0)

					await ackManager.flushAll()

					if (totalDelivered === 0) {
						await sleep(opts.idleBackoffMs, { signal, resolveOnAbort: true })
					}
				} catch (error) {
					if (signal.aborted || this.state !== 'running') break
					// A per-partition error the classifier deemed fatal (e.g. CorruptMessage, auth) is
					// authoritative even if its code is globally "retriable" — surface it rather than
					// retrying the same partition forever. Share-session errors are excluded: they are
					// genuinely retried with a fresh session.
					if (
						error instanceof KafkaProtocolError &&
						!isShareSessionError(error.errorCode) &&
						classifyFetchPartitionError(error.errorCode) === 'fatal'
					) {
						throw error
					}
					// Transient infrastructure failures (dead broker, stale leader, moved coordinator)
					// must not kill the consumer: refresh metadata, back off, and retry. Acquired-but-
					// unprocessed records are redelivered after their acquisition lock expires.
					if (!isTransientShareError(error)) throw error
					this.logger.debug('share fetch loop transient error, refreshing metadata and retrying', {
						error: (error as Error).message,
					})
					await this.cluster.refreshMetadata(this.subscribedTopics).catch(() => {})
					await sleep(opts.idleBackoffMs, { signal, resolveOnAbort: true })
				}
			}
		} finally {
			// Best-effort final ack flush on shutdown and prevent scheduled flushes
			await ackManager.shutdown().catch(err => {
				this.logger.error('failed to flush share acknowledgements during shutdown', {
					error: (err as Error).message,
				})
			})
		}
	}

	async runEach<S extends ShareSubscriptionInput>(
		subscription: S,
		handler: ShareMessageHandler<ShareMsgOf<S>, ShareKeyOf<S>>,
		options?: ShareRunEachOptions
	): Promise<void> {
		this.startRun(options?.signal)

		try {
			const { subscriptions, topics: rawTopics } = this.getSubscriptionsAndTopics(subscription)
			const topics = [...new Set(rawTopics)].sort()
			this.subscribedTopics = topics
			await this.refreshTopicIdMaps(topics)

			this.logger.info('starting share consumer', { topics })

			await this.ensureCoordinator()
			await this.joinGroup(topics)

			const heartbeatPromise = this.heartbeatLoop(topics)
			const fetchPromise = this.fetchLoop(
				subscriptions,
				handler as ShareMessageHandler<unknown, unknown>,
				options ?? {},
				true
			)
			const loops = Promise.all([heartbeatPromise, fetchPromise]).then(() => {})

			this.emit('running')

			let loopError: Error | null = null
			loops.catch(err => {
				loopError = err instanceof Error ? err : new Error(String(err))
				this.stop()
			})

			const stopPromise = new Promise<void>(resolve => {
				this.runPromiseResolve = resolve
			})

			await Promise.race([loops, stopPromise])
			await Promise.allSettled([heartbeatPromise, fetchPromise])

			this.runPromiseResolve = null

			if (loopError) {
				const err: Error = loopError ?? new Error('ShareConsumer loop failed')
				throw err
			}
		} catch (error) {
			this.emitError(error)
			throw error
		} finally {
			await this.finalizeRun()
		}
	}

	async *stream<S extends ShareSubscriptionInput>(
		subscription: S
	): AsyncIterable<{ message: ShareMessage<ShareMsgOf<S>, ShareKeyOf<S>>; ctx: ConsumeContext }> {
		this.startRun()

		let heartbeatPromise: Promise<void> | null = null
		let fetchPromise: Promise<void> | null = null
		let loopError: Error | null = null
		let previousMessage: ShareMessage<unknown, unknown> | null = null
		// Resolves the back-pressure gate when the iterator drains; declared out here so the finally can
		// release a fetch loop parked on a full queue (otherwise shutdown would deadlock). A holder object
		// keeps the closure-assigned resolver visible to control-flow analysis at the call sites.
		const gate: { resume: (() => void) | null } = { resume: null }

		const implicitFinalize = (
			message: ShareMessage<unknown, unknown>,
			op: 'ack' | 'release',
			stopOnError: boolean
		): Promise<void> =>
			message[op]().catch(error => {
				if (error instanceof ShareMessageAlreadyHandledError) return
				const err = error instanceof Error ? error : new Error(String(error))
				this.emitError(err)
				if (stopOnError) this.stop()
			})

		try {
			const { subscriptions, topics: rawTopics } = this.getSubscriptionsAndTopics(subscription)
			const topics = [...new Set(rawTopics)].sort()
			this.subscribedTopics = topics
			await this.refreshTopicIdMaps(topics)
			await this.ensureCoordinator()
			await this.joinGroup(topics)

			const signal = this.abortController!.signal

			// Message queue for yielding
			const messageQueue: Array<{ message: ShareMessage<unknown, unknown>; ctx: ConsumeContext }> = []
			let resolveNext: (() => void) | null = null

			const enqueueHandler: ShareMessageHandler<unknown, unknown> = (message, ctx) => {
				messageQueue.push({ message, ctx })
				if (resolveNext) {
					resolveNext()
					resolveNext = null
				}
				// Back-pressure: park the fetch loop (concurrency 1) until the iterator drains below the
				// high-water mark, so we don't fetch/lock records unboundedly ahead of a slow consumer.
				if (messageQueue.length >= STREAM_QUEUE_HIGH_WATER_MARK) {
					return new Promise<void>(resolve => {
						gate.resume = () => resolve()
					})
				}
				return Promise.resolve()
			}

			heartbeatPromise = this.heartbeatLoop(topics)
			fetchPromise = this.fetchLoop(subscriptions, enqueueHandler, { concurrency: 1 }, false)
			const loops = Promise.all([heartbeatPromise, fetchPromise]).then(() => {})

			loops.catch(err => {
				loopError = err instanceof Error ? err : new Error(String(err))
				this.emitError(loopError)
				this.stop()
			})

			this.emit('running')

			// Yield loop
			while (this.state === 'running' && !signal.aborted) {
				if (messageQueue.length === 0) {
					await new Promise<void>(resolve => {
						resolveNext = resolve
						setTimeout(resolve, 100)
					})
				}

				while (messageQueue.length > 0) {
					const item = messageQueue.shift()!
					// Drained below the gate: unblock the fetch loop.
					if (gate.resume && messageQueue.length < STREAM_QUEUE_HIGH_WATER_MARK) {
						gate.resume()
						gate.resume = null
					}
					if (previousMessage) {
						implicitFinalize(previousMessage, 'ack', true)
					}
					previousMessage = item.message
					yield item as { message: ShareMessage<ShareMsgOf<S>, ShareKeyOf<S>>; ctx: ConsumeContext }
				}
			}
		} finally {
			// Iterator exit is ambiguous (clean break vs user throw); release rather than ack so the
			// message returns to the share group for redelivery instead of being silently consumed. The
			// release is enqueued (synchronously into the ack manager) before shutdown closes it.
			const releasePromise = previousMessage ? implicitFinalize(previousMessage, 'release', false) : null
			previousMessage = null

			// Unblock a fetch loop parked on the back-pressure gate so it can reach its finally and flush.
			if (gate.resume) {
				gate.resume()
				gate.resume = null
			}

			this.stop()
			const promises = [heartbeatPromise, fetchPromise].filter((p): p is Promise<void> => p !== null)
			await Promise.allSettled(promises)
			await this.finalizeRun()
			if (releasePromise) await releasePromise
		}

		// Surface a fatal background loop error to the iterator consumer (matching runEach) instead of
		// ending the stream cleanly as if it had drained normally.
		if (loopError) {
			const err: Error = loopError ?? new Error('ShareConsumer stream loop failed')
			throw err
		}
	}
}
