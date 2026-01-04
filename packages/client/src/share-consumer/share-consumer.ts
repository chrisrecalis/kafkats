/**
 * @experimental
 *
 * ShareConsumer implements Kafka Share Groups (KIP-932).
 *
 * This is a separate consumer from the classic Consumer group implementation and uses:
 * - ShareGroupHeartbeat for membership
 * - ShareFetch for record acquisition
 * - ShareAcknowledge for per-record acknowledgements
 */

import { EventEmitter } from 'node:events'
import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import {
	CoordinatorNotAvailableError,
	KafkaFeatureDisabledError,
	KafkaFeatureUnsupportedError,
	KafkaProtocolError,
	NotCoordinatorError,
	UnsupportedVersionError,
} from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
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
import { AckManager, ACK_ACCEPT, ACK_RELEASE, ACK_REJECT } from './ack-manager.js'
import {
	convertHeaders,
	isControlBatch,
	nextShareSessionEpoch,
	randomKafkaUuid,
	SHARE_SESSION_INITIAL_EPOCH,
	toShareTopicSubscription,
	toTopicPartitionKey,
} from './share-consumer-helpers.js'
import { pmap } from './pmap.js'

const EMPTY_BUFFER = Buffer.alloc(0)

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

/**
 * @experimental
 */
export class ShareConsumer extends EventEmitter<ShareConsumerEvents> {
	private readonly cluster: Cluster
	private readonly logger: Logger
	private readonly config: Required<
		Pick<ShareConsumerConfig, 'groupId' | 'maxWaitMs' | 'minBytes' | 'maxBytes' | 'maxRecords' | 'batchSize'>
	> &
		Pick<ShareConsumerConfig, 'rackId'>

	private state: ShareConsumerState = 'idle'
	private abortController: AbortController | null = null
	private runPromiseResolve: (() => void) | null = null
	private runCleanupPromise: Promise<void> | null = null
	private runCleanupResolve: (() => void) | null = null

	private coordinator: Broker | null = null
	private memberId: string = randomKafkaUuid()
	private memberEpoch: number = 0
	private shareSessionEpochByBrokerId: Map<number, number> = new Map()
	private shareSessionQueueByBrokerId: Map<number, Promise<void>> = new Map()
	private heartbeatIntervalMs: number = 1000
	private assignment: TopicPartition[] = []
	private didInitialPrefetch = false
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
	}

	get isRunning(): boolean {
		return this.state === 'running'
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
			const request: ShareFetchRequest = {
				groupId: this.config.groupId,
				memberId: this.memberId,
				shareSessionEpoch: epoch,
				maxWaitMs: overrides?.maxWaitMs ?? this.config.maxWaitMs,
				minBytes: overrides?.minBytes ?? this.config.minBytes,
				maxBytes: this.config.maxBytes,
				maxRecords: this.config.maxRecords,
				batchSize: this.config.batchSize,
				topics: topicPartitions.map(tp => ({
					topicId: tp.topicId,
					partitions: tp.partitions.map(p => ({
						partitionIndex: p,
						acknowledgementBatches: [],
					})),
				})),
				forgottenTopicsData: [],
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

			if (
				response.errorCode === ErrorCode.ShareSessionNotFound ||
				response.errorCode === ErrorCode.InvalidShareSessionEpoch
			) {
				this.resetShareSessionEpoch(broker.nodeId)
				throw new KafkaProtocolError(response.errorCode, response.errorMessage ?? 'ShareFetch failed')
			}

			this.shareSessionEpochByBrokerId.set(broker.nodeId, nextShareSessionEpoch(epoch))
			return response
		})
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

			if (
				response.errorCode === ErrorCode.ShareSessionNotFound ||
				response.errorCode === ErrorCode.InvalidShareSessionEpoch
			) {
				this.resetShareSessionEpoch(broker.nodeId)
				throw new KafkaProtocolError(response.errorCode, response.errorMessage ?? 'ShareAcknowledge failed')
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

		// Share groups effectively start from "now" when a member first begins fetching. On initial
		// assignment, proactively issue a non-blocking ShareFetch to establish the starting point
		// before emitting partitionsAssigned (so callers can safely produce after this event).
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
		while (decoder.remaining() > 0) {
			try {
				const batch = await decodeRecordBatchFrom(decoder, { assumeSequentialOffsets: true })
				if (isControlBatch(batch.attributes)) continue
				out.push(...batch.records)
			} catch {
				break
			}
		}
		return out
	}

	private async collectShareFetchWorkItems(
		broker: Broker,
		response: ShareFetchResponse,
		subscriptionByTopic: Map<string, TopicSubscription<unknown, unknown>>
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
					// Best-effort metadata refresh on leadership/unknown topic errors
					if (
						partitionResponse.errorCode === ErrorCode.NotLeaderOrFollower ||
						partitionResponse.errorCode === ErrorCode.UnknownTopicOrPartition ||
						partitionResponse.errorCode === ErrorCode.UnknownTopicId
					) {
						await this.cluster.refreshMetadata([topicName])
						await this.refreshTopicIdMaps([])
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

					if (
						partitionResponse.acknowledgeErrorCode === ErrorCode.ShareSessionNotFound ||
						partitionResponse.acknowledgeErrorCode === ErrorCode.InvalidShareSessionEpoch
					) {
						this.resetShareSessionEpoch(broker.nodeId)
					}

					throw new KafkaProtocolError(
						partitionResponse.acknowledgeErrorCode,
						partitionResponse.acknowledgeErrorMessage ??
							`ShareFetch acknowledgement failed for ${topicName}-${partitionResponse.partitionIndex}`
					)
				}

				const recordsData = partitionResponse.recordsData
				if (!recordsData || recordsData.length === 0) {
					continue
				}

				const records = await this.decodeRecords(recordsData)
				if (records.length === 0) {
					continue
				}

				const acquiredRecords = partitionResponse.acquiredRecords

				for (const record of records) {
					if (signal.aborted || this.state !== 'running') break

					const range = acquiredRecords.find(
						r => record.offset >= r.firstOffset && record.offset <= r.lastOffset
					)

					items.push({
						topicName,
						topicId: topicResponse.topicId,
						partitionIndex: partitionResponse.partitionIndex,
						record,
						deliveryCount: range?.deliveryCount,
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
					throw new Error(`Share message ${topicName}[${partitionIndex}]@${record.offset} already handled`)
				}
				handled = 1
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_ACCEPT)
			},
			release: async () => {
				if (handled !== 0) {
					throw new Error(`Share message ${topicName}[${partitionIndex}]@${record.offset} already handled`)
				}
				handled = 2
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_RELEASE)
			},
			reject: async () => {
				if (handled !== 0) {
					throw new Error(`Share message ${topicName}[${partitionIndex}]@${record.offset} already handled`)
				}
				handled = 3
				await ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_REJECT)
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
			void ackManager.enqueue(topicName, topicId, partitionIndex, record.offset, ACK_ACCEPT).catch(() => {})
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
				if (this.assignment.length === 0) {
					await sleep(opts.idleBackoffMs, { signal, resolveOnAbort: true })
					continue
				}

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

				if (partitionsByLeader.size === 0) {
					await sleep(opts.idleBackoffMs, { signal, resolveOnAbort: true })
					continue
				}

				const fetchTasks: Array<Promise<{ broker: Broker; response: ShareFetchResponse } | null>> = []

				for (const [leaderId, byTopicId] of partitionsByLeader) {
					fetchTasks.push(
						(async () => {
							const broker = await this.cluster.getBroker(leaderId)

							const topicPartitions = [...byTopicId.entries()].map(([topicId, partitions]) => ({
								topicId,
								partitions,
							}))

							try {
								const response = await this.shareFetch(broker, topicPartitions)
								return { broker, response }
							} catch (error) {
								if (
									error instanceof KafkaProtocolError &&
									(error.errorCode === ErrorCode.ShareSessionNotFound ||
										error.errorCode === ErrorCode.InvalidShareSessionEpoch)
								) {
									this.logger.debug('share session error, retrying with a new session', {
										leaderId,
										errorCode: error.errorCode,
										error: error.message,
									})
									return null
								}
								throw error
							}
						})()
					)
				}

				const fetchResults = await Promise.all(fetchTasks)
				const workItems: ShareFetchWorkItem[] = []
				for (const result of fetchResults) {
					if (!result || signal.aborted || this.state !== 'running') {
						continue
					}
					workItems.push(
						...(await this.collectShareFetchWorkItems(result.broker, result.response, subscriptionByTopic))
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

		const implicitAck = (message: ShareMessage<unknown, unknown>) => {
			void message.ack().catch(error => {
				const err = error instanceof Error ? error : new Error(String(error))
				if (err.message.includes('already handled')) {
					return
				}
				this.emitError(err)
				this.stop()
			})
		}

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
					if (previousMessage) {
						implicitAck(previousMessage)
					}
					previousMessage = item.message
					yield item as { message: ShareMessage<ShareMsgOf<S>, ShareKeyOf<S>>; ctx: ConsumeContext }
				}
			}
		} finally {
			if (previousMessage) {
				implicitAck(previousMessage)
				previousMessage = null
			}

			this.stop()
			const promises = [heartbeatPromise, fetchPromise].filter((p): p is Promise<void> => p !== null)
			await Promise.allSettled(promises)
			await this.finalizeRun()
		}
	}
}
