import type { Broker } from '@/client/broker.js'
import { KafkaProtocolError } from '@/client/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type {
	ShareAcknowledgeRequest,
	ShareAcknowledgeRequestPartition,
} from '@/protocol/messages/requests/share-acknowledge.js'
import type { ShareAcknowledgeResponse } from '@/protocol/messages/responses/share-acknowledge.js'

import type { Logger } from '@/logger.js'

export const ACK_ACCEPT = 1
export const ACK_RELEASE = 2
export const ACK_REJECT = 3
/** RENEW (KIP-1222, ShareAcknowledge/ShareFetch v2+) extends the acquisition lock without finalizing delivery. */
export const ACK_RENEW = 4

export type AcknowledgeType = typeof ACK_ACCEPT | typeof ACK_RELEASE | typeof ACK_REJECT | typeof ACK_RENEW

type PendingAckEntry = {
	offset: bigint
	type: AcknowledgeType
	resolve: () => void
	reject: (error: Error) => void
}

export type ShareAcknowledgeRequestWithoutEpoch = Omit<ShareAcknowledgeRequest, 'shareSessionEpoch'>

type PendingPartitionAcks = {
	topicName: string
	topicId: string
	partitionIndex: number
	entries: PendingAckEntry[]
}

/**
 * Pick a single entry per offset. A finalizing ack (ACCEPT/RELEASE/REJECT) supersedes
 * any RENEW for the same offset; multiple RENEWs collapse to one. Dropped entries' promises
 * resolve/reject together with the kept entry so callers don't observe a stalled renew().
 *
 * Returns the original array (no allocation) when every offset is already unique, which is
 * the common case — duplicates only arise when the user calls `renew()` on a message that
 * is also being ack'd, or `renew()` more than once on the same message.
 */
function dedupePendingAckEntries(entries: PendingAckEntry[]): PendingAckEntry[] {
	if (entries.length < 2) return entries

	const byOffset = new Map<bigint, PendingAckEntry[]>()
	let hasDuplicates = false
	for (const e of entries) {
		const list = byOffset.get(e.offset)
		if (list) {
			list.push(e)
			hasDuplicates = true
		} else {
			byOffset.set(e.offset, [e])
		}
	}
	if (!hasDuplicates) return entries

	const result: PendingAckEntry[] = []
	for (const list of byOffset.values()) {
		if (list.length === 1) {
			result.push(list[0]!)
			continue
		}
		const finalizer = list.find(e => e.type !== ACK_RENEW)
		const kept = finalizer ?? list[0]!
		const shadows = list.filter(e => e !== kept)
		result.push({
			offset: kept.offset,
			type: kept.type,
			resolve: () => {
				kept.resolve()
				for (const s of shadows) s.resolve()
			},
			reject: err => {
				kept.reject(err)
				for (const s of shadows) s.reject(err)
			},
		})
	}
	return result
}

/**
 * Coalesce sorted ack entries into ranges where consecutive offsets have the same type.
 * This reduces the number of acknowledgement batches sent to the broker. Also reports whether
 * any RENEW (4) entries are present so the caller can set the request's `IsRenewAck` flag
 * (KIP-1222) without re-walking the batches.
 */
function coalesceAckEntries(entries: PendingAckEntry[]): {
	batches: ShareAcknowledgeRequestPartition['acknowledgementBatches']
	entriesByBatch: PendingAckEntry[][]
	hasRenew: boolean
} {
	if (entries.length === 0) {
		return { batches: [], entriesByBatch: [], hasRenew: false }
	}

	const deduped = dedupePendingAckEntries(entries)
	const sorted = (deduped === entries ? [...deduped] : deduped).sort((a, b) =>
		a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0
	)

	const batches: ShareAcknowledgeRequestPartition['acknowledgementBatches'] = []
	const entriesByBatch: PendingAckEntry[][] = []
	let hasRenew = false

	let currentBatch: (typeof batches)[number] | null = null
	let currentEntries: PendingAckEntry[] = []

	for (const entry of sorted) {
		if (entry.type === ACK_RENEW) hasRenew = true
		if (
			currentBatch === null ||
			entry.type !== currentBatch.acknowledgeTypes[0] ||
			entry.offset !== currentBatch.lastOffset + 1n
		) {
			if (currentBatch !== null) {
				batches.push(currentBatch)
				entriesByBatch.push(currentEntries)
			}
			currentBatch = {
				firstOffset: entry.offset,
				lastOffset: entry.offset,
				acknowledgeTypes: [entry.type],
			}
			currentEntries = [entry]
		} else {
			currentBatch.lastOffset = entry.offset
			currentEntries.push(entry)
		}
	}

	if (currentBatch !== null) {
		batches.push(currentBatch)
		entriesByBatch.push(currentEntries)
	}

	return { batches, entriesByBatch, hasRenew }
}

/** Delay before flushing acks to allow batching (ms) */
const ACK_FLUSH_DELAY_MS = 5

export class AckManager {
	private pendingByPartitionKey = new Map<string, PendingPartitionAcks>()
	private scheduledTimer: ReturnType<typeof setTimeout> | null = null
	private flushing: Promise<void> | null = null
	private totalPending = 0
	private closed = false

	constructor(
		private readonly groupId: string,
		private readonly getMemberId: () => string,
		private readonly sendAcknowledge: (
			broker: Broker,
			request: ShareAcknowledgeRequestWithoutEpoch
		) => Promise<ShareAcknowledgeResponse>,
		private readonly resolveLeader: (topic: string, partition: number) => Promise<Broker>,
		private readonly refreshMetadata: (topics?: string[]) => Promise<void>,
		private readonly resetShareSessionEpoch: (brokerId: number) => void,
		private readonly logger: Logger,
		private readonly ackBatchSize: number
	) {}

	enqueue(
		topicName: string,
		topicId: string,
		partitionIndex: number,
		offset: bigint,
		type: AcknowledgeType
	): Promise<void> {
		if (this.closed) {
			// Consumer is stopping/stopped; ignore late acks (prevents timers firing after shutdown).
			return Promise.resolve()
		}

		const key = `${topicId}:${partitionIndex}`
		let pending = this.pendingByPartitionKey.get(key)
		if (!pending) {
			pending = {
				topicName,
				topicId,
				partitionIndex,
				entries: [],
			}
			this.pendingByPartitionKey.set(key, pending)
		}
		pending.topicName = topicName

		let resolve!: () => void
		let reject!: (error: Error) => void
		const promise = new Promise<void>((res, rej) => {
			resolve = res
			reject = rej
		})

		pending.entries.push({
			offset,
			type,
			resolve,
			reject,
		})
		this.totalPending++

		if (this.totalPending >= this.ackBatchSize) {
			// Batch is full, flush immediately
			this.cancelScheduledFlush()
			void this.flushAll().catch(err => {
				this.logger.error('share acknowledge flush failed', { error: (err as Error).message })
			})
		} else {
			this.scheduleFlush()
		}

		return promise
	}

	private cancelScheduledFlush(): void {
		if (this.scheduledTimer !== null) {
			clearTimeout(this.scheduledTimer)
			this.scheduledTimer = null
		}
	}

	private scheduleFlush(): void {
		if (this.scheduledTimer !== null) {
			return
		}
		this.scheduledTimer = setTimeout(() => {
			this.scheduledTimer = null
			void this.flushAll().catch(err => {
				this.logger.error('share acknowledge flush failed', { error: (err as Error).message })
			})
		}, ACK_FLUSH_DELAY_MS)
	}

	async flushAll(): Promise<void> {
		this.cancelScheduledFlush()
		if (this.flushing) {
			return this.flushing
		}

		const promise = this.flushAllLoop().finally(() => {
			this.flushing = null
		})
		this.flushing = promise
		return promise
	}

	/**
	 * Stop scheduling periodic flushes and best-effort flush any outstanding acks.
	 * After shutdown, all enqueues are ignored.
	 */
	async shutdown(): Promise<void> {
		if (this.closed) {
			return this.flushing ?? Promise.resolve()
		}
		this.closed = true
		this.cancelScheduledFlush()
		await this.flushAll()
	}

	private async flushAllLoop(): Promise<void> {
		try {
			while (this.pendingByPartitionKey.size > 0) {
				const snapshot = this.pendingByPartitionKey
				this.pendingByPartitionKey = new Map()
				this.totalPending = 0
				await this.flushSnapshotWithRetry(snapshot)
			}
		} catch (error) {
			const err = error instanceof Error ? error : new Error(String(error))
			for (const pending of this.pendingByPartitionKey.values()) {
				for (const e of pending.entries) {
					e.reject(err)
				}
			}
			this.pendingByPartitionKey.clear()
			this.totalPending = 0
			throw err
		}
	}

	private async flushSnapshotWithRetry(snapshot: Map<string, PendingPartitionAcks>): Promise<void> {
		if (snapshot.size === 0) {
			return
		}

		let remaining = snapshot
		for (let attempt = 0; attempt < 2; attempt++) {
			const { retry, retryErrors } = await this.flushPartitions(remaining)
			if (retry.size === 0) {
				return
			}

			if (attempt === 0) {
				const topicsToRefresh = [...new Set([...retry.values()].map(p => p.topicName))]
				if (topicsToRefresh.length > 0) {
					await this.refreshMetadata(topicsToRefresh)
				}
			}

			remaining = retry

			if (attempt === 1 && remaining.size > 0) {
				for (const [key, pending] of remaining) {
					const err = retryErrors.get(key) ?? new Error('ShareAcknowledge failed after retry')
					for (const e of pending.entries) {
						e.reject(err)
					}
				}
				throw [...retryErrors.values()][0] ?? new Error('ShareAcknowledge failed after retry')
			}
		}
	}

	private async flushPartitions(pendingByKey: Map<string, PendingPartitionAcks>): Promise<{
		retry: Map<string, PendingPartitionAcks>
		retryErrors: Map<string, Error>
	}> {
		const isLeaderError = (errorCode: ErrorCode): boolean =>
			errorCode === ErrorCode.NotLeaderOrFollower ||
			errorCode === ErrorCode.UnknownTopicOrPartition ||
			errorCode === ErrorCode.UnknownTopicId

		const isShareSessionError = (errorCode: ErrorCode): boolean =>
			errorCode === ErrorCode.ShareSessionNotFound || errorCode === ErrorCode.InvalidShareSessionEpoch

		const retry = new Map<string, PendingPartitionAcks>()
		const retryErrors = new Map<string, Error>()

		const partitions = [...pendingByKey.values()]
		const resolved = await Promise.allSettled(
			partitions.map(async partition => {
				const broker = await this.resolveLeader(partition.topicName, partition.partitionIndex)
				return { broker, partition }
			})
		)

		const partitionsByBroker = new Map<number, Array<{ broker: Broker; partition: PendingPartitionAcks }>>()

		for (let i = 0; i < resolved.length; i++) {
			const result = resolved[i]!
			const partition = partitions[i]!

			if (result.status === 'rejected') {
				const err = result.reason instanceof Error ? result.reason : new Error(String(result.reason))
				const key = `${partition.topicId}:${partition.partitionIndex}`
				retry.set(key, partition)
				retryErrors.set(key, err)
				continue
			}

			const { broker } = result.value
			const entries = partitionsByBroker.get(broker.nodeId) ?? []
			entries.push({ broker, partition })
			partitionsByBroker.set(broker.nodeId, entries)
		}

		const results = await Promise.allSettled(
			[...partitionsByBroker.entries()].map(async ([brokerId, items]) => {
				try {
					const broker = items[0]!.broker

					const topicMap = new Map<string, ShareAcknowledgeRequest['topics'][number]>()
					let hasRenewAck = false
					for (const { partition } of items) {
						const { batches, hasRenew } = coalesceAckEntries(partition.entries)
						if (hasRenew) hasRenewAck = true

						const topicEntry = topicMap.get(partition.topicId) ?? {
							topicId: partition.topicId,
							partitions: [],
						}
						topicEntry.partitions.push({
							partitionIndex: partition.partitionIndex,
							acknowledgementBatches: batches,
						})
						topicMap.set(partition.topicId, topicEntry)
					}

					const topics = [...topicMap.values()]

					const request: ShareAcknowledgeRequestWithoutEpoch = {
						groupId: this.groupId,
						memberId: this.getMemberId(),
						isRenewAck: hasRenewAck,
						topics,
					}

					const response = await this.sendAcknowledge(broker, request)
					if (response.errorCode !== ErrorCode.None) {
						throw new KafkaProtocolError(
							response.errorCode,
							response.errorMessage ?? 'ShareAcknowledge failed'
						)
					}

					const responseByPartitionKey = new Map<
						string,
						ShareAcknowledgeResponse['topics'][number]['partitions'][number]
					>()
					for (const topic of response.topics) {
						for (const p of topic.partitions) {
							responseByPartitionKey.set(`${topic.topicId}:${p.partitionIndex}`, p)
						}
					}

					let fatalError: Error | null = null
					for (const { partition } of items) {
						const key = `${partition.topicId}:${partition.partitionIndex}`
						const partitionResponse = responseByPartitionKey.get(key)
						if (!partitionResponse) {
							const err = new Error(
								`ShareAcknowledge response missing ${partition.topicId}:${partition.partitionIndex}`
							)
							for (const e of partition.entries) {
								e.reject(err)
							}
							fatalError ??= err
							continue
						}

						if (partitionResponse.errorCode === ErrorCode.None) {
							for (const e of partition.entries) {
								e.resolve()
							}
							continue
						}

						const err = new KafkaProtocolError(
							partitionResponse.errorCode,
							partitionResponse.errorMessage ??
								`ShareAcknowledge failed for ${partition.topicName}-${partition.partitionIndex}`
						)

						if (isLeaderError(partitionResponse.errorCode)) {
							retry.set(key, partition)
							retryErrors.set(key, err)
							continue
						}

						if (isShareSessionError(partitionResponse.errorCode)) {
							this.resetShareSessionEpoch(brokerId)
							retry.set(key, partition)
							retryErrors.set(key, err)
							continue
						}

						for (const e of partition.entries) {
							e.reject(err)
						}
						fatalError ??= err
					}

					if (fatalError) {
						throw fatalError
					}
				} catch (error) {
					const err = error instanceof Error ? error : new Error(String(error))
					if (
						err instanceof KafkaProtocolError &&
						(isLeaderError(err.errorCode) || isShareSessionError(err.errorCode))
					) {
						if (isShareSessionError(err.errorCode)) {
							this.resetShareSessionEpoch(brokerId)
						}
						for (const { partition } of items) {
							const key = `${partition.topicId}:${partition.partitionIndex}`
							retry.set(key, partition)
							retryErrors.set(key, err)
						}
						return
					}
					for (const { partition } of items) {
						for (const e of partition.entries) {
							e.reject(err)
						}
					}
					throw err
				}
			})
		)

		let topError: Error | null = null
		for (const r of results) {
			if (r.status === 'rejected') {
				const err = r.reason instanceof Error ? r.reason : new Error(String(r.reason))
				topError ??= err
			}
		}

		if (topError) {
			for (const pending of retry.values()) {
				for (const e of pending.entries) {
					e.reject(topError)
				}
			}
			this.logger.error('share acknowledge failed', { error: topError.message })
			throw topError
		}

		return { retry, retryErrors }
	}
}
