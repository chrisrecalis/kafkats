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

export type AcknowledgeType = typeof ACK_ACCEPT | typeof ACK_RELEASE | typeof ACK_REJECT

type PendingAckEntry = {
	batch: ShareAcknowledgeRequestPartition['acknowledgementBatches'][number]
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

export class AckManager {
	private pendingByPartitionKey = new Map<string, PendingPartitionAcks>()
	private scheduled = false
	private flushing: Promise<void> | null = null

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
		const entries = pending.entries

		let resolve!: () => void
		let reject!: (error: Error) => void
		const promise = new Promise<void>((res, rej) => {
			resolve = res
			reject = rej
		})

		entries.push({
			batch: {
				firstOffset: offset,
				lastOffset: offset,
				acknowledgeTypes: [type],
			},
			resolve,
			reject,
		})

		const pendingCount = entries.length
		if (pendingCount >= this.ackBatchSize) {
			void this.flushAll().catch(err => {
				this.logger.error('share acknowledge flush failed', { error: (err as Error).message })
			})
		} else {
			this.scheduleFlush()
		}

		return promise
	}

	private scheduleFlush(): void {
		if (this.scheduled) {
			return
		}
		this.scheduled = true
		setTimeout(() => {
			this.scheduled = false
			void this.flushAll().catch(err => {
				this.logger.error('share acknowledge flush failed', { error: (err as Error).message })
			})
		}, 0)
	}

	async flushAll(): Promise<void> {
		if (this.flushing) {
			return this.flushing
		}

		const promise = this.flushAllLoop().finally(() => {
			this.flushing = null
		})
		this.flushing = promise
		return promise
	}

	private async flushAllLoop(): Promise<void> {
		try {
			while (this.pendingByPartitionKey.size > 0) {
				const snapshot = this.pendingByPartitionKey
				this.pendingByPartitionKey = new Map()
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
					for (const { partition } of items) {
						const topicEntry = topicMap.get(partition.topicId) ?? {
							topicId: partition.topicId,
							partitions: [],
						}
						topicEntry.partitions.push({
							partitionIndex: partition.partitionIndex,
							acknowledgementBatches: partition.entries.map(e => e.batch),
						})
						topicMap.set(partition.topicId, topicEntry)
					}

					const topics = [...topicMap.values()]

					const request: ShareAcknowledgeRequestWithoutEpoch = {
						groupId: this.groupId,
						memberId: this.getMemberId(),
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
