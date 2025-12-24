/**
 * Offset manager for consumer
 *
 * Handles:
 * - Tracking consumed offsets
 * - Auto-commit loop
 * - Fetching committed offsets
 * - Resolving starting offsets (earliest/latest/committed)
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { isKafkaError, KafkaProtocolError, shouldRefreshMetadata } from '@/client/errors.js'
import { OFFSET_TIMESTAMP } from '@/protocol/messages/requests/list-offsets.js'
import type { TopicPartition, AutoOffsetReset } from './types.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey } from '@/utils/topic-partition.js'
import { sleep } from '@/utils/sleep.js'

/**
 * Offset manager
 */
export class OffsetManager {
	private readonly cluster: Cluster
	private readonly groupId: string
	private readonly groupInstanceId?: string
	private readonly logger: Logger

	// Current state (set by consumer group on join/rejoin)
	private memberId: string = ''
	private generationId: number = -1
	private coordinator: Broker | null = null

	// Consumed offsets (next offset to commit = consumed + 1)
	private readonly consumedOffsets: Map<string, bigint> = new Map()

	// Auto-commit state
	private autoCommitTimer: ReturnType<typeof setTimeout> | null = null
	private abortController: AbortController | null = null

	constructor(cluster: Cluster, groupId: string, groupInstanceId?: string, logger?: Logger) {
		this.cluster = cluster
		this.groupId = groupId
		this.groupInstanceId = groupInstanceId
		this.logger = logger?.child({ component: 'offset-manager', groupId }) ?? noopLogger
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
	 * Mark a message as consumed
	 * The committed offset will be the next offset (consumed + 1)
	 */
	markConsumed(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
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
				.catch(error => {
					this.logger.error('auto-commit failed', { error: (error as Error).message })
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
	 * Commit all pending offsets
	 */
	async commitPendingOffsets(): Promise<void> {
		if (this.consumedOffsets.size === 0 || !this.coordinator) {
			return
		}

		this.logger.debug('committing offsets', { count: this.consumedOffsets.size })

		// Build commit request
		const topicMap = new Map<string, Map<number, bigint>>()
		for (const [key, offset] of this.consumedOffsets) {
			const [topic, partitionStr] = key.split(':')
			const partition = parseInt(partitionStr!, 10)

			if (!topicMap.has(topic!)) {
				topicMap.set(topic!, new Map())
			}
			topicMap.get(topic!)!.set(partition, offset + 1n)
		}

		const topics = Array.from(topicMap.entries()).map(([name, partitions]) => ({
			name,
			partitions: Array.from(partitions.entries()).map(([partitionIndex, committedOffset]) => ({
				partitionIndex,
				committedOffset,
			})),
		}))

		const response = await this.coordinator.offsetCommit({
			groupId: this.groupId,
			generationId: this.generationId,
			memberId: this.memberId,
			groupInstanceId: this.groupInstanceId ?? null,
			topics,
		})

		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				if (partition.errorCode !== ErrorCode.None) {
					this.logger.error('offset commit error', {
						topic: topic.name,
						partition: partition.partitionIndex,
						errorCode: partition.errorCode,
					})
					throw new KafkaProtocolError(
						partition.errorCode,
						`OffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
					)
				}
			}
		}

		this.logger.debug('offsets committed successfully')
		this.consumedOffsets.clear()
	}

	/**
	 * Commit offsets for specific partitions only (for cooperative rebalance)
	 * Generation-safe: no-op if group state is not ready (prevents throws during early-phase revoke)
	 */
	async commitPartitions(partitions: TopicPartition[]): Promise<void> {
		// No-op if not ready (prevents throws during early-phase revoke)
		if (!this.coordinator || !this.memberId || this.generationId < 0) {
			this.logger.debug('skipping partition commit - group state not ready')
			return
		}

		if (partitions.length === 0) {
			return
		}

		// Filter consumedOffsets to only requested partitions
		const toCommit = new Map<string, bigint>()
		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const offset = this.consumedOffsets.get(key)
			if (offset !== undefined) {
				toCommit.set(key, offset)
			}
		}

		if (toCommit.size === 0) {
			this.logger.debug('no consumed offsets to commit for requested partitions')
			return
		}

		this.logger.debug('committing offsets for specific partitions', {
			requestedCount: partitions.length,
			toCommitCount: toCommit.size,
		})

		// Build commit request
		const topicMap = new Map<string, Map<number, bigint>>()
		for (const [key, offset] of toCommit) {
			const [topic, partitionStr] = key.split(':')
			const partition = parseInt(partitionStr!, 10)

			if (!topicMap.has(topic!)) {
				topicMap.set(topic!, new Map())
			}
			topicMap.get(topic!)!.set(partition, offset + 1n)
		}

		const topics = Array.from(topicMap.entries()).map(([name, partitions]) => ({
			name,
			partitions: Array.from(partitions.entries()).map(([partitionIndex, committedOffset]) => ({
				partitionIndex,
				committedOffset,
			})),
		}))

		const response = await this.coordinator.offsetCommit({
			groupId: this.groupId,
			generationId: this.generationId,
			memberId: this.memberId,
			groupInstanceId: this.groupInstanceId ?? null,
			topics,
		})

		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				if (partition.errorCode !== ErrorCode.None) {
					this.logger.error('partition offset commit error', {
						topic: topic.name,
						partition: partition.partitionIndex,
						errorCode: partition.errorCode,
					})
					throw new KafkaProtocolError(
						partition.errorCode,
						`OffsetCommit failed for ${topic.name}-${partition.partitionIndex}`
					)
				}
			}
		}

		this.logger.debug('partition offsets committed successfully', { count: toCommit.size })

		// Remove ONLY committed partitions from consumedOffsets
		for (const key of toCommit.keys()) {
			this.consumedOffsets.delete(key)
		}
	}

	/**
	 * Fetch committed offsets for partitions
	 */
	async fetchCommittedOffsets(partitions: TopicPartition[]): Promise<Map<string, bigint>> {
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

		const response = await this.coordinator.offsetFetch({
			groupId: this.groupId,
			topics,
		})

		// Check for top-level error
		if (response.errorCode !== ErrorCode.None) {
			this.logger.error('offset fetch failed', { errorCode: response.errorCode })
			throw new KafkaProtocolError(response.errorCode, 'OffsetFetch failed')
		}

		// Build result map
		const result = new Map<string, bigint>()
		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				if (partition.errorCode !== ErrorCode.None) {
					continue
				}
				// -1 means no committed offset
				if (partition.committedOffset >= 0n) {
					const key = tpKey(topic.name, partition.partitionIndex)
					result.set(key, partition.committedOffset)
				}
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
	 */
	async resolveStartingOffset(
		topic: string,
		partition: number,
		autoOffsetReset: AutoOffsetReset,
		committedOffsets: Map<string, bigint>
	): Promise<bigint> {
		const key = tpKey(topic, partition)

		const committed = committedOffsets.get(key)
		if (committed !== undefined) {
			return committed
		}

		// No committed offset - apply reset strategy
		switch (autoOffsetReset) {
			case 'earliest':
				return this.getEarliestOffset(topic, partition)
			case 'latest':
				return this.getLatestOffset(topic, partition)
			case 'none':
				throw new Error(`No committed offset for ${topic}-${partition} and autoOffsetReset is 'none'`)
		}
	}

	/**
	 * Get earliest offset for a partition
	 */
	async getEarliestOffset(topic: string, partition: number): Promise<bigint> {
		return this.listOffset(topic, partition, OFFSET_TIMESTAMP.EARLIEST)
	}

	/**
	 * Get latest offset for a partition
	 */
	async getLatestOffset(topic: string, partition: number): Promise<bigint> {
		return this.listOffset(topic, partition, OFFSET_TIMESTAMP.LATEST)
	}

	/**
	 * List offset for a specific timestamp
	 */
	private async listOffset(topic: string, partition: number, timestamp: bigint): Promise<bigint> {
		const maxAttempts = 5
		let lastError: unknown

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			try {
				const leader = await this.cluster.getLeaderForPartition(topic, partition)

				const response = await leader.listOffsets({
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
			} catch (error) {
				lastError = error

				const retriable = isKafkaError(error) && error.retriable
				if (!retriable || attempt >= maxAttempts) {
					throw error
				}

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

				const delayMs = Math.min(100 * 2 ** (attempt - 1), 2000)
				this.logger.debug('retrying listOffsets after error', {
					topic,
					partition,
					attempt,
					delayMs,
					error: error instanceof Error ? error.message : String(error),
				})
				await sleep(delayMs)
			}
		}

		throw lastError instanceof Error ? lastError : new Error(`ListOffsets failed for ${topic}-${partition}`)
	}
}
