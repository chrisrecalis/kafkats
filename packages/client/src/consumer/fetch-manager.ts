/**
 * Fetch manager for consumer
 *
 * Handles:
 * - Fetching records from assigned partitions
 * - Managing partition concurrency
 * - Decoding record batches
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import {
	decodeRecordBatchFrom,
	decodeRecordBatchFromSync,
	isControlBatch,
	isTransactional,
} from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import type { TopicPartition, AutoOffsetReset, IsolationLevel, PartitionBatch } from './types.js'
import type { OffsetManager } from './offset-manager.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey, formatPartitions } from '@/utils/topic-partition.js'
import { sleep } from '@/utils/sleep.js'

function filterRecordsFromOffset(records: DecodedRecord[], minOffset: bigint): DecodedRecord[] {
	if (records.length === 0) {
		return records
	}

	if (records[0]!.offset >= minOffset) {
		return records
	}
	let start = 0
	while (start < records.length && records[start]!.offset < minOffset) {
		start++
	}

	return start === 0 ? records : records.slice(start)
}

/**
 * Completed fetch from a single partition
 * Similar to Java client's CompletedFetch
 */
interface CompletedFetch {
	topic: string
	partition: number
	records: DecodedRecord[]
	byteSize: number
}

/**
 * Buffer for completed fetches with memory bounding and wait/signal
 */
class FetchBuffer {
	private queue: CompletedFetch[] = []
	private bufferedBytes = 0
	private readonly maxBytes: number
	private waiters: Array<() => void> = []

	constructor(maxBytes: number) {
		this.maxBytes = maxBytes
	}

	/**
	 * Add a completed fetch to the buffer and signal any waiters
	 */
	add(fetch: CompletedFetch): void {
		this.queue.push(fetch)
		this.bufferedBytes += fetch.byteSize
		this.signalWaiters()
	}

	/**
	 * Drain all buffered fetches
	 */
	drain(): CompletedFetch[] {
		const result = this.queue
		this.queue = []
		this.bufferedBytes = 0
		return result
	}

	isEmpty(): boolean {
		return this.queue.length === 0
	}

	isFull(): boolean {
		return this.bufferedBytes >= this.maxBytes
	}

	/**
	 * Wait for data to be available in the buffer
	 * @returns true if data is available, false if timeout/aborted
	 */
	async waitForData(timeoutMs: number, signal?: AbortSignal): Promise<boolean> {
		if (!this.isEmpty()) {
			return true
		}

		return new Promise<boolean>(resolve => {
			let timeoutId: ReturnType<typeof setTimeout> | null = null
			let onAbort: (() => void) | null = null

			const cleanup = () => {
				if (timeoutId) {
					clearTimeout(timeoutId)
					timeoutId = null
				}
				if (signal && onAbort) {
					signal.removeEventListener('abort', onAbort)
				}
				// Remove from waiters
				const idx = this.waiters.indexOf(waiterCallback)
				if (idx !== -1) {
					this.waiters.splice(idx, 1)
				}
			}

			const waiterCallback = () => {
				cleanup()
				resolve(true)
			}

			this.waiters.push(waiterCallback)

			// Set timeout
			timeoutId = setTimeout(() => {
				cleanup()
				resolve(false)
			}, timeoutMs)

			// Handle abort signal
			if (signal) {
				if (signal.aborted) {
					cleanup()
					resolve(false)
					return
				}
				onAbort = () => {
					cleanup()
					resolve(false)
				}
				signal.addEventListener('abort', onAbort)
			}
		})
	}

	/**
	 * Signal all waiters that data is available
	 */
	private signalWaiters(): void {
		const waiters = this.waiters
		this.waiters = []
		for (const waiter of waiters) {
			waiter()
		}
	}

	/**
	 * Clear all buffered data
	 */
	clear(): void {
		this.queue = []
		this.bufferedBytes = 0
	}

	/**
	 * Remove buffered data for partitions not in the given set
	 */
	removePartitionsNotIn(validKeys: Set<string>): void {
		let newBytes = 0
		const filtered: CompletedFetch[] = []
		for (const f of this.queue) {
			if (validKeys.has(tpKey(f.topic, f.partition))) {
				filtered.push(f)
				newBytes += f.byteSize
			}
		}
		this.queue = filtered
		this.bufferedBytes = newBytes
	}

	/**
	 * Remove buffered data for specific partitions
	 */
	removePartitions(partitions: Array<{ topic: string; partition: number }>): void {
		if (partitions.length === 0) return
		const keysToRemove = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
		let newBytes = 0
		const filtered: CompletedFetch[] = []
		for (const f of this.queue) {
			if (!keysToRemove.has(tpKey(f.topic, f.partition))) {
				filtered.push(f)
				newBytes += f.byteSize
			}
		}
		this.queue = filtered
		this.bufferedBytes = newBytes
	}
}

/**
 * Per-partition fetch state
 */
interface PartitionState {
	topic: string
	partition: number
	offset: bigint
	paused: boolean
	abortController: AbortController // Per-partition abort signal for incremental control
	cachedLeader: Broker | null // Cached leader broker - invalidated on NotLeaderOrFollower
}

/**
 * Fetch manager configuration
 */
export interface FetchManagerConfig {
	maxBytesPerPartition: number
	minBytes: number
	maxWaitMs: number
	partitionConcurrency: number
	isolationLevel: IsolationLevel
	/** Maximum bytes to buffer for poll() mode. Default: maxBytesPerPartition * 10 */
	maxBufferedBytes?: number
}

/**
 * Default fetch manager configuration
 */
export const DEFAULT_FETCH_CONFIG: FetchManagerConfig = {
	maxBytesPerPartition: 1048576, // 1MB
	minBytes: 1,
	maxWaitMs: 5000,
	partitionConcurrency: 1,
	isolationLevel: 'read_committed',
}

/**
 * Fetch manager
 *
 * Uses broker-level fetch batching: groups all ready partitions by their leader broker
 * and issues one fetch request per broker containing all partitions.
 */
export class FetchManager {
	private readonly cluster: Cluster
	private readonly config: FetchManagerConfig
	private readonly partitionStates: Map<string, PartitionState> = new Map()
	private readonly logger: Logger
	private readonly autoOffsetReset?: AutoOffsetReset
	private readonly offsetManager?: OffsetManager
	private abortController: AbortController | null = null
	private running = false

	// Buffered poll mode fields
	private fetchBuffer: FetchBuffer | null = null
	private brokerInFlight: Map<number, boolean> = new Map()

	constructor(
		cluster: Cluster,
		offsetManager: OffsetManager,
		autoOffsetReset: AutoOffsetReset,
		config: Partial<FetchManagerConfig>,
		logger?: Logger
	) {
		this.cluster = cluster
		this.offsetManager = offsetManager
		this.autoOffsetReset = autoOffsetReset
		this.config = { ...DEFAULT_FETCH_CONFIG, ...config }
		this.logger = logger?.child({ component: 'fetch-manager' }) ?? noopLogger
	}

	/**
	 * Set partitions to fetch from with their starting offsets
	 * This replaces all partitions (used for eager rebalance)
	 */
	setPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		this.logger.debug('setting partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})
		// Abort all existing partition loops
		for (const state of this.partitionStates.values()) {
			state.abortController.abort()
		}
		this.partitionStates.clear()

		// Clear buffer for removed partitions (keep only new partitions)
		if (this.fetchBuffer) {
			const newKeys = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
			this.fetchBuffer.removePartitionsNotIn(newKeys)
		}

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
			}
			this.partitionStates.set(key, state)
		}
	}

	/**
	 * Add partitions incrementally (used for cooperative rebalance)
	 */
	addPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		if (partitions.length === 0) return

		this.logger.debug('adding partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			// Skip if already exists
			if (this.partitionStates.has(key)) {
				this.logger.debug('partition already exists, skipping', { topic: tp.topic, partition: tp.partition })
				continue
			}

			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
			}
			this.partitionStates.set(key, state)
		}
	}

	/**
	 * Remove partitions incrementally (used for cooperative rebalance)
	 * Aborts fetch loops for removed partitions
	 */
	removePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('removing partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				// Abort the partition's fetch loop
				state.abortController.abort()
				this.partitionStates.delete(key)
			}
		}
	}

	/**
	 * Pause partitions (stop fetching but keep state)
	 * Also clears any buffered records for the paused partitions
	 */
	pausePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('pausing partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = true
			}
		}

		// Clear buffered records for paused partitions to ensure they are not delivered
		this.fetchBuffer?.removePartitions(partitions)
	}

	/**
	 * Resume paused partitions
	 */
	resumePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('resuming partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = false
			}
		}
	}

	/**
	 * Seek to a specific offset for a partition
	 *
	 * Updates the fetch position for the partition to the given offset.
	 * The next fetch request will request records starting from this offset.
	 *
	 * @param topic - Topic name
	 * @param partition - Partition index
	 * @param offset - The offset to fetch from next
	 */
	seekPartition(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (state) {
			this.logger.debug('seeking partition', {
				topic,
				partition,
				previousOffset: String(state.offset),
				newOffset: String(offset),
			})
			state.offset = offset
		} else {
			this.logger.warn('seek attempted on unassigned partition', { topic, partition })
		}
	}

	/**
	 * Update offset for a partition (after consuming)
	 */
	updateOffset(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (state) {
			state.offset = offset + 1n // Next offset to fetch
		}
	}

	/**
	 * Handle fetch error for a partition
	 */
	private async handleFetchError(state: PartitionState, errorCode: ErrorCode): Promise<void> {
		switch (errorCode) {
			case ErrorCode.OffsetOutOfRange:
				if (this.offsetManager && this.autoOffsetReset) {
					switch (this.autoOffsetReset) {
						case 'earliest':
							state.offset = await this.offsetManager.getEarliestOffset(state.topic, state.partition)
							break
						case 'latest':
							state.offset = await this.offsetManager.getLatestOffset(state.topic, state.partition)
							break
						case 'none':
							throw new Error(
								`Offset out of range for ${state.topic}-${state.partition} at offset ${state.offset}`
							)
					}
				}
				break

			case ErrorCode.NotLeaderOrFollower:
			case ErrorCode.UnknownTopicOrPartition:
				state.cachedLeader = null // Invalidate cached leader
				await this.cluster.refreshMetadata([state.topic])
				break

			default:
				this.logger.error('fetch error', {
					topic: state.topic,
					partition: state.partition,
					errorCode,
				})
		}
	}

	/**
	 * Stop the fetch loop
	 */
	stop(): void {
		this.logger.debug('stopping fetch manager')
		this.running = false
		// Abort global controller (for sleep in start loop)
		this.abortController?.abort()
		// Abort all per-partition controllers
		for (const state of this.partitionStates.values()) {
			state.abortController.abort()
		}
		// Cleanup buffered poll mode
		if (this.fetchBuffer) {
			this.fetchBuffer.clear()
			this.fetchBuffer = null
		}
		this.brokerInFlight.clear()
	}

	/**
	 * Background fetch loop for buffered poll mode
	 * Continuously fetches from brokers and buffers results
	 * Uses one-outstanding-fetch-per-broker pattern like Java client
	 */
	private async runBackgroundFetchLoop(): Promise<void> {
		const signal = this.abortController?.signal
		// Reusable array to avoid allocations on every iteration
		const readyPartitions: PartitionState[] = []

		while (this.running && !signal?.aborted) {
			try {
				// Get ready (non-paused) partitions - reuse array
				readyPartitions.length = 0
				for (const state of this.partitionStates.values()) {
					if (!state.paused) {
						readyPartitions.push(state)
					}
				}

				if (readyPartitions.length === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Group partitions by leader broker
				const partitionsByBroker = await this.groupPartitionsByBroker(readyPartitions)

				if (partitionsByBroker.size === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Issue fetch to each broker that doesn't have in-flight request
				let fetchesIssued = 0
				for (const [brokerId, { broker, partitions }] of partitionsByBroker) {
					if (this.brokerInFlight.get(brokerId)) {
						continue // Skip if already fetching from this broker
					}

					this.brokerInFlight.set(brokerId, true)
					fetchesIssued++

					// Fire and forget - don't await, let it run in background
					this.fetchFromBrokerToBuffer(broker, partitions)
						.catch(error => {
							this.logger.error('background fetch failed', {
								brokerId,
								error: (error as Error).message,
							})
						})
						.finally(() => {
							this.brokerInFlight.set(brokerId, false)
						})
				}

				// Only sleep when necessary to prevent busy-loop
				if (this.fetchBuffer?.isFull()) {
					// Buffer is full, wait for consumer to drain it
					await sleep(10, { signal }).catch(() => {})
				} else if (fetchesIssued === 0) {
					// All brokers have in-flight requests, wait briefly
					await sleep(1, { signal }).catch(() => {})
				}
				// If fetches were issued and buffer is not full, continue immediately
			} catch (error) {
				if (signal?.aborted) break
				this.logger.error('background fetch loop error', { error: (error as Error).message })
				await sleep(100, { signal }).catch(() => {})
			}
		}
	}

	/**
	 * Group partitions by their leader broker
	 */
	private async groupPartitionsByBroker(
		partitions: PartitionState[]
	): Promise<Map<number, { broker: Broker; partitions: PartitionState[] }>> {
		const partitionsByBroker = new Map<number, { broker: Broker; partitions: PartitionState[] }>()

		// Collect partitions needing leader lookup
		const needsLookup: PartitionState[] = []
		for (const state of partitions) {
			if (state.cachedLeader) {
				const brokerId = state.cachedLeader.nodeId
				if (!partitionsByBroker.has(brokerId)) {
					partitionsByBroker.set(brokerId, { broker: state.cachedLeader, partitions: [] })
				}
				partitionsByBroker.get(brokerId)!.partitions.push(state)
			} else {
				needsLookup.push(state)
			}
		}

		// Parallel leader lookups for cache misses
		if (needsLookup.length > 0) {
			const lookups = await Promise.all(
				needsLookup.map(async state => {
					try {
						const leader = await this.cluster.getLeaderForPartition(state.topic, state.partition)
						state.cachedLeader = leader
						return { state, leader }
					} catch (error) {
						this.logger.error('failed to get leader', {
							topic: state.topic,
							partition: state.partition,
							error: (error as Error).message,
						})
						return null
					}
				})
			)

			for (const result of lookups) {
				if (!result) continue
				const brokerId = result.leader.nodeId
				if (!partitionsByBroker.has(brokerId)) {
					partitionsByBroker.set(brokerId, { broker: result.leader, partitions: [] })
				}
				partitionsByBroker.get(brokerId)!.partitions.push(result.state)
			}
		}

		return partitionsByBroker
	}

	/**
	 * Fetch from a single broker and add results to buffer
	 */
	private async fetchFromBrokerToBuffer(broker: Broker, partitions: PartitionState[]): Promise<void> {
		if (!this.fetchBuffer) return

		// Track fetch offsets for filtering
		const fetchOffsetByKey = new Map<string, bigint>()

		// Build topics array
		const topicsMap = new Map<string, Array<{ state: PartitionState }>>()
		for (const state of partitions) {
			if (!topicsMap.has(state.topic)) {
				topicsMap.set(state.topic, [])
			}
			topicsMap.get(state.topic)!.push({ state })
			fetchOffsetByKey.set(tpKey(state.topic, state.partition), state.offset)
		}

		const topics = Array.from(topicsMap.entries()).map(([topic, parts]) => ({
			topic,
			topicId: '00000000-0000-0000-0000-000000000000',
			partitions: parts.map(p => ({
				partitionIndex: p.state.partition,
				currentLeaderEpoch: -1,
				fetchOffset: p.state.offset,
				lastFetchedEpoch: -1,
				logStartOffset: -1n,
				partitionMaxBytes: this.config.maxBytesPerPartition,
			})),
		}))

		const response = await broker.fetch({
			replicaId: -1,
			maxWaitMs: this.config.maxWaitMs,
			minBytes: this.config.minBytes,
			maxBytes: this.config.maxBytesPerPartition * partitions.length,
			isolationLevel: this.config.isolationLevel === 'read_committed' ? 1 : 0,
			topics,
		})

		// Process response for each partition
		for (const topicResponse of response.topics) {
			for (const partitionResponse of topicResponse.partitions) {
				const key = tpKey(topicResponse.topic, partitionResponse.partitionIndex)
				const state = this.partitionStates.get(key)
				if (!state) continue

				// Handle errors
				if (partitionResponse.errorCode !== ErrorCode.None) {
					await this.handleFetchError(state, partitionResponse.errorCode)
					continue
				}

				// Decode records
				if (partitionResponse.recordsData && partitionResponse.recordsData.length > 0) {
					const decodeResult = this.decodeRecords(
						partitionResponse.recordsData,
						partitionResponse.abortedTransactions,
						partitionResponse.lastStableOffset
					)
					let records = decodeResult instanceof Promise ? await decodeResult : decodeResult

					const fetchOffset = fetchOffsetByKey.get(key)
					if (fetchOffset !== undefined) {
						records = filterRecordsFromOffset(records, fetchOffset)
					}

					if (records.length > 0) {
						// Advance offset for next fetch
						state.offset = records[records.length - 1]!.offset + 1n

						// Estimate byte size for memory tracking
						let byteSize = 0
						for (const record of records) {
							byteSize += (record.key?.length ?? 0) + (record.value?.length ?? 0) + 64
						}

						// Add to buffer
						this.fetchBuffer.add({
							topic: state.topic,
							partition: state.partition,
							records,
							byteSize,
						})
					}
				}
			}
		}
	}

	/**
	 * Poll for records from all ready partitions (pull-based API)
	 *
	 * Uses buffered polling with background fetch loop for better performance.
	 * The background loop is started lazily on first call.
	 *
	 * Returns records grouped by topic/partition. Internal offsets are advanced
	 * so subsequent polls return new records.
	 */
	async poll(): Promise<PartitionBatch[]> {
		// Lazy initialization: start background fetch loop on first call
		if (!this.fetchBuffer) {
			const maxBufferedBytes = this.config.maxBufferedBytes ?? this.config.maxBytesPerPartition * 10
			this.fetchBuffer = new FetchBuffer(maxBufferedBytes)
			this.running = true
			this.abortController = new AbortController()
			// Fire and forget - loop runs until stopped via running=false or abort signal
			this.runBackgroundFetchLoop()
		}

		// Return immediately if buffer has data
		if (!this.fetchBuffer.isEmpty()) {
			return this.drainBuffer()
		}

		// Wait for data to arrive (up to maxWaitMs)
		const hasData = await this.fetchBuffer.waitForData(this.config.maxWaitMs, this.abortController?.signal)

		return hasData ? this.drainBuffer() : []
	}

	/**
	 * Drain buffer and return completed fetches directly
	 */
	private drainBuffer(): PartitionBatch[] {
		if (!this.fetchBuffer) return []
		return this.fetchBuffer.drain()
	}

	/**
	 * Decode records from raw buffer
	 * Returns DecodedRecord[] directly for better performance (avoids intermediate Message creation)
	 * Returns sync result or Promise depending on whether compression is encountered
	 */
	private decodeRecords(
		data: Buffer,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): DecodedRecord[] | Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)

		// Decode all record batches first so we can:
		// - drop control batches
		// - filter aborted transactional records for read_committed
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}> = []

		// Try synchronous decoding first (faster, no promise overhead)
		let needsAsync = false
		while (decoder.remaining() > 0) {
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = decodeRecordBatchFromSync(decoder, {
					assumeSequentialOffsets: true,
				})
				batches.push(batch)
			} catch (e) {
				// If sync failed due to compression, switch to async path
				if ((e as Error).message.includes('cannot decode compressed')) {
					needsAsync = true
					break
				}
				// Other errors mean end of valid data
				break
			}
		}

		// If compression was encountered, use async decode
		if (needsAsync) {
			return this.decodeRecordsAsync(data, abortedTransactions, lastStableOffset)
		}

		// Sync path - process batches directly
		return this.processBatches(batches, abortedTransactions, lastStableOffset)
	}

	/**
	 * Async decode path for compressed batches
	 */
	private async decodeRecordsAsync(
		data: Buffer,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): Promise<DecodedRecord[]> {
		const decoder = new Decoder(data)
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}> = []

		while (decoder.remaining() > 0) {
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = await decodeRecordBatchFrom(decoder, {
					assumeSequentialOffsets: true,
				})
				batches.push(batch)
			} catch {
				break
			}
		}

		return this.processBatches(batches, abortedTransactions, lastStableOffset)
	}

	/**
	 * Process decoded batches into records
	 */
	private processBatches(
		batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			records: DecodedRecord[]
		}>,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): DecodedRecord[] {
		// Pre-compute aborted transaction ranges (exclusive end offset)
		const abortedRanges: Array<{ producerId: bigint; start: bigint; endExclusive: bigint }> = []
		if (this.config.isolationLevel === 'read_committed' && abortedTransactions.length > 0) {
			// Control batches act as transaction markers; use the next marker offset as the end of the transaction.
			const controlBatches = batches
				.filter(b => isControlBatch(b.attributes))
				.map(b => ({ producerId: b.producerId, offset: b.baseOffset }))
				.sort((a, b) => (a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0))

			for (const tx of abortedTransactions) {
				const end =
					controlBatches.find(cb => cb.producerId === tx.producerId && cb.offset > tx.firstOffset)?.offset ??
					lastStableOffset
				abortedRanges.push({ producerId: tx.producerId, start: tx.firstOffset, endExclusive: end })
			}
		}

		const result: DecodedRecord[] = []

		// Fast path: no aborted transactional ranges to filter.
		// This is the common case (non-transactional workloads).
		if (abortedRanges.length === 0) {
			if (batches.length === 0) {
				return result
			}

			// Common case: a single non-control batch.
			// Avoid copying the records array into a new array.
			if (batches.length === 1) {
				const batch = batches[0]!
				if (isControlBatch(batch.attributes)) {
					return result
				}
				return batch.records
			}

			// Multiple batches: pre-size output to avoid repeated growth.
			let totalRecords = 0
			for (const batch of batches) {
				// Control batches contain transactional markers (commit/abort) and should never be exposed as user messages.
				if (isControlBatch(batch.attributes)) {
					continue
				}

				totalRecords += batch.records.length
			}

			if (totalRecords === 0) {
				return result
			}

			const out = new Array<DecodedRecord>(totalRecords)
			let outIndex = 0
			for (const batch of batches) {
				if (isControlBatch(batch.attributes)) {
					continue
				}

				const records = batch.records
				for (let i = 0; i < records.length; i++) {
					out[outIndex++] = records[i]!
				}
			}
			return out
		}

		for (const batch of batches) {
			// Control batches contain transactional markers (commit/abort) and should never be exposed as user messages.
			if (isControlBatch(batch.attributes)) {
				continue
			}

			const transactional = isTransactional(batch.attributes)
			const ranges = transactional ? abortedRanges.filter(r => r.producerId === batch.producerId) : []

			for (const record of batch.records) {
				// For read_committed, drop records that are part of aborted transactions.
				if (ranges.length > 0 && ranges.some(r => record.offset >= r.start && record.offset < r.endExclusive)) {
					continue
				}

				// Push DecodedRecord directly - consumer will create Message with decoded value
				result.push(record)
			}
		}

		return result
	}
}
