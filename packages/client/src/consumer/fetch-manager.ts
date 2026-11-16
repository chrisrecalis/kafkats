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
import { KafkaProtocolError, OffsetOutOfRangeError } from '@/client/errors.js'
import {
	decodeRecordBatchFrom,
	decodeRecordBatchFromSync,
	isControlBatch,
	isTransactional,
} from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import type {
	TopicPartition,
	TopicPartitionOffset,
	AutoOffsetReset,
	IsolationLevel,
	OffsetResetPolicy,
	PartitionBatch,
	ConsumerPartitionMetrics,
} from './types.js'
import type { OffsetManager } from './offset-manager.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey, formatPartitions } from '@/utils/topic-partition.js'
import { sleep } from '@/utils/sleep.js'
import { ReconnectionStrategy } from '@/utils/reconnection-strategy.js'
import type { PriorityStrategy, ScheduleDecision, SchedulerPartition, SchedulerState } from './priority.js'

/**
 * True when the bytes at the decoder's position cannot hold the batch they declare: either the
 * 12-byte preamble (baseOffset + batchLength) is cut off, or fewer than `12 + batchLength` bytes
 * remain. The broker may legally cut the trailing batch at maxBytes; only that case may be
 * skipped - decode failures on a fully-present batch are corruption.
 */
function isTruncatedTrailingBatch(decoder: Decoder): boolean {
	if (decoder.remaining() < 12) {
		return true
	}
	const batchStart = decoder.offset()
	decoder.seek(batchStart + 8) // skip baseOffset(int64)
	const batchLength = decoder.readInt32()
	decoder.seek(batchStart)
	return decoder.remaining() < 12 + batchLength
}

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

/** Estimated in-memory size of a decoded record; drives buffer accounting and the per-partition batch cap. */
function recordBytes(record: DecodedRecord): number {
	return (record.key?.length ?? 0) + (record.value?.length ?? 0) + 64
}

/** The smallest of `offset` and the defined `others`. */
function earliest(offset: bigint, ...others: Array<bigint | undefined>): bigint {
	for (const other of others) {
		if (other !== undefined && other < offset) offset = other
	}
	return offset
}

/**
 * Completed fetch from a single partition
 * Similar to Java client's CompletedFetch
 */
interface CompletedFetch {
	topic: string
	partition: number
	records: DecodedRecord[]
	// Assignment epoch of the partition when the fetch was buffered (see
	// FetchManager.assignmentEpochs). Stale batches are fenced by comparing this
	// against the current epoch at delivery time.
	assignmentEpoch: number
}

interface BufferedFetch {
	topic: string
	partition: number
	records: Array<DecodedRecord | null>
	byteSize: number
	assignmentEpoch: number
	nextRecord: number
}

/** Per-partition aggregate of the records buffered for a partition and not yet drained. */
interface BufferedPartitionStats {
	topic: string
	partition: number
	bytes: number
	records: number
	/** Offset of the earliest of those records. */
	firstOffset: bigint
}

/** Offset of the first record of a buffered fetch that is not drained yet (one is left while it is queued). */
function firstRemainingOffset(fetch: BufferedFetch): bigint {
	return fetch.records[fetch.nextRecord]!.offset
}

/** Anything with set-like `has(topic:partition)`; a Set or a Map keyed by partition works. */
type PartitionKeys = Pick<ReadonlySet<string>, 'has'>

/**
 * Buffer for completed fetches with memory bounding and wait/signal
 */
class FetchBuffer {
	private queue: BufferedFetch[] = []
	private bufferedBytes = 0
	private reservedBytes = 0
	private waiters: Array<(force?: boolean) => void> = []
	private capacityWaiter: (() => void) | null = null

	constructor(readonly maxBytes: number) {}

	get sizeBytes(): number {
		return this.bufferedBytes
	}

	/**
	 * Add a completed fetch to the buffer and signal any waiters
	 */
	add(fetch: CompletedFetch): void {
		let byteSize = 0
		for (const record of fetch.records) byteSize += recordBytes(record)
		this.queue.push({ ...fetch, byteSize, nextRecord: 0 })
		this.bufferedBytes += byteSize
		this.signalWaiters()
	}

	/**
	 * Drain one batch per partition and assignment epoch, coalescing that partition's buffered
	 * fetches up to `maxBytesPerPartition` estimated bytes. Each batch takes at least one record so
	 * an oversized record still makes progress. Planned partitions go first, in plan order and capped
	 * per entry, then the remaining partitions FIFO. A poll stops starting partitions once it has used
	 * `maxBytesPerPartition` of budget; served partitions rotate behind the others for the next poll.
	 */
	drain(
		maxBytesPerPartition: number,
		excludedPartitions?: PartitionKeys,
		plan: ScheduleDecision['drain'] = []
	): CompletedFetch[] {
		const byPartition = new Map<string, BufferedFetch[]>()
		for (const fetch of this.queue) {
			const key = tpKey(fetch.topic, fetch.partition)
			if (excludedPartitions?.has(key)) continue
			const entries = byPartition.get(key)
			if (entries) entries.push(fetch)
			else byPartition.set(key, [fetch])
		}

		const drained = new Map<string, CompletedFetch>()
		const served = new Set<string>()
		// Bytes charged against the poll budget. A partition is charged at most its own allowance, so
		// an oversized first record cannot crowd out the partitions planned after it.
		let budgetUsed = 0

		const take = (partitionKey: string, maxBytes: number): void => {
			let takenBytes = 0
			for (const fetch of byPartition.get(partitionKey)!) {
				const key = `${partitionKey}\0${fetch.assignmentEpoch}`
				let batch = drained.get(key)
				while (fetch.nextRecord < fetch.records.length) {
					const record = fetch.records[fetch.nextRecord]!
					const size = recordBytes(record)
					if (takenBytes > 0 && takenBytes + size > maxBytes) break
					if (!batch) {
						batch = {
							topic: fetch.topic,
							partition: fetch.partition,
							records: [],
							assignmentEpoch: fetch.assignmentEpoch,
						}
						drained.set(key, batch)
					}
					batch.records.push(record)
					fetch.records[fetch.nextRecord++] = null
					fetch.byteSize -= size
					this.bufferedBytes -= size
					takenBytes += size
				}
				if (fetch.nextRecord < fetch.records.length) break
			}
			served.add(partitionKey)
			budgetUsed += Math.min(takenBytes, maxBytes)
		}

		for (const entry of plan) {
			if (budgetUsed >= maxBytesPerPartition) break
			const partitionKey = tpKey(entry.topic, entry.partition)
			if (served.has(partitionKey) || !byPartition.has(partitionKey)) continue
			const maxBytes = Math.min(entry.maxBytes, maxBytesPerPartition)
			take(partitionKey, maxBytes > 0 ? maxBytes : 0)
		}
		for (const partitionKey of byPartition.keys()) {
			if (budgetUsed >= maxBytesPerPartition) break
			if (!served.has(partitionKey)) take(partitionKey, maxBytesPerPartition)
		}

		const untouched: BufferedFetch[] = []
		const rotated: BufferedFetch[] = []
		for (const fetch of this.queue) {
			if (fetch.nextRecord >= fetch.records.length) continue
			const target = served.has(tpKey(fetch.topic, fetch.partition)) ? rotated : untouched
			target.push(fetch)
		}
		this.queue = [...untouched, ...rotated]
		this.signalCapacityWaiter()
		return Array.from(drained.values())
	}

	/** Aggregate buffered records per partition, in queue (first-seen) order. */
	snapshot(): BufferedPartitionStats[] {
		const stats = new Map<string, BufferedPartitionStats>()
		for (const fetch of this.queue) {
			const key = tpKey(fetch.topic, fetch.partition)
			const records = fetch.records.length - fetch.nextRecord
			const firstOffset = firstRemainingOffset(fetch)
			const existing = stats.get(key)
			if (existing) {
				existing.bytes += fetch.byteSize
				existing.records += records
				existing.firstOffset = earliest(existing.firstOffset, firstOffset)
			} else {
				stats.set(key, {
					topic: fetch.topic,
					partition: fetch.partition,
					bytes: fetch.byteSize,
					records,
					firstOffset,
				})
			}
		}
		return Array.from(stats.values())
	}

	/** Offset of the earliest buffered record of a partition, if any is buffered. */
	firstOffset(topic: string, partition: number): bigint | undefined {
		let first: bigint | undefined
		for (const fetch of this.queue) {
			if (fetch.topic !== topic || fetch.partition !== partition) continue
			const offset = firstRemainingOffset(fetch)
			if (first === undefined || offset < first) first = offset
		}
		return first
	}

	hasDataFor(excludedPartitions?: PartitionKeys): boolean {
		return this.queue.some(fetch => !excludedPartitions?.has(tpKey(fetch.topic, fetch.partition)))
	}

	isFull(): boolean {
		return this.remainingCapacity() === 0
	}

	remainingCapacity(): number {
		return Math.max(0, this.maxBytes - this.bufferedBytes - this.reservedBytes)
	}

	/** Reserve part of the memory budget for a Fetch response before dispatching it. */
	reserveCapacity(maxBytes: number): number {
		const reserved = Math.min(maxBytes, this.remainingCapacity())
		this.reservedBytes += reserved
		return reserved
	}

	releaseCapacity(bytes: number): void {
		this.reservedBytes = Math.max(0, this.reservedBytes - bytes)
		this.signalCapacityWaiter()
	}

	/** Wait until poll() or partition removal frees buffer capacity. */
	async waitForCapacity(signal?: AbortSignal): Promise<void> {
		if (!this.isFull() || signal?.aborted) {
			return
		}

		await new Promise<void>(resolve => {
			const resume = () => {
				this.capacityWaiter = null
				signal?.removeEventListener('abort', resume)
				resolve()
			}

			this.capacityWaiter = resume
			signal?.addEventListener('abort', resume, { once: true })
		})
	}

	/**
	 * Wait for data to be available in the buffer
	 * @returns true if data is available, false if timeout/aborted
	 */
	async waitForData(timeoutMs: number, signal?: AbortSignal, excludedPartitions?: PartitionKeys): Promise<boolean> {
		if (this.hasDataFor(excludedPartitions)) {
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

			const waiterCallback = (force = false) => {
				if (!force && !this.hasDataFor(excludedPartitions)) return
				cleanup()
				resolve(this.hasDataFor(excludedPartitions))
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
	wakeWaiters(): void {
		this.signalWaiters(true)
	}

	private signalWaiters(force = false): void {
		const waiters = [...this.waiters]
		for (const waiter of waiters) {
			waiter(force)
		}
	}

	private signalCapacityWaiter(): void {
		if (!this.isFull()) this.capacityWaiter?.()
	}

	/**
	 * Clear all buffered data
	 */
	clear(): void {
		this.queue = []
		this.bufferedBytes = 0
		this.signalCapacityWaiter()
	}

	/**
	 * Remove buffered data for partitions not in the given set
	 */
	removePartitionsNotIn(validKeys: Set<string>): void {
		let newBytes = 0
		const filtered: BufferedFetch[] = []
		for (const f of this.queue) {
			if (validKeys.has(tpKey(f.topic, f.partition))) {
				filtered.push(f)
				newBytes += f.byteSize
			}
		}
		this.queue = filtered
		this.bufferedBytes = newBytes
		this.signalCapacityWaiter()
	}

	/**
	 * Remove buffered data for specific partitions
	 */
	removePartitions(partitions: Array<{ topic: string; partition: number }>): void {
		if (partitions.length === 0) return
		const keysToRemove = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
		let newBytes = 0
		const filtered: BufferedFetch[] = []
		for (const f of this.queue) {
			if (!keysToRemove.has(tpKey(f.topic, f.partition))) {
				filtered.push(f)
				newBytes += f.byteSize
			}
		}
		this.queue = filtered
		this.bufferedBytes = newBytes
		this.signalCapacityWaiter()
	}

	/**
	 * Remove buffered data for specific partitions, returning the earliest buffered record offset
	 * per partition that had buffered data. Used by pause(): the buffered records were never
	 * delivered but the fetch offset already advanced past them, so the caller rewinds to the
	 * earliest offset to re-fetch (and re-deliver) them on resume instead of dropping them.
	 */
	removePartitionsReturningEarliest(partitions: Array<{ topic: string; partition: number }>): Map<string, bigint> {
		const earliest = new Map<string, bigint>()
		if (partitions.length === 0) return earliest
		const keysToRemove = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
		let newBytes = 0
		const filtered: BufferedFetch[] = []
		for (const f of this.queue) {
			const key = tpKey(f.topic, f.partition)
			if (!keysToRemove.has(key)) {
				filtered.push(f)
				newBytes += f.byteSize
				continue
			}
			for (let index = f.nextRecord; index < f.records.length; index++) {
				const record = f.records[index]!
				const current = earliest.get(key)
				if (current === undefined || record.offset < current) {
					earliest.set(key, record.offset)
				}
			}
		}
		this.queue = filtered
		this.bufferedBytes = newBytes
		this.signalCapacityWaiter()
		return earliest
	}
}

/**
 * Per-partition fetch state
 */
interface PartitionState {
	topic: string
	partition: number
	offset: bigint
	/** Paused by pausePartitions (or an out-of-range position under policy 'none'); cleared only by resume. */
	paused: boolean
	/** Added held: not fetched until releaseHeld, independently of `paused`. */
	held: boolean
	/** How far the partition could be read as of the last fetch response. */
	endOffset?: bigint
	/** Whether records were fetched for it since it was assigned or last sought. */
	fetched?: boolean
	abortController: AbortController // Per-partition abort signal for incremental control
	cachedLeader: Broker | null // Cached leader broker - invalidated on NotLeaderOrFollower
	// Bumped by seek(). A fetch captures the epoch at issue time; a response whose
	// partition epoch changed is dropped, so a seek that lands mid-flight is not
	// clobbered by the stale response advancing the offset.
	fetchEpoch: number
}

function newPartitionState(tp: TopicPartition & { offset: bigint }, held: boolean): PartitionState {
	return {
		topic: tp.topic,
		partition: tp.partition,
		offset: tp.offset,
		paused: false,
		held,
		abortController: new AbortController(),
		cachedLeader: null,
		fetchEpoch: 0,
	}
}

/** Whether the fetch loop may fetch the partition: neither paused nor held. */
function isFetchable(state: PartitionState): boolean {
	return !state.paused && !state.held
}

/**
 * Fetch manager configuration
 */
export interface FetchManagerConfig {
	/** Max bytes per partition in one Fetch request, and per delivered partition batch (estimated). */
	maxBytesPerPartition: number
	minBytes: number
	maxWaitMs: number
	partitionConcurrency: number
	isolationLevel: IsolationLevel
	/** Verify the CRC32C of each fetched batch (default true). See ConsumerConfig.checkCrcs. */
	checkCrcs?: boolean
	/**
	 * Target maximum bytes to buffer for poll() mode. Kafka may return one oversized first
	 * record batch per in-flight broker request. Default: maxBytesPerPartition * 10.
	 */
	maxBufferedBytes?: number
	/** Called whenever a decoded batch range advances the fetch position. */
	onFetchPosition?: (position: TopicPartitionOffset, recordCount: number) => void
	/**
	 * Called with failures of the background fetch loop (broker down, leader lookup failed, ...).
	 * Throttled to one call per {@link FETCH_ERROR_REPORT_INTERVAL_MS}; the loop keeps retrying
	 * with backoff regardless.
	 */
	onError?: (error: Error) => void
	/**
	 * Called when the fetch position of a partition is out of range and its reset policy is 'none'. The
	 * partition is paused first, as by pausePartitions; seek it before resuming it. Without this callback the
	 * next poll() throws OffsetOutOfRangeError instead.
	 */
	onOffsetOutOfRange?: (position: TopicPartitionOffset) => void
	/** Called after an out-of-range fetch position was reset by the partition's 'earliest' or 'latest' policy. */
	onOffsetReset?: (reset: {
		topic: string
		partition: number
		from: bigint
		to: bigint
		reason: 'out-of-range'
	}) => void
	/**
	 * The batches poll() returned that the caller still holds without having delivered them (a batch is
	 * delivered once its handler is invoked). A pause rewinds the fetch position to the earliest of them and
	 * voids them (isBatchAssigned turns false, and the caller drops them); nextOffset, currentLag and
	 * partitionMetrics count them as not delivered. Without it, a pause leaves such batches deliverable and
	 * nextOffset can be past their records.
	 */
	undelivered?: () => Iterable<PartitionBatch>
	priority?: PriorityStrategy
}

/** Minimum spacing between reported background fetch failures (log line + onError). */
export const FETCH_ERROR_REPORT_INTERVAL_MS = 5000
/** Consecutive failed fetches against one broker before its cached leadership is re-resolved. */
const FETCH_FAILURES_BEFORE_LEADER_REFRESH = 3

function newFetchBackoff(): ReconnectionStrategy {
	return new ReconnectionStrategy({ initialDelayMs: 100, maxDelayMs: 5000, jitter: 0.2 })
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
	checkCrcs: true,
}

/**
 * Result of decoding a fetch response's record data.
 *
 * `records` are the user-visible records (control batches dropped, aborted-transaction records
 * filtered for read_committed). `nextOffset` is the offset immediately after the last decoded
 * batch (max over batches of baseOffset + lastOffsetDelta + 1), or null when no batch decoded.
 * The caller advances the fetch position to `nextOffset` even when `records` is empty — a batch
 * that is entirely control records, fully aborted, or empty/compacted yields zero records but
 * must still advance the position, otherwise the same offset is re-fetched forever.
 */
interface DecodedBatchResult {
	records: DecodedRecord[]
	nextOffset: bigint | null
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
	// Per-partition assignment epoch, bumped whenever a partition is removed or sought, and when a pause
	// voids batches drained before it. Survives remove/re-add cycles (eager rebalance), so a batch drained
	// before the rebalance can be told apart from one fetched under the new assignment of the same partition.
	private readonly assignmentEpochs: Map<string, number> = new Map()
	private readonly logger: Logger
	private readonly autoOffsetReset: OffsetResetPolicy
	private readonly offsetManager: OffsetManager
	private abortController: AbortController | null = null
	private running = false

	// Buffered poll mode fields
	private fetchBuffer: FetchBuffer | null = null
	private brokerInFlight: Map<number, boolean> = new Map()
	// Brokers this fetch manager has fetched from. It fetches on connections of its own (see
	// Broker.fetch), which stop() closes.
	private readonly fetchBrokers = new Set<Broker>()
	// Per-broker retry gate after failed fetches (exponential backoff), so a down broker is polled
	// at a bounded rate instead of in a hot loop. Cleared by the first successful fetch.
	private readonly brokerBackoff: Map<number, { strategy: ReconnectionStrategy; until: number; failures: number }> =
		new Map()
	// Backoff for failed leader lookups (no broker reachable to resolve/connect the leader).
	private readonly lookupBackoff = newFetchBackoff()
	private lookupRetryAt = 0
	private lastErrorReportAt = 0
	private suppressedErrors = 0
	// A fatal, non-retriable fetch error (e.g. CORRUPT_MESSAGE) surfaced to the next poll().
	// The background fetch loop swallows rejected fetches (logs only), so fatal per-partition
	// errors are stashed here and re-thrown from poll() rather than silently dropping data.
	private pendingError: Error | null = null
	// Partitions currently held by handlers, as last passed to poll(). The consumer passes its live
	// `active` map, so the background fetch loop sees current busy state when it schedules fetches.
	private lastExcluded?: PartitionKeys

	/**
	 * @param autoOffsetReset - Where a partition whose fetch position is out of range restarts: a policy, or a
	 * function giving each partition's policy. 'none' pauses the partition (see `onOffsetOutOfRange`).
	 */
	constructor(
		cluster: Cluster,
		offsetManager: OffsetManager,
		autoOffsetReset: OffsetResetPolicy,
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
		for (const [key, state] of this.partitionStates) {
			state.abortController.abort()
			this.bumpAssignmentEpoch(key)
		}
		this.partitionStates.clear()

		// Clear buffer for removed partitions (keep only new partitions)
		if (this.fetchBuffer) {
			const newKeys = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
			this.fetchBuffer.removePartitionsNotIn(newKeys)
		}

		for (const tp of partitions) {
			this.partitionStates.set(tpKey(tp.topic, tp.partition), newPartitionState(tp, false))
		}
	}

	/**
	 * Add partitions incrementally (used for cooperative rebalance). Partitions that are already assigned
	 * keep their state.
	 *
	 * @param options.held - Add them held: they are not fetched until {@link releaseHeld}, so a caller can
	 * pause or seek them before their first fetch.
	 */
	addPartitions(partitions: Array<TopicPartition & { offset: bigint }>, options: { held?: boolean } = {}): void {
		if (partitions.length === 0) return

		this.logger.debug('adding partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
			held: options.held === true,
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			// Skip if already exists
			if (this.partitionStates.has(key)) {
				this.logger.debug('partition already exists, skipping', { topic: tp.topic, partition: tp.partition })
				continue
			}
			this.partitionStates.set(key, newPartitionState(tp, options.held === true))
		}
	}

	/** Let held partitions be fetched. Those also paused stay paused until resumed. */
	releaseHeld(partitions: TopicPartition[]): void {
		for (const tp of partitions) {
			const state = this.partitionStates.get(tpKey(tp.topic, tp.partition))
			if (state) state.held = false
		}
	}

	/**
	 * Remove partitions incrementally (used for cooperative rebalance)
	 * Aborts fetch loops for removed partitions and clears buffered records
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
				// Fence batches already drained by poll(): if the partition is re-added
				// (eager rebalance keeps some partitions), those stale batches must not be
				// delivered — their records are re-fetched from the committed offset.
				this.bumpAssignmentEpoch(key)
			}
		}

		// Clear buffered records for removed partitions
		this.fetchBuffer?.removePartitions(partitions)
	}

	/**
	 * Pause partitions: stop fetching them, and withhold every record of them not delivered yet. Their buffered
	 * records and the caller's undelivered batches of them (see `undelivered`) are dropped, those batches are
	 * voided, and the fetch position rewinds to the earliest dropped record, so resuming delivers it again.
	 */
	pausePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('pausing partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		// The fetch position is already past these records, so rewinding to the earliest of them is what
		// keeps them from being lost (never delivered, never re-fetched).
		const buffered = this.fetchBuffer?.removePartitionsReturningEarliest(partitions) ?? new Map<string, bigint>()
		const undelivered = this.undeliveredFrom()

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (!state) continue
			state.paused = true
			// Not fetched while paused: what is known of the partition's end goes stale.
			state.endOffset = undefined
			// The caller's batches of it must not be delivered either: their records are fetched again.
			if (undelivered.has(key)) this.bumpAssignmentEpoch(key)
			const rewindTo = earliest(state.offset, buffered.get(key), undelivered.get(key))
			if (rewindTo < state.offset) {
				state.offset = rewindTo
				// Fence any in-flight fetch issued at the old (higher) position. The pause flag
				// alone is cleared on resume, so without bumping the epoch a stale fetch that
				// resolves after resume would buffer at the old offset and advance past the
				// rewound records, losing them. The very bug this rewind prevents.
				state.fetchEpoch++
			}
		}
	}

	/**
	 * Resume paused partitions. Only the pause is lifted: a held partition stays held until releaseHeld.
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

	/** Whether records of an assigned partition were fetched since it was assigned or last sought. */
	hasFetched(topic: string, partition: number): boolean {
		return this.partitionStates.get(tpKey(topic, partition))?.fetched === true
	}

	/** The offset the next fetch of an assigned partition starts at, if the partition is assigned. */
	getFetchPosition(topic: string, partition: number): bigint | undefined {
		return this.partitionStates.get(tpKey(topic, partition))?.offset
	}

	/**
	 * The offset of the next record of an assigned partition to be delivered: the earliest record fetched but
	 * not delivered yet (buffered, or in one of the caller's undelivered batches), else the fetch position,
	 * which moves past control records and aborted transactions. Undefined if the partition is not assigned.
	 */
	nextOffset(topic: string, partition: number): bigint | undefined {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (!state) return undefined
		return earliest(state.offset, this.fetchBuffer?.firstOffset(topic, partition), this.undeliveredFrom().get(key))
	}

	/**
	 * How many records of an assigned partition are readable but not delivered yet: `endOffset - nextOffset`,
	 * at least 0, with `endOffset` (the last stable offset under read_committed, else the high watermark) from
	 * the partition's last fetch response. Undefined before that response, and while the partition is paused
	 * or held (its end is not followed then).
	 */
	currentLag(topic: string, partition: number): bigint | undefined {
		const state = this.partitionStates.get(tpKey(topic, partition))
		if (state?.endOffset === undefined || !isFetchable(state)) return undefined
		const lag = state.endOffset - this.nextOffset(topic, partition)!
		return lag > 0n ? lag : 0n
	}

	/**
	 * Seek to a specific offset for a partition
	 *
	 * Updates the fetch position for the partition to the given offset.
	 * The next fetch request will request records starting from this offset. Records of the partition not
	 * delivered yet are dropped: its buffered records, and the caller's batches of it, which are voided.
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
			// Fence any in-flight fetch issued at the old position, and drop buffered records and
			// drained batches from before the seek, so none of them clobbers the new position.
			state.fetched = false
			state.fetchEpoch++
			this.bumpAssignmentEpoch(key)
			this.fetchBuffer?.removePartitions([{ topic, partition }])
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
	 * Check if a partition is currently assigned to this consumer
	 * Used to filter stale records after rebalance
	 */
	isPartitionAssigned(topic: string, partition: number): boolean {
		return this.partitionStates.has(tpKey(topic, partition))
	}

	private bumpAssignmentEpoch(key: string): void {
		this.assignmentEpochs.set(key, (this.assignmentEpochs.get(key) ?? 0) + 1)
	}

	/**
	 * Current assignment epoch for a partition (0 if it was never removed, sought or paused over drained batches)
	 */
	getAssignmentEpoch(topic: string, partition: number): number {
		return this.assignmentEpochs.get(tpKey(topic, partition)) ?? 0
	}

	/**
	 * Check whether a drained batch is still deliverable: its partition must be assigned
	 * AND its assignment epoch must match the current one. A mismatch means the batch was
	 * voided after it was buffered: the partition was removed (and possibly re-added by an
	 * eager rebalance), sought, or paused, and its records are fetched again from the
	 * partition's new position; delivering the batch would skip or duplicate them.
	 */
	isBatchAssigned(batch: PartitionBatch): boolean {
		if (!this.partitionStates.has(tpKey(batch.topic, batch.partition))) {
			return false
		}
		return (
			batch.assignmentEpoch === undefined ||
			batch.assignmentEpoch === this.getAssignmentEpoch(batch.topic, batch.partition)
		)
	}

	/** First offset of the earliest of the caller's undelivered batches, per partition, for deliverable batches. */
	private undeliveredFrom(): Map<string, bigint> {
		const firstOffsets = new Map<string, bigint>()
		for (const batch of this.config.undelivered?.() ?? []) {
			const first = batch.records[0]?.offset
			if (first === undefined || !this.isBatchAssigned(batch)) continue
			const key = tpKey(batch.topic, batch.partition)
			firstOffsets.set(key, earliest(first, firstOffsets.get(key)))
		}
		return firstOffsets
	}

	/**
	 * Handle fetch error for a partition
	 */
	private async handleFetchError(state: PartitionState, errorCode: ErrorCode): Promise<void> {
		switch (errorCode) {
			case ErrorCode.OffsetOutOfRange:
				await this.handleOffsetOutOfRange(state)
				break

			case ErrorCode.NotLeaderOrFollower:
			case ErrorCode.UnknownTopicOrPartition:
				state.cachedLeader = null // Invalidate cached leader
				await this.cluster.refreshMetadata([state.topic])
				break

			case ErrorCode.CorruptMessage:
				// Re-fetching the same offset yields the same corrupt record, so this is not
				// retriable. Surface it to the consumer (via the next poll()) instead of logging
				// and dropping the partition's data; the position is left unchanged so the user can
				// seek past the bad offset to recover.
				this.pendingError = new KafkaProtocolError(
					ErrorCode.CorruptMessage,
					`${state.topic}-${state.partition} at offset ${state.offset}`
				)
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
	 * The fetch position of a partition is out of range (retention deleted it, or the log was truncated below
	 * it). The partition's reset policy decides: 'earliest' and 'latest' move the position there and report the
	 * reset; 'none' pauses the partition and reports it, so the application decides where it goes on.
	 */
	private async handleOffsetOutOfRange(state: PartitionState): Promise<void> {
		const { topic, partition, offset } = state
		let policy: AutoOffsetReset
		try {
			policy =
				typeof this.autoOffsetReset === 'function'
					? this.autoOffsetReset(topic, partition)
					: this.autoOffsetReset
		} catch (error) {
			// A policy function that throws is an application error: the next poll() ends the run with it.
			this.pendingError = error instanceof Error ? error : new Error(String(error))
			return
		}
		this.logger.warn('fetch offset out of range', { topic, partition, offset: String(offset), policy })

		if (policy === 'none') {
			this.pausePartitions([{ topic, partition }])
			if (this.config.onOffsetOutOfRange) this.config.onOffsetOutOfRange({ topic, partition, offset })
			else this.pendingError = new OffsetOutOfRangeError(topic, partition, offset)
			return
		}

		// Records buffered from the now-invalid position precede (or are unrelated to) the reset offset;
		// delivering them would violate the reset. Discard them BEFORE the listOffsets round-trip below,
		// otherwise a concurrent poll() could drain them while we await, or a transient lookup failure could
		// skip the clear entirely.
		this.fetchBuffer?.removePartitions([{ topic, partition }])

		// A seek that lands while the reset offset is resolved takes precedence over the reset, and a
		// partition revoked meanwhile is not reset at all.
		const epochBeforeReset = state.fetchEpoch
		const resetOffset =
			policy === 'earliest'
				? await this.offsetManager.getEarliestOffset(topic, partition)
				: await this.offsetManager.getLatestOffset(topic, partition)
		if (state.fetchEpoch !== epochBeforeReset || state.abortController.signal.aborted) return
		state.offset = resetOffset
		this.config.onOffsetReset?.({ topic, partition, from: offset, to: resetOffset, reason: 'out-of-range' })
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
		// Closing the connections also ends a long poll in flight; its result would be discarded anyway.
		for (const broker of this.fetchBrokers) {
			broker.closeFetchConnection(this).catch(() => {})
		}
		this.fetchBrokers.clear()
	}

	/** Log and surface a background failure, at most once per FETCH_ERROR_REPORT_INTERVAL_MS. */
	private reportFailure(message: string, error: Error): void {
		const now = Date.now()
		if (now - this.lastErrorReportAt < FETCH_ERROR_REPORT_INTERVAL_MS) {
			this.suppressedErrors++
			return
		}
		this.lastErrorReportAt = now
		this.logger.error(message, { error: error.message, suppressedSinceLastReport: this.suppressedErrors })
		this.suppressedErrors = 0
		this.config.onError?.(error)
	}

	/**
	 * A fetch against `brokerId` failed (typically a dropped connection). Back that broker off
	 * exponentially. The cached Broker reconnects by itself, so the leader is only re-resolved
	 * (fresh metadata) after repeated failures, in case leadership moved to another node.
	 */
	private handleBrokerFetchFailure(brokerId: number, partitions: PartitionState[], error: Error): void {
		const entry = this.brokerBackoff.get(brokerId) ?? { strategy: newFetchBackoff(), until: 0, failures: 0 }
		entry.until = Date.now() + entry.strategy.nextDelay()
		entry.strategy.recordFailure()
		entry.failures++
		this.brokerBackoff.set(brokerId, entry)

		this.reportFailure('background fetch failed', error)

		if (entry.failures >= FETCH_FAILURES_BEFORE_LEADER_REFRESH) {
			const topics = new Set<string>()
			for (const state of partitions) {
				if (state.cachedLeader?.nodeId === brokerId) state.cachedLeader = null
				topics.add(state.topic)
			}
			this.cluster.refreshMetadata([...topics]).catch(() => {})
		}
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
				const fetchBuffer = this.fetchBuffer
				if (!fetchBuffer) break

				// Do not issue another Fetch once buffered data plus in-flight response
				// reservations have reached the memory budget.
				if (fetchBuffer.isFull()) {
					await fetchBuffer.waitForCapacity(signal)
					continue
				}

				// Get ready (neither paused nor held) partitions - reuse array
				readyPartitions.length = 0
				for (const state of this.partitionStates.values()) {
					if (isFetchable(state)) {
						readyPartitions.push(state)
					}
				}

				if (this.config.priority && readyPartitions.length > 0) {
					const decision = this.config.priority.schedule(
						this.buildSchedulerState(fetchBuffer, this.lastExcluded)
					)
					const allowed = new Set(decision.fetch.map(tp => tpKey(tp.topic, tp.partition)))
					let writeIndex = 0
					for (const state of readyPartitions) {
						if (allowed.has(tpKey(state.topic, state.partition))) readyPartitions[writeIndex++] = state
					}
					readyPartitions.length = writeIndex
					if (readyPartitions.length === 0) {
						// Nothing fetchable until the buffer drains or a handler frees a partition.
						await sleep(10, { signal }).catch(() => {})
						continue
					}
				}

				if (readyPartitions.length === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Group partitions by leader broker
				const partitionsByBroker = await this.groupPartitionsByBroker(readyPartitions)

				// stop() may have run during the await above; don't issue a final fetch
				// against the stale buffer captured before it.
				if (!this.running || signal?.aborted) break

				if (partitionsByBroker.size === 0) {
					// Leader lookup failed (or is backing off) for every ready partition: wait for the retry slot.
					await sleep(Math.max(1, Math.min(this.lookupRetryAt - Date.now(), 100)), { signal }).catch(() => {})
					continue
				}
				// Issue fetch to each broker that doesn't have an in-flight request. Split
				// remaining capacity across those brokers so concurrent responses cannot each
				// independently claim the entire buffer budget.
				let fetchesIssued = 0
				const now = Date.now()
				let backoffRemainingMs = Infinity
				const availableBrokers = Array.from(partitionsByBroker.entries()).filter(([brokerId]) => {
					if (this.brokerInFlight.get(brokerId)) return false
					const until = this.brokerBackoff.get(brokerId)?.until ?? 0
					if (until > now) {
						backoffRemainingMs = Math.min(backoffRemainingMs, until - now)
						return false
					}
					return true
				})
				for (let index = 0; index < availableBrokers.length; index++) {
					const [brokerId, { broker, partitions }] = availableBrokers[index]!
					const remainingBrokers = availableBrokers.length - index
					const fairShare = Math.floor(fetchBuffer.remainingCapacity() / remainingBrokers)
					const naturalMaxBytes = this.config.maxBytesPerPartition * partitions.length
					const reservedBytes = fetchBuffer.reserveCapacity(Math.min(naturalMaxBytes, fairShare))
					if (reservedBytes === 0) break

					this.brokerInFlight.set(brokerId, true)
					fetchesIssued++

					// Fire and forget - don't await, let it run in background
					this.fetchFromBrokerToBuffer(broker, partitions, reservedBytes)
						.then(() => {
							this.brokerBackoff.delete(brokerId)
						})
						.catch(error => {
							// stop() closed the connection under it.
							if (signal?.aborted) return
							this.handleBrokerFetchFailure(
								brokerId,
								partitions,
								error instanceof Error ? error : new Error(String(error))
							)
						})
						.finally(() => {
							fetchBuffer.releaseCapacity(reservedBytes)
							this.brokerInFlight.set(brokerId, false)
						})
				}

				// Only sleep when necessary to prevent busy-loop. Capacity backpressure
				// is handled at the top of the loop before any request is issued.
				if (fetchesIssued === 0) {
					// All brokers have in-flight requests (or are backing off after failures), wait briefly
					await sleep(Math.min(backoffRemainingMs, backoffRemainingMs === Infinity ? 1 : 10), {
						signal,
					}).catch(() => {})
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
		if (needsLookup.length > 0 && Date.now() >= this.lookupRetryAt) {
			const lookups = await Promise.all(
				needsLookup.map(async state => {
					try {
						const leader = await this.cluster.getLeaderForPartition(state.topic, state.partition)
						state.cachedLeader = leader
						return { state, leader }
					} catch (error) {
						this.reportFailure(
							'failed to get leader',
							error instanceof Error ? error : new Error(String(error))
						)
						return null
					}
				})
			)

			if (lookups.some(result => result === null)) {
				this.lookupRetryAt = Date.now() + this.lookupBackoff.nextDelay()
				this.lookupBackoff.recordFailure()
			} else {
				this.lookupBackoff.reset()
				this.lookupRetryAt = 0
			}

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
	private async fetchFromBrokerToBuffer(
		broker: Broker,
		partitions: PartitionState[],
		maxBytes: number
	): Promise<void> {
		if (!this.fetchBuffer) return

		// Map response entries back to the in-flight state we issued the fetch
		// against. Used to fence stale responses via state.abortController and fetchEpoch.
		const inflightStateByKey = new Map<string, PartitionState>()
		// Epoch captured at issue time; a seek during the fetch bumps state.fetchEpoch,
		// so the stale response is dropped instead of clobbering the new position.
		const issuedEpochByKey = new Map<string, number>()
		const topicsMap = new Map<string, Array<{ state: PartitionState }>>()
		for (const state of partitions) {
			if (!topicsMap.has(state.topic)) {
				topicsMap.set(state.topic, [])
			}
			topicsMap.get(state.topic)!.push({ state })
			const key = tpKey(state.topic, state.partition)
			inflightStateByKey.set(key, state)
			issuedEpochByKey.set(key, state.fetchEpoch)
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

		this.fetchBrokers.add(broker)
		const response = await broker.fetch(
			{
				replicaId: -1,
				maxWaitMs: this.config.maxWaitMs,
				minBytes: this.config.minBytes,
				maxBytes,
				isolationLevel: this.config.isolationLevel === 'read_committed' ? 1 : 0,
				topics,
			},
			this
		)

		for (const topicResponse of response.topics) {
			for (const partitionResponse of topicResponse.partitions) {
				const key = tpKey(topicResponse.topic, partitionResponse.partitionIndex)
				const state = inflightStateByKey.get(key)
				// Fence: state was revoked, the partition was paused, or a seek changed the fetch
				// position while the fetch was in flight. Drop the records without advancing the
				// offset so they are re-fetched (on resume / from the post-seek position) instead
				// of being clobbered.
				if (
					!state ||
					state.abortController.signal.aborted ||
					!isFetchable(state) ||
					state.fetchEpoch !== issuedEpochByKey.get(key)
				)
					continue

				if (partitionResponse.errorCode !== ErrorCode.None) {
					await this.handleFetchError(state, partitionResponse.errorCode)
					continue
				}
				// What this consumer can read up to: under read_committed, not past an open transaction.
				state.endOffset =
					this.config.isolationLevel === 'read_committed'
						? partitionResponse.lastStableOffset
						: partitionResponse.highWatermark

				if (partitionResponse.recordsData && partitionResponse.recordsData.length > 0) {
					const fetchOffset = state.offset
					const decodeResult = this.decodeRecords(
						partitionResponse.recordsData,
						partitionResponse.abortedTransactions,
						partitionResponse.lastStableOffset
					)
					const { records: decoded, nextOffset } =
						decodeResult instanceof Promise ? await decodeResult : decodeResult

					// decodeRecords may have awaited; re-fence (revoked, paused, or seeked mid-decode).
					if (
						state.abortController.signal.aborted ||
						!isFetchable(state) ||
						state.fetchEpoch !== issuedEpochByKey.get(key)
					)
						continue

					const records = filterRecordsFromOffset(decoded, fetchOffset)

					if (records.length > 0) {
						state.fetched = true
						this.fetchBuffer.add({
							topic: state.topic,
							partition: state.partition,
							records,
							assignmentEpoch: this.getAssignmentEpoch(state.topic, state.partition),
						})
					}

					// Advance the fetch position past the whole decoded batch range, even when no
					// records survived (all aborted / control-only / empty-compacted) — otherwise the
					// position never moves and the same offset is re-fetched forever. nextOffset is the
					// batch-declared end (>= last record offset + 1), which also steps over a compacted
					// tail. Guarded by `> state.offset` so a stale/overlapping batch can't rewind.
					const advanceTo =
						nextOffset ?? (records.length > 0 ? records[records.length - 1]!.offset + 1n : null)
					if (advanceTo !== null && advanceTo > state.offset) {
						state.offset = advanceTo
						this.config.onFetchPosition?.(
							{ topic: state.topic, partition: state.partition, offset: advanceTo },
							records.length
						)
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
	async poll(excludedPartitions?: PartitionKeys): Promise<PartitionBatch[]> {
		// Surface a fatal fetch error (e.g. CORRUPT_MESSAGE) recorded by the background loop.
		// Cleared once thrown; if the condition persists the next fetch re-records it.
		this.throwPendingError()

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
		if (this.fetchBuffer.hasDataFor(excludedPartitions)) {
			return this.drainBuffer(excludedPartitions)
		}

		// Wait for data to arrive (up to maxWaitMs)
		const hasData = await this.fetchBuffer.waitForData(
			this.config.maxWaitMs,
			this.abortController?.signal,
			excludedPartitions
		)

		// A fatal error may have been recorded by the background loop while we waited (it adds no
		// data, so the waiter isn't signalled). Surface it now rather than returning an empty poll.
		this.throwPendingError()

		return hasData ? this.drainBuffer(excludedPartitions) : []
	}

	/** Wake a poll waiting for an idle partition after handler state changes. */
	wakePoll(): void {
		this.fetchBuffer?.wakeWaiters()
	}

	/** Throw and clear any fatal fetch error recorded by the background loop. No-op otherwise. */
	private throwPendingError(): void {
		if (!this.pendingError) return
		const error = this.pendingError
		this.pendingError = null
		throw error
	}

	/** Position, next delivery offset, readable end, buffered records and pause state of every assigned partition. */
	partitionMetrics(): ConsumerPartitionMetrics[] {
		const buffered = new Map(this.fetchBuffer?.snapshot().map(stat => [tpKey(stat.topic, stat.partition), stat]))
		const undelivered = this.undeliveredFrom()
		return Array.from(this.partitionStates, ([key, state]) => {
			const stat = buffered.get(key)
			return {
				topic: state.topic,
				partition: state.partition,
				position: state.offset,
				nextOffset: earliest(state.offset, stat?.firstOffset, undelivered.get(key)),
				endOffset: state.endOffset,
				bufferedBytes: stat?.bytes ?? 0,
				bufferedRecords: stat?.records ?? 0,
				paused: state.paused,
			}
		})
	}

	/**
	 * Drain buffer and return completed fetches directly
	 */
	private drainBuffer(excludedPartitions?: PartitionKeys): PartitionBatch[] {
		const fetchBuffer = this.fetchBuffer
		if (!fetchBuffer) return []
		const { maxBytesPerPartition, priority } = this.config
		if (!priority) return fetchBuffer.drain(maxBytesPerPartition, excludedPartitions)
		this.lastExcluded = excludedPartitions
		const decision = priority.schedule(this.buildSchedulerState(fetchBuffer, excludedPartitions))
		return fetchBuffer.drain(maxBytesPerPartition, excludedPartitions, decision.drain)
	}

	/** Buffered partitions first (in buffer order), then idle assigned partitions with zero buffered. */
	private buildSchedulerState(fetchBuffer: FetchBuffer, excludedPartitions?: PartitionKeys): SchedulerState {
		const buffered = new Map(fetchBuffer.snapshot().map(stat => [tpKey(stat.topic, stat.partition), stat]))
		const partitions: SchedulerPartition[] = []
		const describe = (state: PartitionState, key: string): SchedulerPartition => {
			const stat = buffered.get(key)
			return {
				topic: state.topic,
				partition: state.partition,
				bufferedBytes: stat?.bytes ?? 0,
				busy: excludedPartitions?.has(key) ?? false,
			}
		}
		for (const key of buffered.keys()) {
			const state = this.partitionStates.get(key)
			if (state && isFetchable(state)) partitions.push(describe(state, key))
		}
		for (const [key, state] of this.partitionStates) {
			if (isFetchable(state) && !buffered.has(key)) partitions.push(describe(state, key))
		}
		return {
			partitions,
			byteBudget: this.config.maxBytesPerPartition,
			bufferCapacityBytes: fetchBuffer.maxBytes,
			bufferedBytes: fetchBuffer.sizeBytes,
		}
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
	): DecodedBatchResult | Promise<DecodedBatchResult> {
		const decoder = new Decoder(data)

		// Decode all record batches first so we can:
		// - drop control batches
		// - filter aborted transactional records for read_committed
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			lastOffsetDelta: number
			records: DecodedRecord[]
		}> = []

		// Try synchronous decoding first (faster, no promise overhead)
		let needsAsync = false
		while (decoder.remaining() > 0) {
			// A maxBytes-truncated trailing batch is legal and detected structurally (framing
			// underflow). The broker always returns the FIRST batch complete, so underflow with
			// nothing decoded is corruption; and any decode failure on a fully-present batch
			// (e.g. CRC mismatch) is corruption too - treating either as truncation would
			// silently drop records and leave the partition re-fetching the same data forever.
			if (batches.length > 0 && isTruncatedTrailingBatch(decoder)) {
				break
			}
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = decodeRecordBatchFromSync(decoder, {
					assumeSequentialOffsets: true,
					verifyCrc: this.config.checkCrcs !== false,
				})
				batches.push(batch)
			} catch (e) {
				// If sync failed due to compression, switch to async path
				if ((e as Error).message.includes('cannot decode compressed')) {
					needsAsync = true
					break
				}
				throw new Error(`Failed to decode record batch (corrupt data): ${(e as Error).message}`)
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
	): Promise<DecodedBatchResult> {
		const decoder = new Decoder(data)
		const batches: Array<{
			attributes: number
			producerId: bigint
			baseOffset: bigint
			lastOffsetDelta: number
			records: DecodedRecord[]
		}> = []

		while (decoder.remaining() > 0) {
			// A maxBytes-truncated trailing batch is legal and detected structurally (framing
			// underflow). The broker always returns the FIRST batch complete, so underflow with
			// nothing decoded is corruption; and any decode failure on a fully-present batch
			// (e.g. CRC mismatch) is corruption too - treating either as truncation would
			// silently drop records and leave the partition re-fetching the same data forever.
			if (batches.length > 0 && isTruncatedTrailingBatch(decoder)) {
				break
			}
			try {
				// Hot-path decode options:
				// - Assume sequential offsets to avoid per-record BigInt conversions
				const batch = await decodeRecordBatchFrom(decoder, {
					assumeSequentialOffsets: true,
					verifyCrc: this.config.checkCrcs !== false,
				})
				batches.push(batch)
			} catch (e) {
				throw new Error(`Failed to decode record batch (corrupt data): ${(e as Error).message}`)
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
			lastOffsetDelta: number
			records: DecodedRecord[]
		}>,
		abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>,
		lastStableOffset: bigint
	): DecodedBatchResult {
		// Offset immediately after the last decoded batch, across ALL batches (data, control, and
		// empty). The caller advances the fetch position here even when no user records survive.
		let nextOffset: bigint | null = null
		for (const batch of batches) {
			const end = batch.baseOffset + BigInt(batch.lastOffsetDelta) + 1n
			if (nextOffset === null || end > nextOffset) {
				nextOffset = end
			}
		}

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
				return { records: result, nextOffset }
			}

			// Common case: a single non-control batch.
			// Avoid copying the records array into a new array.
			if (batches.length === 1) {
				const batch = batches[0]!
				if (isControlBatch(batch.attributes)) {
					return { records: result, nextOffset }
				}
				return { records: batch.records, nextOffset }
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
				return { records: result, nextOffset }
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
			return { records: out, nextOffset }
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

		return { records: result, nextOffset }
	}
}
