import {
	Decoder,
	ErrorCode,
	KafkaProtocolError,
	decodeRecordBatchFrom,
	isControlBatch,
	isTransactional,
	noopLogger,
	shouldRefreshMetadata,
	type Admin,
	type ConsumerGroupDescription,
	type DecodedRecordBatch,
} from '@kafkats/client'
import { computePartitionLag, fetchOffsetFor, type RecordAt } from './compute.js'
import { isSelected } from './filter.js'
import { RateTracker } from './rate.js'
import {
	emptyStats,
	type CollectionError,
	type CollectionStats,
	type GroupLag,
	type GroupMemberRef,
	type LagMonitorConfig,
	type LagSnapshot,
	type LagSource,
	type PartitionLag,
} from './types.js'

export type LagCollectorConfig = Omit<LagMonitorConfig, 'intervalMs' | 'meter' | 'clusterName'>

/** Enough for the broker to return the first record batch whole; it always returns at least one batch. */
const TIMESTAMP_FETCH_MAX_BYTES = 64 * 1024
const TIMESTAMP_FETCH_MAX_WAIT_MS = 100
/**
 * Fetches per partition per pass while stepping over batches with no readable record (transaction
 * markers, aborted transactions). Exceeding it is a failure, not "no records".
 */
const TIMESTAMP_FETCH_MAX_ROUNDS = 16
const GROUP_CONCURRENCY = 8
const DEFAULT_MAX_CONCURRENT_FETCHES = 8
const DEFAULT_TIMESTAMP_CACHE_TTL_MS = 10 * 60_000

interface PartitionOffsets {
	topic: string
	partition: number
	earliestOffset: bigint
	latestOffset: bigint
}

interface CachedRecord {
	record: RecordAt | null
	resolvedAt: number
}

interface CommittedPartition {
	topic: string
	partition: number
	committedOffset: bigint
}

/** A committed partition joined with its log bounds; the unit everything after offset discovery works on */
interface Target extends CommittedPartition {
	groupId: string
	bounds: PartitionOffsets
	/** Offset to fetch for time lag, or `null` when no fetch is needed (caught up, or estimated) */
	fetchOffset: bigint | null
	/** Rate-based estimate, when the mode allows one and the partition's history supports it */
	estimatedLagSeconds: number | null
	/** Filled in as the pass resolves the partition; `unknown` until then */
	source: LagSource
}

/**
 * One collection pass over the cluster: groups → committed offsets → partition bounds →
 * record timestamps → per-partition and per-group lag.
 */
export class LagCollector {
	private readonly admin: Admin
	private readonly config: Required<
		Pick<
			LagCollectorConfig,
			'maxConcurrentFetches' | 'timestampCacheTtlMs' | 'isolationLevel' | 'logger' | 'now' | 'mode'
		>
	> &
		LagCollectorConfig
	private readonly rates = new RateTracker()
	private readonly timestampCache = new Map<string, CachedRecord>()
	private errors: CollectionError[] = []
	/** Groups for which some partition's lag could not be determined this pass */
	private incompleteGroups = new Set<string>()
	private stats: CollectionStats = emptyStats()

	constructor(config: LagCollectorConfig) {
		this.config = {
			...config,
			maxConcurrentFetches: config.maxConcurrentFetches ?? DEFAULT_MAX_CONCURRENT_FETCHES,
			timestampCacheTtlMs: config.timestampCacheTtlMs ?? DEFAULT_TIMESTAMP_CACHE_TTL_MS,
			isolationLevel: config.isolationLevel ?? 'read_uncommitted',
			mode: config.mode ?? 'exact',
			logger: config.logger ?? noopLogger,
			now: config.now ?? Date.now,
		}
		this.admin = config.client.admin()
	}

	async collect(): Promise<LagSnapshot> {
		const startedAt = this.config.now()
		this.errors = []
		this.incompleteGroups = new Set()
		this.stats = emptyStats()

		const groupIds = await this.discoverGroups()
		const [descriptions, committedByGroup] = await Promise.all([
			this.describeGroups(groupIds),
			this.fetchCommittedOffsets(groupIds),
		])
		const { bounds, deletedTopics, highWatermarks } = await this.fetchPartitionBounds(committedByGroup)

		const estimating = this.config.mode !== 'exact'
		if (estimating) {
			const sampledAt = this.config.now()
			for (const [key, b] of bounds) this.rates.observe(key, highWatermarks.get(key) ?? b.latestOffset, sampledAt)
			// Only prune on a clean pass: a transient ListOffsets failure must not throw away the history,
			// or every affected partition warms up again and `auto` falls back to fetching them all.
			if (this.errors.length === 0) this.rates.retain(new Set(bounds.keys()))
		}

		const targets: Target[] = []
		for (const [groupId, committed] of committedByGroup) {
			for (const tp of committed) {
				const key = partitionKey(tp.topic, tp.partition)
				const b = bounds.get(key)
				if (!b) {
					if (!deletedTopics.has(tp.topic)) {
						this.incompleteGroups.add(groupId)
						this.stats.partitions.unknown++
					}
					continue
				}
				// Caught up and beyond-the-log-end are settled from the offsets alone; only a partition
				// that is actually behind needs an estimate or a fetch.
				const behind = tp.committedOffset < b.latestOffset
				const estimatedLagSeconds =
					behind && estimating ? this.rates.estimate(key, tp.committedOffset, startedAt) : null
				// `estimate` never fetches; `auto` fetches only where the estimate came up empty
				const fetchOffset =
					!behind || this.config.mode === 'estimate' || estimatedLagSeconds !== null
						? null
						: fetchOffsetFor(tp.committedOffset, b.earliestOffset, b.latestOffset)
				const source: LagSource =
					tp.committedOffset === b.latestOffset
						? 'caught_up'
						: estimatedLagSeconds !== null
							? 'estimated'
							: 'unknown'
				targets.push({ ...tp, groupId, bounds: b, fetchOffset, estimatedLagSeconds, source })
			}
		}

		const records = await this.resolveRecordTimestamps(targets)

		const now = this.config.now()
		const groups = Array.from(committedByGroup.keys(), groupId =>
			this.buildGroupLag(
				groupId,
				descriptions.get(groupId),
				targets.filter(t => t.groupId === groupId),
				records,
				now
			)
		)

		return {
			collectedAt: startedAt,
			durationMs: this.config.now() - startedAt,
			groups: groups.sort((a, b) => a.groupId.localeCompare(b.groupId)),
			errors: this.errors,
			stats: this.stats,
		}
	}

	private fail(scope: string, message: string, error: unknown, context: Record<string, unknown> = {}): void {
		const detail = error instanceof Error ? error.message : String(error)
		this.errors.push({ scope, message: `${message}: ${detail}` })
		this.config.logger.warn(`lag: ${message}`, { ...context, error: detail })
	}

	private async discoverGroups(): Promise<string[]> {
		try {
			// Strict: a broker that cannot be queried would otherwise silently drop every group it
			// coordinates from this snapshot, and the metric reset would erase their series.
			const listed = await this.admin.listGroups({ strict: true })
			return listed
				.filter(g => g.protocolType === '' || g.protocolType === 'consumer')
				.map(g => g.groupId)
				.filter(id => isSelected(id, this.config.groups, this.config.excludeGroups))
				.sort()
		} catch (error) {
			this.fail('groups', 'ListGroups failed', error)
			return []
		}
	}

	private async describeGroups(groupIds: string[]): Promise<Map<string, ConsumerGroupDescription>> {
		const result = new Map<string, ConsumerGroupDescription>()
		if (groupIds.length === 0) return result
		try {
			for (const description of await this.admin.describeGroups(groupIds)) {
				if (description.errorCode !== ErrorCode.None) {
					this.errors.push({
						scope: `group:${description.groupId}`,
						message: `DescribeGroups returned ${ErrorCode[description.errorCode] ?? description.errorCode}`,
					})
					continue
				}
				result.set(description.groupId, description)
			}
		} catch (error) {
			this.fail('groups', 'DescribeGroups failed', error)
		}
		return result
	}

	private async fetchCommittedOffsets(groupIds: string[]): Promise<Map<string, CommittedPartition[]>> {
		const result = new Map<string, CommittedPartition[]>()
		await parallel(groupIds, GROUP_CONCURRENCY, async groupId => {
			try {
				const offsets = await this.admin.listConsumerGroupOffsets(groupId)
				result.set(
					groupId,
					offsets.flatMap(o =>
						o.offset !== null && isSelected(o.topic, this.config.topics, this.config.excludeTopics)
							? [{ topic: o.topic, partition: o.partition, committedOffset: o.offset }]
							: []
					)
				)
			} catch (error) {
				this.fail(`group:${groupId}`, 'OffsetFetch failed', error, { groupId })
			}
		})
		return result
	}

	private async fetchPartitionBounds(committedByGroup: Map<string, CommittedPartition[]>): Promise<{
		bounds: Map<string, PartitionOffsets>
		deletedTopics: Set<string>
		/** Log end offsets when they differ from `latestOffset`, i.e. under `read_committed` with estimation on */
		highWatermarks: Map<string, bigint>
	}> {
		const partitionsByTopic = new Map<string, Set<number>>()
		const deletedTopics = new Set<string>()
		const highWatermarks = new Map<string, bigint>()
		// The produce rate must come from the log end, not the last stable offset: an open transaction
		// freezes the LSO and its commit releases it all at once, which would read as a rate spike.
		const sampleHighWatermark = this.config.mode !== 'exact' && this.config.isolationLevel === 'read_committed'
		for (const committed of committedByGroup.values()) {
			for (const tp of committed) {
				partitionsByTopic.set(tp.topic, (partitionsByTopic.get(tp.topic) ?? new Set()).add(tp.partition))
			}
		}

		// Groups keep committed offsets for topics that have since been deleted. Those cannot be measured
		// and are not an error; skip them rather than fail the pass on every collection.
		if (partitionsByTopic.size > 0) {
			try {
				const existing = new Set(await this.admin.listTopics())
				for (const topic of partitionsByTopic.keys()) {
					if (!existing.has(topic)) {
						partitionsByTopic.delete(topic)
						deletedTopics.add(topic)
						this.config.logger.debug('lag: skipping committed offsets for deleted topic', { topic })
					}
				}
			} catch (error) {
				this.fail('topics', 'Metadata failed', error)
				return { bounds: new Map(), deletedTopics, highWatermarks }
			}
		}

		const bounds = new Map<string, PartitionOffsets>()
		await parallel(Array.from(partitionsByTopic), GROUP_CONCURRENCY, async ([topic, partitionSet]) => {
			const partitions = Array.from(partitionSet)
			try {
				const [earliest, latest, logEnd] = await Promise.all([
					this.admin.fetchTopicOffsets(topic, partitions, 'earliest'),
					this.admin.fetchTopicOffsets(topic, partitions, 'latest', {
						isolationLevel: this.config.isolationLevel,
					}),
					sampleHighWatermark ? this.admin.fetchTopicOffsets(topic, partitions, 'latest') : undefined,
				])
				for (const partition of partitions) {
					const earliestOffset = earliest.get(partition)
					const latestOffset = latest.get(partition)
					if (earliestOffset === undefined || latestOffset === undefined) continue
					const key = partitionKey(topic, partition)
					bounds.set(key, { topic, partition, earliestOffset, latestOffset })
					const hwm = logEnd?.get(partition)
					if (hwm !== undefined) highWatermarks.set(key, hwm)
				}
			} catch (error) {
				this.fail(`offsets:${topic}`, 'ListOffsets failed', error, { topic })
			}
		})
		return { bounds, deletedTopics, highWatermarks }
	}

	/**
	 * Resolve the record at each distinct (topic, partition, fetch offset) once per pass, reusing
	 * cached results. Two groups committed at the same offset share a single fetch, and a group
	 * whose commit has not moved costs nothing until the cache entry expires.
	 */
	private async resolveRecordTimestamps(targets: Target[]): Promise<Map<string, RecordAt | null>> {
		const wanted = new Map<string, Target>()
		const fetchedKeys = new Set<string>()
		// Cache entries are kept for every partition that is behind, estimated or not: if an estimate
		// stops being possible later, `auto` can still reuse the exact timestamp instead of refetching.
		const retain = new Set<string>()
		for (const t of targets) {
			if (t.fetchOffset !== null) wanted.set(recordKey(t), t)
			if (t.committedOffset < t.bounds.latestOffset) retain.add(recordKey(t))
		}

		const now = this.config.now()
		const resolved = new Map<string, RecordAt | null>()
		const toFetch: Target[] = []
		for (const [key, target] of wanted) {
			const cached = this.timestampCache.get(key)
			if (cached && now - cached.resolvedAt < this.config.timestampCacheTtlMs) {
				resolved.set(key, cached.record)
			} else {
				toFetch.push(target)
			}
		}

		await parallel(toFetch, this.config.maxConcurrentFetches, async target => {
			const { topic, partition, fetchOffset } = target
			try {
				fetchedKeys.add(recordKey(target))
				const record = await this.fetchRecordAt(topic, partition, fetchOffset!, target.bounds.latestOffset)
				// A null result (nothing readable up to the log end) is not cached: the key stays the same
				// while new records arrive, and the next pass should see them.
				if (record) this.timestampCache.set(recordKey(target), { record, resolvedAt: this.config.now() })
				resolved.set(recordKey(target), record)
			} catch (error) {
				this.fail(`fetch:${topic}-${partition}`, `Fetch at offset ${fetchOffset} failed`, error, {
					topic,
					partition,
				})
			}
		})
		for (const t of targets) {
			if (t.fetchOffset === null) continue
			const key = recordKey(t)
			if (!resolved.has(key)) this.incompleteGroups.add(t.groupId)
			else t.source = fetchedKeys.has(key) ? 'fetched' : 'cached'
		}

		// Drop cache entries no group points at any more
		for (const key of this.timestampCache.keys()) {
			if (!retain.has(key)) this.timestampCache.delete(key)
		}

		return resolved
	}

	/**
	 * Read the first record at or after `offset` that a consumer would see: control batches are
	 * skipped, and under `read_committed` so are records of aborted transactions. Batches are
	 * decompressed to reach the per-record timestamps, but record keys, values, and headers are only
	 * skipped over.
	 *
	 * The broker guarantees only the *first* batch of a response fits, so when that batch holds
	 * nothing readable the fetch continues from the end of the last decoded batch until a record is
	 * found or `endOffset` is reached. `null` means nothing readable remains before `endOffset`.
	 *
	 * `KafkaClient.fetch` routes by cached metadata and does not retry, so a stale leader is refreshed
	 * and retried once here.
	 */
	private async fetchRecordAt(
		topic: string,
		partition: number,
		offset: bigint,
		endOffset: bigint
	): Promise<RecordAt | null> {
		const readCommitted = this.config.isolationLevel === 'read_committed'
		const fetchOptions = {
			maxWaitMs: TIMESTAMP_FETCH_MAX_WAIT_MS,
			minBytes: 1,
			maxBytes: TIMESTAMP_FETCH_MAX_BYTES,
			isolationLevel: readCommitted ? (1 as const) : (0 as const),
		}

		let position = offset
		for (let round = 0; position < endOffset; round++) {
			if (round >= TIMESTAMP_FETCH_MAX_ROUNDS) {
				throw new Error(`No readable record within ${TIMESTAMP_FETCH_MAX_ROUNDS} fetches from offset ${offset}`)
			}

			this.stats.fetchRequests++
			let response = await this.config.client.fetch(topic, partition, position, fetchOptions)
			if (response.errorCode !== ErrorCode.None && shouldRefreshMetadata(response.errorCode)) {
				await this.config.client.getMetadata([topic])
				this.stats.fetchRequests++
				response = await this.config.client.fetch(topic, partition, position, fetchOptions)
			}
			this.stats.fetchBytes += response.recordsData?.length ?? 0
			if (response.errorCode !== ErrorCode.None) {
				throw new KafkaProtocolError(response.errorCode, `Fetch failed for ${topic}-${partition}`)
			}

			const batches = await decodeBatches(response.recordsData)
			if (batches.length === 0) return null

			const aborted = readCommitted
				? abortedRanges(response.abortedTransactions, batches, response.lastStableOffset)
				: []

			let nextPosition = position
			for (const batch of batches) {
				const batchEnd = batch.baseOffset + BigInt(batch.lastOffsetDelta) + 1n
				if (batchEnd > nextPosition) nextPosition = batchEnd
				if (isControlBatch(batch.attributes)) continue

				const ranges = isTransactional(batch.attributes)
					? aborted.filter(r => r.producerId === batch.producerId)
					: []
				for (const record of batch.records) {
					if (record.offset < offset) continue
					if (ranges.some(r => record.offset >= r.start && record.offset < r.endExclusive)) continue
					return { offset: record.offset, timestamp: record.timestamp }
				}
			}

			// The broker returned only batches below our position (should not happen); stop rather than spin.
			if (nextPosition <= position) return null
			position = nextPosition
		}
		return null
	}

	private timeLagFor(t: Target, records: Map<string, RecordAt | null>, now: number): number | null {
		if (t.estimatedLagSeconds !== null) return t.estimatedLagSeconds
		// In `estimate` mode a caught-up partition is still exactly 0; behind or beyond the end is unknown
		if (this.config.mode === 'estimate') return t.committedOffset === t.bounds.latestOffset ? 0 : null
		return computePartitionLag({
			committedOffset: t.committedOffset,
			latestOffset: t.bounds.latestOffset,
			record: t.fetchOffset === null ? undefined : records.get(recordKey(t)),
			now,
		})
	}

	private buildGroupLag(
		groupId: string,
		description: ConsumerGroupDescription | undefined,
		targets: Target[],
		records: Map<string, RecordAt | null>,
		now: number
	): GroupLag {
		const memberByPartition = new Map<string, GroupMemberRef>()
		for (const { memberId, clientId, clientHost, assignment } of description?.members ?? []) {
			for (const tp of assignment) {
				memberByPartition.set(partitionKey(tp.topic, tp.partition), { memberId, clientId, clientHost })
			}
		}

		for (const t of targets) this.stats.partitions[t.source]++

		const partitions: PartitionLag[] = targets
			.map(t => ({
				topic: t.topic,
				partition: t.partition,
				committedOffset: t.committedOffset,
				earliestOffset: t.bounds.earliestOffset,
				latestOffset: t.bounds.latestOffset,
				timeLagSeconds: this.timeLagFor(t, records, now),
				member: memberByPartition.get(partitionKey(t.topic, t.partition)) ?? null,
			}))
			.sort(comparePartitions)

		// A maximum over a partial set would under-report whenever the most-lagged partition is the one
		// without a value, whether that is a failed fetch, an estimate still warming up, or an idle
		// partition in `estimate` mode. The group maximum is only emitted when every partition has one.
		const timeLags = partitions.map(p => p.timeLagSeconds)
		const complete = !this.incompleteGroups.has(groupId) && timeLags.every(v => v !== null)

		return {
			groupId,
			partitions,
			maxTimeLagSeconds: complete && timeLags.length > 0 ? Math.max(...(timeLags as number[])) : null,
		}
	}
}

/**
 * Decode every whole batch in a fetch response. The broker returns the first batch whole; a partial
 * trailing batch is expected when the response was cut at maxBytes and is dropped. A decode failure
 * on the first batch is a real error.
 */
async function decodeBatches(data: Buffer | null): Promise<DecodedRecordBatch[]> {
	const batches: DecodedRecordBatch[] = []
	if (!data || data.length === 0) return batches
	const decoder = new Decoder(data)
	while (decoder.remaining() > 0) {
		try {
			batches.push(await decodeRecordBatchFrom(decoder, { verifyCrc: false }))
		} catch (error) {
			if (batches.length > 0) break
			throw error
		}
	}
	return batches
}

/**
 * Offset ranges (exclusive end) covered by aborted transactions in this response. Mirrors the
 * consumer's fetch manager: a transaction ends at its producer's next control marker, or at the
 * last stable offset when the marker lies beyond the fetched batches.
 */
function abortedRanges(
	abortedTransactions: ReadonlyArray<{ producerId: bigint; firstOffset: bigint }>,
	batches: DecodedRecordBatch[],
	lastStableOffset: bigint
): Array<{ producerId: bigint; start: bigint; endExclusive: bigint }> {
	if (abortedTransactions.length === 0) return []
	const markers = batches
		.filter(b => isControlBatch(b.attributes))
		.map(b => ({ producerId: b.producerId, offset: b.baseOffset }))
		.sort((a, b) => (a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0))
	return abortedTransactions.map(tx => ({
		producerId: tx.producerId,
		start: tx.firstOffset,
		endExclusive:
			markers.find(m => m.producerId === tx.producerId && m.offset > tx.firstOffset)?.offset ?? lastStableOffset,
	}))
}

function partitionKey(topic: string, partition: number): string {
	return `${topic} ${partition}`
}

/** Keyed by the offset a fetch would read, so estimated and fetched passes share cache entries */
function recordKey(t: Target): string {
	const offset = t.fetchOffset ?? fetchOffsetFor(t.committedOffset, t.bounds.earliestOffset, t.bounds.latestOffset)
	return `${t.topic} ${t.partition} ${offset}`
}

function comparePartitions(a: { topic: string; partition: number }, b: { topic: string; partition: number }): number {
	return a.topic.localeCompare(b.topic) || a.partition - b.partition
}

async function parallel<T>(items: readonly T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
	let next = 0
	const workers = Array.from({ length: Math.min(Math.max(1, concurrency), items.length) }, async () => {
		while (next < items.length) await fn(items[next++]!)
	})
	await Promise.all(workers)
}
