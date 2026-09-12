import type { KafkaClient, Logger, MemberDescription } from '@kafkats/client'

/**
 * Selects consumer groups or topics by name.
 *
 * - `string[]`: exact names
 * - `RegExp`: tested against the name
 * - function: predicate
 */
export type NameFilter = string[] | RegExp | ((name: string) => boolean)

/**
 * How time lag is measured.
 *
 * - `exact`: fetch the record at the committed offset and read its timestamp. Accurate, but costs one
 *   small fetch per moving partition per collection.
 * - `estimate`: divide the offset lag by the partition's recent produce rate. No fetches, so it scales
 *   to very large partition counts, but it is an estimate and yields nothing while warming up or for
 *   idle partitions.
 * - `auto`: `estimate`, falling back to `exact` for the partitions where no estimate is possible.
 */
export type LagMode = 'exact' | 'estimate' | 'auto'

export interface LagMonitorConfig {
	/** Connected (or connectable) client used for all broker requests */
	client: KafkaClient
	/** Only monitor groups matching this filter (default: every consumer group) */
	groups?: NameFilter
	/** Skip groups matching this filter */
	excludeGroups?: NameFilter
	/** Only report partitions of topics matching this filter (default: every topic a group has committed to) */
	topics?: NameFilter
	/** Skip partitions of topics matching this filter */
	excludeTopics?: NameFilter
	/** Collection interval in milliseconds (default: 30000) */
	intervalMs?: number
	/** How time lag is measured (default: `exact`); see {@link LagMode} */
	mode?: LagMode
	/** Maximum concurrent timestamp fetches per collection (default: 8) */
	maxConcurrentFetches?: number
	/**
	 * How long a resolved (offset → record timestamp) entry stays cached before it is re-fetched
	 * (default: 600000 = 10 minutes). A committed offset that has not moved costs no fetches while
	 * cached; the periodic refresh only exists to notice compaction or retention deleting the record.
	 */
	timestampCacheTtlMs?: number
	/**
	 * Isolation level used for the "latest" offset (default: `read_uncommitted`, the high watermark).
	 * Use `read_committed` when the monitored consumers run with that isolation level so lag matches
	 * what they can actually read (the last stable offset).
	 */
	isolationLevel?: 'read_uncommitted' | 'read_committed'
	/** Logger for collection warnings (default: the client's logger is not reused; silent) */
	logger?: Logger
	/** Called with the current time; injectable for tests (default: Date.now) */
	now?: () => number
}

export type GroupMemberRef = Pick<MemberDescription, 'memberId' | 'clientId' | 'clientHost'>

export interface PartitionLag {
	topic: string
	partition: number
	/** The group's committed offset */
	committedOffset: bigint
	earliestOffset: bigint
	latestOffset: bigint
	/**
	 * Age in seconds of the oldest record the group has not yet consumed, or 0 when caught up. When the
	 * committed record was deleted by retention or compaction, the next surviving record stands in
	 * (a lower bound). `null` when unknown: the fetch failed, the record carries no timestamp, or the
	 * committed offset is beyond the log end.
	 */
	timeLagSeconds: number | null
	/** Member currently assigned this partition, if the group is active */
	member: GroupMemberRef | null
}

export interface GroupLag {
	groupId: string
	partitions: PartitionLag[]
	/**
	 * Maximum `timeLagSeconds` across partitions. `null` when any of the group's partitions is missing
	 * from `partitions` or has unknown lag because a broker request failed: a maximum over a partial
	 * set would silently under-report.
	 */
	maxTimeLagSeconds: number | null
}

export interface CollectionError {
	/** What failed: `collect`, `groups`, `group:<id>`, `offsets:<topic>`, `fetch:<topic>-<partition>` */
	scope: string
	message: string
}

/** How a partition's time lag was obtained in a collection */
export type LagSource = 'caught_up' | 'fetched' | 'cached' | 'estimated' | 'unknown'

/** Broker load and measurement mix of one collection; the numbers to watch when sizing the exporter */
export interface CollectionStats {
	/** Fetch requests sent to brokers for record timestamps, including retries and multi-batch rounds */
	fetchRequests: number
	/** Bytes of record data returned by those fetches */
	fetchBytes: number
	/** Partitions by how their lag was obtained */
	partitions: Record<LagSource, number>
}

export function emptyStats(): CollectionStats {
	return {
		fetchRequests: 0,
		fetchBytes: 0,
		partitions: { caught_up: 0, fetched: 0, cached: 0, estimated: 0, unknown: 0 },
	}
}

export interface LagSnapshot {
	/** Epoch ms when collection started */
	collectedAt: number
	/** Wall time the collection took */
	durationMs: number
	groups: GroupLag[]
	/** Errors during collection; the affected groups or partitions are missing from this snapshot */
	errors: CollectionError[]
	stats: CollectionStats
}

export interface LagMonitorEvents {
	snapshot: [snapshot: LagSnapshot]
}
