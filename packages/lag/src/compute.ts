/**
 * Pure lag arithmetic. Everything that talks to Kafka lives in collector.ts; this module is
 * what the unit tests pin down.
 */

export interface RecordAt {
	/** Offset of the first non-control record at or after the requested offset */
	offset: bigint
	/** Record timestamp in epoch ms, or -1 when the record has none */
	timestamp: bigint
}

export interface PartitionLagInput {
	committedOffset: bigint
	latestOffset: bigint
	/**
	 * Result of fetching at `fetchOffsetFor(...)`:
	 * - `undefined`: not fetched (caught up, or the fetch failed)
	 * - `null`: fetched, but no readable record exists between that offset and the log end (only
	 *   control records or aborted transactional records remain)
	 * - `RecordAt`: the record found
	 */
	record: RecordAt | null | undefined
	/** Current time, epoch ms */
	now: number
}

/**
 * The offset whose record timestamp defines time lag, or `null` when no fetch is needed.
 *
 * Caught-up partitions (and committed offsets beyond the log end, which cannot be measured) need no
 * fetch. A committed offset below the log start offset points at a deleted record, so the oldest
 * *available* record (at the log start) stands in for it.
 */
export function fetchOffsetFor(committedOffset: bigint, earliestOffset: bigint, latestOffset: bigint): bigint | null {
	if (committedOffset >= latestOffset) return null
	return committedOffset < earliestOffset ? earliestOffset : committedOffset
}

/**
 * Age in seconds of the oldest record the group still has to read, `0` when caught up, `null` when
 * unknown (fetch failed, record without a timestamp, or a committed offset beyond the log end).
 *
 * When the committed record has been deleted (retention or compaction) the next surviving record
 * stands in for it, so the result is a lower bound in that case.
 */
export function computePartitionLag(input: PartitionLagInput): number | null {
	const { committedOffset, latestOffset, record, now } = input

	// A commit beyond the log end (topic recreated, log truncated, or a hand-written offset) is not
	// "caught up": the consumer will hit OffsetOutOfRange and reset. Lag is unknown, not zero.
	if (committedOffset > latestOffset) return null
	if (committedOffset === latestOffset) return 0
	if (record === undefined) return null
	// Everything between the committed offset and the log end is invisible to the consumer
	// (transaction markers, aborted records), so it has nothing left to read.
	if (record === null) return 0
	if (record.timestamp < 0n) return null

	return Math.max(0, now - Number(record.timestamp)) / 1000
}
