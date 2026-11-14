import type { StreamClock, WorkerContext } from '@/processors/base.js'

const CLEANUP_INTERVAL_MS = 60_000

type TimedRecord = { topic: string; partition: number }

/**
 * Tracks stream time (the highest record timestamp seen) per source topic-partition.
 *
 * Stream time must be per partition: partitions progress independently (lag, backfill, rebalance),
 * so a single shared clock would let one partition that is far ahead mark in-order records of
 * another partition as "late" and silently drop them. State is partitioned the same way, so both
 * late-record decisions and store expiry use the clock of the record's own partition.
 *
 * Each worker clone of a node gets its own view (see {@link StreamTimeTracker.attach}).
 */
export class StreamTimeTracker {
	private static nextId = 0

	constructor(
		private readonly id: number = StreamTimeTracker.nextId++,
		private readonly clocks: Map<string, StreamClock> = new Map()
	) {}

	/**
	 * The tracker as seen by one worker. Clocks live on the worker so the runtime can reset them
	 * with the rest of the worker's uncommitted progress: stream time advanced by records of an
	 * aborted transaction must not make those same records look late when they are replayed.
	 */
	attach(worker: WorkerContext): StreamTimeTracker {
		return new StreamTimeTracker(this.id, worker.streamClocks)
	}

	private key(record: TimedRecord): string {
		return `${this.id}|${record.topic}:${record.partition}`
	}

	/** Stream time of the record's partition (0 if nothing observed yet). */
	streamTime(record: TimedRecord): number {
		return this.clocks.get(this.key(record))?.streamTimeMs ?? 0
	}

	/**
	 * Make sure the record's partition has a clock, seeding a new one from durable state.
	 *
	 * Clocks only live in memory and are dropped whenever the partition restarts from its committed
	 * offset (restart, rebalance, aborted transaction). Starting again from 0 would accept a late
	 * record that had already been dropped as past retention and overwrite the final result of an
	 * expired window, so the clock resumes from the newest timestamp found in the restored store.
	 */
	async ensureClock(record: TimedRecord, seed: () => Promise<number>): Promise<void> {
		const key = this.key(record)
		if (this.clocks.has(key)) return
		const streamTimeMs = await seed()
		if (!this.clocks.has(key)) {
			this.clocks.set(key, { streamTimeMs, lastCleanupMs: 0 })
		}
	}

	/**
	 * Advance the record's partition clock and, once per CLEANUP_INTERVAL_MS of stream time, call
	 * `expire` with the current stream time as cutoff. Retention is driven by stream time (not wall
	 * clock) so backfill/replay matches live processing; without it, store-backed processors leak.
	 */
	async advance(record: TimedRecord, timestamp: number, expire: (cutoff: number) => Promise<unknown>) {
		const key = this.key(record)
		let state = this.clocks.get(key)
		if (!state) {
			state = { streamTimeMs: 0, lastCleanupMs: 0 }
			this.clocks.set(key, state)
		}
		if (timestamp > state.streamTimeMs) {
			state.streamTimeMs = timestamp
		}
		if (state.streamTimeMs - state.lastCleanupMs <= CLEANUP_INTERVAL_MS) {
			return
		}
		state.lastCleanupMs = state.streamTimeMs
		await expire(state.streamTimeMs)
	}
}
