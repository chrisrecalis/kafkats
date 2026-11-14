/**
 * @file Aggregation processor nodes (count, reduce, aggregate; windowed and session variants).
 *
 * Idempotence: every aggregator in this file does a sequential read-modify-write
 * (`store.get` → apply → `store.put`) per record. The RMW is NOT transactional
 * across input-offset commits. Under the default `at_least_once` processing
 * guarantee, a redelivery (e.g. crash before offset commit) re-applies the
 * aggregator and double-counts. This matches Kafka Streams' at_least_once
 * semantics. Configure the flow with `processingGuarantee: 'exactly_once'` to
 * scope state writes + offset commits + downstream produce into a single
 * transactional batch (see flow.ts `commitTransactionBatch`).
 *
 * Session-window variants (SessionReduceNode, SessionAggregateNode) are extra
 * sensitive: redelivery can cause session deletion + re-merge with new events,
 * producing structurally different (not just numerically inflated) output.
 */

import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore, WindowStore, SessionStore, WindowedKey } from '@/state.js'
import type { Windowed } from '@/types.js'

/**
 * Processor node for aggregations (count, reduce, aggregate).
 * Maintains state in a KeyValueStore and emits updated aggregates downstream.
 */
export class AggregateNode<K, V, A> extends Processor<K, V, K, A> {
	private store: KeyValueStore<K, A> | null = null

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, A> {
		void worker
		// Share the same store reference across clones
		return new AggregateNode<K, V, A>(this.storeName, this.storeRef, this.initializer, this.aggregator)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys for aggregations
			return
		}
		if (record.value === null) {
			// Skip tombstones for aggregations
			return
		}

		const storedAggregate = await store.get(key)
		const aggregate: A = storedAggregate !== undefined ? storedAggregate : this.initializer()

		// Apply aggregation
		const newAggregate = this.aggregator(key, record.value, aggregate)

		// Store updated aggregate
		await store.put(key, newAggregate)

		// Forward the updated aggregate downstream
		const next: StreamRecord<K, A> = {
			...record,
			key,
			value: newAggregate,
		}
		await this.forward(next)
	}
}

/**
 * Processor node for reduce operations.
 * Maintains reduced state in a KeyValueStore and emits updated aggregates downstream.
 */
export class ReduceNode<K, V> extends Processor<K, V, K, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, V> | null },
		private readonly reducer: (aggregate: V, value: V) => V
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new ReduceNode<K, V>(this.storeName, this.storeRef, this.reducer)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const storedAggregate = await store.get(key)
		const newAggregate = storedAggregate === undefined ? value : this.reducer(storedAggregate, value)
		await store.put(key, newAggregate)

		const next: StreamRecord<K, V> = { ...record, key, value: newAggregate }
		await this.forward(next)
	}
}

// Validate and default TimeWindows.advanceBy: 0 ⇒ tumbling (advance == size); advance > size would create gaps that drop records.
function resolveAdvanceMs(sizeMs: number, advanceMs: number): number {
	const advance = advanceMs > 0 ? advanceMs : sizeMs
	if (advance > sizeMs) {
		throw new Error(
			`TimeWindows.advanceBy must be <= window size (got advance=${advance}, size=${sizeMs}). ` +
				'Larger advance values would silently drop records that fall in the gaps between windows.'
		)
	}
	return advance
}

const CLEANUP_INTERVAL_MS = 60_000

// Window/session stores retain state for this multiple of the window size / session gap
// (see grouped.ts store creation). The aggregation nodes reuse the same factor to detect a
// late record whose window/session has already passed retention, so they don't resurrect it.
export const WINDOW_STORE_RETENTION_MULTIPLIER = 24

type TimedRecord = { topic: string; partition: number }

/**
 * Tracks stream time (the highest record timestamp seen) per source topic-partition.
 *
 * Stream time must be per partition: partitions progress independently (lag, backfill, rebalance),
 * so a single shared clock would let one partition that is far ahead mark in-order records of
 * another partition as "late" and silently drop them. State is partitioned the same way, so both
 * late-record decisions and store expiry use the clock of the record's own partition.
 *
 * One tracker is shared by all worker clones of a node.
 */
export class StreamTimeTracker {
	private readonly byPartition = new Map<string, { streamTimeMs: number; lastCleanupMs: number }>()

	attach(worker: WorkerContext): this {
		void worker
		return this
	}

	/** Stream time of the record's partition (0 if nothing observed yet). */
	streamTime(record: TimedRecord): number {
		return this.byPartition.get(`${record.topic}:${record.partition}`)?.streamTimeMs ?? 0
	}

	/**
	 * Advance the record's partition clock. Returns an expiry cutoff when a cleanup pass is due for
	 * that partition (once per CLEANUP_INTERVAL_MS of stream time), otherwise undefined.
	 */
	observe(record: TimedRecord, timestamp: number): number | undefined {
		const key = `${record.topic}:${record.partition}`
		let state = this.byPartition.get(key)
		if (!state) {
			state = { streamTimeMs: 0, lastCleanupMs: 0 }
			this.byPartition.set(key, state)
		}
		if (timestamp > state.streamTimeMs) {
			state.streamTimeMs = timestamp
		}
		if (state.streamTimeMs - state.lastCleanupMs <= CLEANUP_INTERVAL_MS) {
			return undefined
		}
		state.lastCleanupMs = state.streamTimeMs
		return state.streamTimeMs
	}
}

// Stream-time-driven retention: advance the record's partition clock and fire expiry once per
// CLEANUP_INTERVAL_MS of stream time. Caller passes the store's expire method (windows or
// sessions); without this, state-store-backed processors leak. Shared by the windowed/session
// aggregation nodes and the stream-stream join nodes.
export async function maybeExpire(
	tracker: StreamTimeTracker,
	record: TimedRecord,
	timestamp: number,
	expire: (cutoff: number) => Promise<unknown>
) {
	const cutoff = tracker.observe(record, timestamp)
	if (cutoff !== undefined) {
		await expire(cutoff)
	}
}

const bySessionStart = <K>(a: { windowedKey: WindowedKey<K> }, b: { windowedKey: WindowedKey<K> }): number =>
	a.windowedKey.windowStart - b.windowedKey.windowStart || a.windowedKey.windowEnd - b.windowedKey.windowEnd

// Half-open windows [start, start+size); negative starts are skipped — only matters for timestamps near epoch.
function* windowStartsFor(
	timestamp: number,
	sizeMs: number,
	advanceMs: number
): Iterable<{ start: number; end: number }> {
	const first = Math.floor((timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs
	const last = Math.floor(timestamp / advanceMs) * advanceMs
	for (let start = first; start <= last; start += advanceMs) {
		if (start < 0) continue
		yield { start, end: start + sizeMs }
	}
}

/**
 * Processor node for windowed reduce operations. Honours TimeWindows.advanceBy() (hopping windows).
 */
export class WindowedReduceNode<K, V> extends Processor<K, V, Windowed<K>, V> {
	private readonly advanceMs: number
	private readonly cleanupState: StreamTimeTracker

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: WindowStore<K, V> | null },
		private readonly reducer: (aggregate: V, value: V) => V,
		private readonly windowSizeMs: number,
		advanceMs: number = 0,
		cleanupState?: StreamTimeTracker
	) {
		super()
		this.advanceMs = resolveAdvanceMs(windowSizeMs, advanceMs)
		this.cleanupState = cleanupState ?? new StreamTimeTracker()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, V> {
		return new WindowedReduceNode<K, V>(
			this.storeName,
			this.storeRef,
			this.reducer,
			this.windowSizeMs,
			this.advanceMs,
			this.cleanupState.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Window store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)
		const retentionCutoff =
			this.cleanupState.streamTime(record) - this.windowSizeMs * WINDOW_STORE_RETENTION_MULTIPLIER
		for (const { start, end } of windowStartsFor(timestamp, this.windowSizeMs, this.advanceMs)) {
			// Drop a late record whose window has already passed retention: re-creating the
			// window from an empty aggregate emits a wrong partial result that contradicts the
			// already-finalized (and possibly expired) window.
			if (end < retentionCutoff) continue
			const windowedKey: WindowedKey<K> = { key, windowStart: start, windowEnd: end }

			const storedAggregate = await store.get(windowedKey)
			const newAggregate = storedAggregate === undefined ? value : this.reducer(storedAggregate, value)
			await store.put(windowedKey, newAggregate)

			const windowedResult: Windowed<K> = { key, window: { start, end } }
			const next: StreamRecord<Windowed<K>, V> = { ...record, key: windowedResult, value: newAggregate }
			await this.forward(next)
		}

		await maybeExpire(this.cleanupState, record, timestamp, t => store.expireOldWindows(t))
	}
}

/**
 * Processor node for session-window reduce operations.
 * Sessions are merged when events arrive within the inactivity gap.
 */
export class SessionReduceNode<K, V> extends Processor<K, V, Windowed<K>, V> {
	private readonly cleanupState: StreamTimeTracker

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: SessionStore<K, V> | null },
		private readonly reducer: (aggregate: V, value: V) => V,
		private readonly gapMs: number,
		cleanupState?: StreamTimeTracker
	) {
		super()
		this.cleanupState = cleanupState ?? new StreamTimeTracker()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, V> {
		return new SessionReduceNode<K, V>(
			this.storeName,
			this.storeRef,
			this.reducer,
			this.gapMs,
			this.cleanupState.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Session store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)
		const searchFrom = timestamp - this.gapMs
		const searchTo = timestamp + this.gapMs

		const overlappingSessions: Array<{ windowedKey: WindowedKey<K>; value: V }> = []
		for await (const [windowedKey, sessionValue] of store.findSessions(key, searchFrom, searchTo)) {
			overlappingSessions.push({ windowedKey, value: sessionValue })
		}

		// Drop a late record that has no live session to merge into and whose own session
		// would already be past retention — otherwise an expired session is resurrected.
		const retentionCutoff = this.cleanupState.streamTime(record) - this.gapMs * WINDOW_STORE_RETENTION_MULTIPLIER
		if (overlappingSessions.length === 0 && timestamp < retentionCutoff) {
			return
		}

		// Fold existing sessions in event-time order, then apply the new record last, so the
		// reducer always sees (aggregate, value) — required for non-commutative reducers.
		overlappingSessions.sort(bySessionStart)

		let mergedStart = timestamp
		let mergedEnd = timestamp
		let merged: { value: V } | null = null

		for (const { windowedKey, value: sessionValue } of overlappingSessions) {
			await store.delete(windowedKey)
			mergedStart = Math.min(mergedStart, windowedKey.windowStart)
			mergedEnd = Math.max(mergedEnd, windowedKey.windowEnd)
			merged = { value: merged === null ? sessionValue : this.reducer(merged.value, sessionValue) }
		}
		const mergedValue: V = merged === null ? value : this.reducer(merged.value, value)

		const newSessionKey: WindowedKey<K> = { key, windowStart: mergedStart, windowEnd: mergedEnd }
		await store.put(newSessionKey, mergedValue)

		// Retract each merged-away session whose window differs from the new merged window, so a
		// downstream table/consumer keyed on the windowed key doesn't retain the absorbed sessions.
		for (const { windowedKey } of overlappingSessions) {
			if (windowedKey.windowStart === mergedStart && windowedKey.windowEnd === mergedEnd) continue
			const removed: Windowed<K> = { key, window: { start: windowedKey.windowStart, end: windowedKey.windowEnd } }
			await this.forward({ ...record, key: removed, value: null })
		}

		const windowedResult: Windowed<K> = { key, window: { start: mergedStart, end: mergedEnd } }
		const next: StreamRecord<Windowed<K>, V> = { ...record, key: windowedResult, value: mergedValue }
		await this.forward(next)

		await maybeExpire(this.cleanupState, record, timestamp, t => store.expireOldSessions(t))
	}
}

/**
 * Processor node for windowed aggregations. Honours TimeWindows.advanceBy() (hopping windows).
 */
export class WindowedAggregateNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	private readonly cleanupState: StreamTimeTracker
	private readonly advanceMs: number

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: WindowStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly windowSizeMs: number,
		advanceMs: number = 0,
		cleanupState?: StreamTimeTracker
	) {
		super()
		this.advanceMs = resolveAdvanceMs(windowSizeMs, advanceMs)
		this.cleanupState = cleanupState ?? new StreamTimeTracker()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		return new WindowedAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.windowSizeMs,
			this.advanceMs,
			this.cleanupState.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Window store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			return
		}
		if (record.value === null) {
			// Skip tombstones for aggregations
			return
		}

		const timestamp = Number(record.timestamp)
		const retentionCutoff =
			this.cleanupState.streamTime(record) - this.windowSizeMs * WINDOW_STORE_RETENTION_MULTIPLIER
		for (const { start, end } of windowStartsFor(timestamp, this.windowSizeMs, this.advanceMs)) {
			// Drop a late record whose window has already passed retention: re-creating the
			// window from the initializer emits a wrong partial result that contradicts the
			// already-finalized (and possibly expired) window.
			if (end < retentionCutoff) continue
			const windowedKey: WindowedKey<K> = { key, windowStart: start, windowEnd: end }

			const storedAggregate = await store.get(windowedKey)
			const aggregate: A = storedAggregate !== undefined ? storedAggregate : this.initializer()
			const newAggregate = this.aggregator(key, record.value, aggregate)
			await store.put(windowedKey, newAggregate)

			const windowedResult: Windowed<K> = { key, window: { start, end } }
			const next: StreamRecord<Windowed<K>, A> = { ...record, key: windowedResult, value: newAggregate }
			await this.forward(next)
		}

		// Stream-time-driven retention (not wall clock) so backfill/replay matches live processing.
		await maybeExpire(this.cleanupState, record, timestamp, t => store.expireOldWindows(t))
	}
}

/**
 * Processor node for session window aggregations.
 * Sessions are merged when events arrive within the inactivity gap.
 */
export class SessionAggregateNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	private readonly cleanupState: StreamTimeTracker

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: SessionStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly merger: (aggregate1: A, aggregate2: A) => A,
		private readonly gapMs: number,
		cleanupState?: StreamTimeTracker
	) {
		super()
		this.cleanupState = cleanupState ?? new StreamTimeTracker()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		return new SessionAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.merger,
			this.gapMs,
			this.cleanupState.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Session store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			return
		}
		if (record.value === null) {
			// Skip tombstones for aggregations
			return
		}

		const timestamp = Number(record.timestamp)

		// Find all sessions that could be merged with this event
		// A session overlaps if the new event's timestamp is within gap of the session's boundaries
		const searchFrom = timestamp - this.gapMs
		const searchTo = timestamp + this.gapMs

		const overlappingSessions: Array<{ windowedKey: WindowedKey<K>; value: A }> = []
		for await (const [windowedKey, value] of store.findSessions(key, searchFrom, searchTo)) {
			overlappingSessions.push({ windowedKey, value })
		}

		// Drop a late record that has no live session to merge into and whose own session
		// would already be past retention — otherwise an expired session is resurrected.
		const retentionCutoff = this.cleanupState.streamTime(record) - this.gapMs * WINDOW_STORE_RETENTION_MULTIPLIER
		if (overlappingSessions.length === 0 && timestamp < retentionCutoff) {
			return
		}

		// Merge in event-time order (store iteration order is not guaranteed).
		overlappingSessions.sort(bySessionStart)

		let mergedStart = timestamp
		let mergedEnd = timestamp
		let mergedValue = this.initializer()

		if (overlappingSessions.length > 0) {
			// Delete all overlapping sessions - they will be merged
			for (const { windowedKey } of overlappingSessions) {
				await store.delete(windowedKey)
				// Track merged boundaries
				mergedStart = Math.min(mergedStart, windowedKey.windowStart)
				mergedEnd = Math.max(mergedEnd, windowedKey.windowEnd)
			}

			// Merge all session values together
			for (const { value } of overlappingSessions) {
				mergedValue = this.merger(mergedValue, value)
			}
		}

		// Apply the new event's value
		mergedValue = this.aggregator(key, record.value, mergedValue)

		// Store the merged session
		const newSessionKey: WindowedKey<K> = {
			key,
			windowStart: mergedStart,
			windowEnd: mergedEnd,
		}
		await store.put(newSessionKey, mergedValue)

		// Retract each merged-away session whose window differs from the new merged window, so a
		// downstream table/consumer keyed on the windowed key doesn't retain the absorbed sessions.
		for (const { windowedKey } of overlappingSessions) {
			if (windowedKey.windowStart === mergedStart && windowedKey.windowEnd === mergedEnd) continue
			const removed: Windowed<K> = { key, window: { start: windowedKey.windowStart, end: windowedKey.windowEnd } }
			await this.forward({ ...record, key: removed, value: null })
		}

		// Forward the session result
		const windowedResult: Windowed<K> = {
			key,
			window: { start: mergedStart, end: mergedEnd },
		}

		const next: StreamRecord<Windowed<K>, A> = {
			...record,
			key: windowedResult,
			value: mergedValue,
		}
		await this.forward(next)

		await maybeExpire(this.cleanupState, record, timestamp, t => store.expireOldSessions(t))
	}
}
