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

/**
 * Processor node for windowed reduce operations.
 *
 * Honours TimeWindows.advanceBy(): when advanceMs < windowSizeMs (hopping
 * windows), each record is reduced into every overlapping window and one
 * forward is emitted per window.
 */
export class WindowedReduceNode<K, V> extends Processor<K, V, Windowed<K>, V> {
	private readonly advanceMs: number

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: WindowStore<K, V> | null },
		private readonly reducer: (aggregate: V, value: V) => V,
		private readonly windowSizeMs: number,
		advanceMs: number = 0
	) {
		super()
		this.advanceMs = advanceMs > 0 ? advanceMs : windowSizeMs
		if (this.advanceMs > this.windowSizeMs) {
			throw new Error(
				`TimeWindows.advanceBy must be <= window size (got advance=${this.advanceMs}, size=${this.windowSizeMs}). ` +
					'Larger advance values would silently drop records that fall in the gaps between windows.'
			)
		}
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, V> {
		void worker
		return new WindowedReduceNode<K, V>(this.storeName, this.storeRef, this.reducer, this.windowSizeMs, this.advanceMs)
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
		const firstWindowStart =
			Math.floor((timestamp - this.windowSizeMs + this.advanceMs) / this.advanceMs) * this.advanceMs
		const lastWindowStart = Math.floor(timestamp / this.advanceMs) * this.advanceMs

		for (let windowStart = firstWindowStart; windowStart <= lastWindowStart; windowStart += this.advanceMs) {
			if (windowStart < 0) continue
			const windowEnd = windowStart + this.windowSizeMs
			const windowedKey: WindowedKey<K> = { key, windowStart, windowEnd }

			const storedAggregate = await store.get(windowedKey)
			const newAggregate = storedAggregate === undefined ? value : this.reducer(storedAggregate, value)
			await store.put(windowedKey, newAggregate)

			const windowedResult: Windowed<K> = { key, window: { start: windowStart, end: windowEnd } }
			const next: StreamRecord<Windowed<K>, V> = { ...record, key: windowedResult, value: newAggregate }
			await this.forward(next)
		}
	}
}

/**
 * Processor node for session-window reduce operations.
 * Sessions are merged when events arrive within the inactivity gap.
 */
export class SessionReduceNode<K, V> extends Processor<K, V, Windowed<K>, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: SessionStore<K, V> | null },
		private readonly reducer: (aggregate: V, value: V) => V,
		private readonly gapMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, V> {
		void worker
		return new SessionReduceNode<K, V>(this.storeName, this.storeRef, this.reducer, this.gapMs)
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

		let mergedStart = timestamp
		let mergedEnd = timestamp
		let mergedValue: V = value

		if (overlappingSessions.length > 0) {
			for (const { windowedKey, value: sessionValue } of overlappingSessions) {
				await store.delete(windowedKey)
				mergedStart = Math.min(mergedStart, windowedKey.windowStart)
				mergedEnd = Math.max(mergedEnd, windowedKey.windowEnd)
				mergedValue = this.reducer(mergedValue, sessionValue)
			}
		}

		const newSessionKey: WindowedKey<K> = { key, windowStart: mergedStart, windowEnd: mergedEnd }
		await store.put(newSessionKey, mergedValue)

		const windowedResult: Windowed<K> = { key, window: { start: mergedStart, end: mergedEnd } }
		const next: StreamRecord<Windowed<K>, V> = { ...record, key: windowedResult, value: mergedValue }
		await this.forward(next)
	}
}

type CleanupState = { lastCleanupStreamTimeMs: number; streamTimeMs: number }

/**
 * Processor node for windowed aggregations.
 *
 * Honours TimeWindows.advanceBy(): when advanceMs < windowSizeMs (hopping
 * windows), each record is aggregated into every overlapping window and one
 * forward is emitted per window. When advanceMs == windowSizeMs (tumbling),
 * each record is assigned to exactly one window.
 */
export class WindowedAggregateNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	private static readonly CLEANUP_INTERVAL_MS = 60_000
	private readonly cleanupState: CleanupState
	private readonly advanceMs: number

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: WindowStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly windowSizeMs: number,
		advanceMs: number = 0,
		cleanupState?: CleanupState
	) {
		super()
		this.advanceMs = advanceMs > 0 ? advanceMs : windowSizeMs
		if (this.advanceMs > this.windowSizeMs) {
			throw new Error(
				`TimeWindows.advanceBy must be <= window size (got advance=${this.advanceMs}, size=${this.windowSizeMs}). ` +
					'Larger advance values would silently drop records that fall in the gaps between windows.'
			)
		}
		this.cleanupState = cleanupState ?? { lastCleanupStreamTimeMs: 0, streamTimeMs: 0 }
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		void worker
		return new WindowedAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.windowSizeMs,
			this.advanceMs,
			this.cleanupState
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

		// Compute the range of window starts containing the record timestamp.
		// For tumbling windows (advance == size) there's exactly one window.
		// For hopping windows (advance < size) every window with
		// windowStart <= timestamp < windowStart + size receives an update.
		const timestamp = Number(record.timestamp)
		const firstWindowStart =
			Math.floor((timestamp - this.windowSizeMs + this.advanceMs) / this.advanceMs) * this.advanceMs
		const lastWindowStart = Math.floor(timestamp / this.advanceMs) * this.advanceMs

		const newAggregates: Array<{ key: WindowedKey<K>; value: A; window: { start: number; end: number } }> = []
		for (let windowStart = firstWindowStart; windowStart <= lastWindowStart; windowStart += this.advanceMs) {
			if (windowStart < 0) continue
			const windowEnd = windowStart + this.windowSizeMs
			const windowedKey: WindowedKey<K> = { key, windowStart, windowEnd }

			const storedAggregate = await store.get(windowedKey)
			const aggregate: A = storedAggregate !== undefined ? storedAggregate : this.initializer()
			const newAggregate = this.aggregator(key, record.value, aggregate)
			await store.put(windowedKey, newAggregate)
			newAggregates.push({ key: windowedKey, value: newAggregate, window: { start: windowStart, end: windowEnd } })
		}

		// Stream time (max record timestamp), not wall clock — so backfill/replay expires identically to live processing.
		this.cleanupState.streamTimeMs = Math.max(this.cleanupState.streamTimeMs, timestamp)
		if (
			this.cleanupState.streamTimeMs - this.cleanupState.lastCleanupStreamTimeMs >
			WindowedAggregateNode.CLEANUP_INTERVAL_MS
		) {
			this.cleanupState.lastCleanupStreamTimeMs = this.cleanupState.streamTimeMs
			await store.expireOldWindows(this.cleanupState.streamTimeMs)
		}

		// Forward one result per window the record landed in.
		for (const entry of newAggregates) {
			const windowedResult: Windowed<K> = { key, window: entry.window }
			const next: StreamRecord<Windowed<K>, A> = { ...record, key: windowedResult, value: entry.value }
			await this.forward(next)
		}
	}
}

/**
 * Processor node for session window aggregations.
 * Sessions are merged when events arrive within the inactivity gap.
 */
export class SessionAggregateNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: SessionStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly merger: (aggregate1: A, aggregate2: A) => A,
		private readonly gapMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		void worker
		return new SessionAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.merger,
			this.gapMs
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
	}
}
