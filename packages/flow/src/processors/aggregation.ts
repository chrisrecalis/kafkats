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
 *
 * Reduce and aggregate differ only in how a new value is folded into the current aggregate, so
 * each node kind is implemented once and the reduce/aggregate classes only supply that fold.
 */

import { Processor, requireStore, type StoreRef, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import { StreamTimeTracker } from '@/processors/stream-time.js'
import type { KeyValueStore, WindowStore, SessionStore, WindowedKey } from '@/state.js'
import type { Windowed } from '@/types.js'

/** Folds a record's value into the current aggregate (undefined when there is none yet). */
type Update<K, V, A> = (key: K, value: V, current: A | undefined) => A

const reduceWith =
	<V>(reducer: (aggregate: V, value: V) => V): Update<unknown, V, V> =>
	(_key, value, current) =>
		current === undefined ? value : reducer(current, value)

const aggregateWith =
	<K, V, A>(initializer: () => A, aggregator: (key: K, value: V, aggregate: A) => A): Update<K, V, A> =>
	(key, value, current) =>
		aggregator(key, value, current !== undefined ? current : initializer())

// Window/session stores retain state for this multiple of the window size / session gap
// (see grouped.ts store creation). The aggregation nodes reuse the same factor to detect a
// late record whose window/session has already passed retention, so they don't resurrect it.
export const WINDOW_STORE_RETENTION_MULTIPLIER = 24

class KeyValueAggregationNode<K, V, A> extends Processor<K, V, K, A> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: StoreRef<KeyValueStore<K, A>>,
		private readonly update: Update<K, V, A>
	) {
		super()
	}

	clone(): Processor<K, V, K, A> {
		return new KeyValueAggregationNode(this.storeName, this.storeRef, this.update)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const { key, value } = record
		// Null keys and tombstones do not take part in aggregations.
		if (key === null || value === null) return

		const aggregate = this.update(key, value, await store.get(key))
		await store.put(key, aggregate)
		await this.forward({ ...record, key, value: aggregate })
	}
}

/** count() and aggregate() of a grouped stream. */
export class AggregateNode<K, V, A> extends KeyValueAggregationNode<K, V, A> {
	constructor(
		storeName: string,
		storeRef: StoreRef<KeyValueStore<K, A>>,
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A
	) {
		super(storeName, storeRef, aggregateWith(initializer, aggregator))
	}
}

/** reduce() of a grouped stream. */
export class ReduceNode<K, V> extends KeyValueAggregationNode<K, V, V> {
	constructor(storeName: string, storeRef: StoreRef<KeyValueStore<K, V>>, reducer: (aggregate: V, value: V) => V) {
		super(storeName, storeRef, reduceWith(reducer))
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
 * Newest window start in a window store. Stream time of a windowed aggregation is counted in these
 * (see WindowedAggregationNode.process), so this is the stream time that produced the store.
 */
async function newestWindowStart(store: { all(): AsyncIterable<[WindowedKey<unknown>, unknown]> }) {
	let newest = 0
	for await (const [windowedKey] of store.all()) {
		if (windowedKey.windowStart > newest) newest = windowedKey.windowStart
	}
	return newest
}

/** Newest session end in a session store: the highest record timestamp that produced it. */
async function newestSessionEnd(store: { all(): AsyncIterable<[WindowedKey<unknown>, unknown]> }) {
	let newest = 0
	for await (const [windowedKey] of store.all()) {
		if (windowedKey.windowEnd > newest) newest = windowedKey.windowEnd
	}
	return newest
}

/** Time-window aggregation. Honours TimeWindows.advanceBy() (hopping windows). */
class WindowedAggregationNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	private readonly advanceMs: number

	constructor(
		private readonly storeName: string,
		private readonly storeRef: StoreRef<WindowStore<K, A>>,
		private readonly update: Update<K, V, A>,
		private readonly windowSizeMs: number,
		advanceMs: number,
		private readonly streamTime: StreamTimeTracker = new StreamTimeTracker()
	) {
		super()
		this.advanceMs = resolveAdvanceMs(windowSizeMs, advanceMs)
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		return new WindowedAggregationNode(
			this.storeName,
			this.storeRef,
			this.update,
			this.windowSizeMs,
			this.advanceMs,
			this.streamTime.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const { key, value } = record
		if (key === null || value === null) return

		const timestamp = Number(record.timestamp)
		await this.streamTime.ensureClock(record, () => newestWindowStart(store))
		const retentionCutoff =
			this.streamTime.streamTime(record) - this.windowSizeMs * WINDOW_STORE_RETENTION_MULTIPLIER
		let newestStart = 0
		for (const { start, end } of windowStartsFor(timestamp, this.windowSizeMs, this.advanceMs)) {
			newestStart = start
			// Drop a late record whose window has already passed retention: re-creating the
			// window from scratch emits a wrong partial result that contradicts the
			// already-finalized (and possibly expired) window.
			if (end < retentionCutoff) continue
			const windowedKey: WindowedKey<K> = { key, windowStart: start, windowEnd: end }

			const aggregate = this.update(key, value, await store.get(windowedKey))
			await store.put(windowedKey, aggregate)
			await this.forward({ ...record, key: { key, window: { start, end } }, value: aggregate })
		}

		// Stream time advances by window, not by timestamp: it is the start of the newest window a
		// record has opened. That is exactly what the store shows after a restart, so a clock that
		// is rebuilt from it gives every record the same late-or-not answer as before. With the
		// timestamp itself, the rebuilt clock would be up to one window behind and accept a record
		// that had already been dropped as late.
		await this.streamTime.advance(record, newestStart, t => store.expireOldWindows(t))
	}
}

export class WindowedAggregateNode<K, V, A> extends WindowedAggregationNode<K, V, A> {
	constructor(
		storeName: string,
		storeRef: StoreRef<WindowStore<K, A>>,
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		windowSizeMs: number,
		advanceMs: number = 0
	) {
		super(storeName, storeRef, aggregateWith(initializer, aggregator), windowSizeMs, advanceMs)
	}
}

export class WindowedReduceNode<K, V> extends WindowedAggregationNode<K, V, V> {
	constructor(
		storeName: string,
		storeRef: StoreRef<WindowStore<K, V>>,
		reducer: (aggregate: V, value: V) => V,
		windowSizeMs: number,
		advanceMs: number = 0
	) {
		super(storeName, storeRef, reduceWith(reducer), windowSizeMs, advanceMs)
	}
}

/**
 * Session-window aggregation. Sessions are merged when events arrive within the inactivity gap.
 * `merge` combines the record's value with the values of the sessions it bridges (oldest first).
 */
class SessionAggregationNode<K, V, A> extends Processor<K, V, Windowed<K>, A> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: StoreRef<SessionStore<K, A>>,
		private readonly merge: (key: K, value: V, sessions: A[]) => A,
		private readonly gapMs: number,
		private readonly streamTime: StreamTimeTracker = new StreamTimeTracker()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, Windowed<K>, A> {
		return new SessionAggregationNode(
			this.storeName,
			this.storeRef,
			this.merge,
			this.gapMs,
			this.streamTime.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = requireStore(this.storeRef, this.storeName)
		const { key, value } = record
		if (key === null || value === null) return

		const timestamp = Number(record.timestamp)
		await this.streamTime.ensureClock(record, () => newestSessionEnd(store))

		// A session overlaps if the new event's timestamp is within gap of the session's boundaries.
		const sessions: Array<{ windowedKey: WindowedKey<K>; value: A }> = []
		for await (const [windowedKey, sessionValue] of store.findSessions(
			key,
			timestamp - this.gapMs,
			timestamp + this.gapMs
		)) {
			sessions.push({ windowedKey, value: sessionValue })
		}

		// Drop a late record that has no live session to merge into and whose own session
		// would already be past retention — otherwise an expired session is resurrected.
		const retentionCutoff = this.streamTime.streamTime(record) - this.gapMs * WINDOW_STORE_RETENTION_MULTIPLIER
		if (sessions.length === 0 && timestamp < retentionCutoff) {
			return
		}

		// Merge in event-time order (store iteration order is not guaranteed); the new record is
		// applied last, which non-commutative reducers rely on.
		sessions.sort(
			(a, b) =>
				a.windowedKey.windowStart - b.windowedKey.windowStart ||
				a.windowedKey.windowEnd - b.windowedKey.windowEnd
		)

		let mergedStart = timestamp
		let mergedEnd = timestamp
		for (const { windowedKey } of sessions) {
			await store.delete(windowedKey)
			mergedStart = Math.min(mergedStart, windowedKey.windowStart)
			mergedEnd = Math.max(mergedEnd, windowedKey.windowEnd)
		}
		const mergedValue = this.merge(
			key,
			value,
			sessions.map(session => session.value)
		)
		await store.put({ key, windowStart: mergedStart, windowEnd: mergedEnd }, mergedValue)

		// Retract each merged-away session whose window differs from the new merged window, so a
		// downstream table/consumer keyed on the windowed key doesn't retain the absorbed sessions.
		for (const { windowedKey } of sessions) {
			if (windowedKey.windowStart === mergedStart && windowedKey.windowEnd === mergedEnd) continue
			const removed: Windowed<K> = { key, window: { start: windowedKey.windowStart, end: windowedKey.windowEnd } }
			await this.forward({ ...record, key: removed, value: null })
		}

		await this.forward({
			...record,
			key: { key, window: { start: mergedStart, end: mergedEnd } },
			value: mergedValue,
		})

		await this.streamTime.advance(record, timestamp, t => store.expireOldSessions(t))
	}
}

export class SessionAggregateNode<K, V, A> extends SessionAggregationNode<K, V, A> {
	constructor(
		storeName: string,
		storeRef: StoreRef<SessionStore<K, A>>,
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		merger: (aggregate1: A, aggregate2: A) => A,
		gapMs: number
	) {
		super(
			storeName,
			storeRef,
			(key, value, sessions) =>
				aggregator(
					key,
					value,
					sessions.reduce((merged, session) => merger(merged, session), initializer())
				),
			gapMs
		)
	}
}

export class SessionReduceNode<K, V> extends SessionAggregationNode<K, V, V> {
	constructor(
		storeName: string,
		storeRef: StoreRef<SessionStore<K, V>>,
		reducer: (aggregate: V, value: V) => V,
		gapMs: number
	) {
		super(
			storeName,
			storeRef,
			(_key, value, sessions) => [...sessions, value].reduce((merged, next) => reducer(merged, next)),
			gapMs
		)
	}
}
