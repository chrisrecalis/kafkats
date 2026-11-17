/**
 * Session-window count, reduce and aggregate (KS KStreamSessionWindowAggregate; 6.5,
 * `KStreamSessionWindowAggregate.java:176-226`).
 *
 * A record finds the sessions of its key that end at most `gap` before it or start at most `gap` after it (read through
 * the store clock's floor) and merges them with itself into one session. The processor clock is the max record
 * timestamp, persisted under the store's name and advanced before the late check: the record is dropped when the
 * merged session ends before `streamTime - grace - gap`, so a late record falling into a session still open is merged.
 * Unless the merged session is the record's own `[ts, ts]`, every session found is removed and retracted, also one with
 * the merged bounds (BC23); then the merged session is stored and forwarded, without an old value. A `[ts, ts]` session
 * found is updated in place instead, its aggregate the old value (what Kafka Streams' record cache sends,
 * `CachingSessionStore.putAndMaybeForward`). A session's results carry its end as their timestamp (KS). Null keys are
 * dropped; null values reach the aggregator. Results carry the headers of the record (KS forwards session updates
 * without headers). With emit on window close, only the final result of each session is forwarded
 * (`operators/emit-final.ts`).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { scanClosedWindows, type FinalResult } from '@/operators/emit-final.js'
import { MaterializedTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { windowedValueGetter, WindowedAggregateProcessor, type Window } from '@/operators/windowed-processor.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import type { ValueAndTimestamp, WindowedKey } from '@/state.js'
import type { ClockedSessionStore } from '@/state/time-clipping.js'

/**
 * The aggregate of a merged session: the aggregates of the sessions the record joins (by start, then end) merged,
 * then the record's value folded in. A null result deletes the session.
 */
export type SessionMerge<K, V, A> = (key: K, value: V, sessions: readonly A[]) => A | null

/** aggregate() and count(): the initializer, merged with each session's aggregate, then the aggregator (KS order). */
export function sessionAggregateWith<K, V, A>(
	initializer: () => A,
	aggregator: (key: K, value: V, aggregate: A) => A,
	merger: (key: K, aggregate1: A, aggregate2: A) => A
): SessionMerge<K, V, A> {
	return (key, value, sessions) =>
		aggregator(
			key,
			value,
			sessions.reduce((merged, session) => merger(key, merged, session), initializer())
		)
}

/**
 * reduce(): the reducer over the sessions' aggregates, then over the record's value, which may be null (the result is
 * null when no session was found: no session is kept).
 */
export function sessionReduceWith<V>(
	reducer: (aggregate: V, value: V | null) => V
): SessionMerge<unknown, V | null, V> {
	// Only the record's value can be null, and it comes last: `merged` never is.
	return (_key, value, sessions) => [...sessions, value].reduce((merged, next) => reducer(merged!, next))
}

export interface SessionWindowSpec {
	readonly gapMs: number
	/** How long after its end plus the gap a session still takes records. */
	readonly graceMs: number
}

/** A session's aggregate as a result: at the session's end (KS). */
const atEnd = <A>(value: A, window: Window): ValueAndTimestamp<A> => ({ value, timestamp: window.end })

export class SessionWindowAggregate<K, V, A> extends MaterializedTableNode<Windowed<K>, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, Windowed<K>, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<Windowed<K>, A>

	constructor(storeName: string, windows: SessionWindowSpec, merge: SessionMerge<K, V, A>, emitFinal: boolean) {
		super()
		this.supplier = () =>
			new SessionWindowAggregateProcessor(storeName, windows, merge, emitFinal, this.sendOldValues)
		this.valueGetter = windowedValueGetter<K, A, A>(storeName, atEnd)
	}
}

class SessionWindowAggregateProcessor<K, V, A> extends WindowedAggregateProcessor<K, V, A, ClockedSessionStore<K, A>> {
	constructor(
		storeName: string,
		private readonly windows: SessionWindowSpec,
		private readonly merge: SessionMerge<K, V, A>,
		emitFinal: boolean,
		sendOldValues: () => boolean
	) {
		super(storeName, emitFinal ? { kind: 'session' } : undefined, sendOldValues)
	}

	protected async aggregate({ key, value, timestamp, headers }: StreamRecord<K, V | null>): Promise<number> {
		const { gapMs, graceMs } = this.windows
		// Advanced before the late check (6.0): the record is judged by a clock that includes it.
		const closeTime = this.clock.advance(timestamp) - graceMs - gapMs
		// In (start, end) order (store contract), the order the merge sees them in.
		const sessions: [WindowedKey<K>, A][] = []
		for await (const session of this.store.findSessions(key, timestamp - gapMs, timestamp + gapMs)) {
			sessions.push(session)
		}
		const merged = sessions.reduce<Window>(
			(window, [session]) => ({
				start: Math.min(window.start, session.windowStart),
				end: Math.max(window.end, session.windowEnd),
			}),
			{ start: timestamp, end: timestamp }
		)
		if (merged.end < closeTime) {
			this.context.recordDropped('expired-window', { store: this.storeName, window: merged, closeTime })
		} else {
			await this.update(key, value as V, sessions, merged, headers)
		}
		return closeTime
	}

	private async update(
		key: K,
		value: V,
		sessions: readonly [WindowedKey<K>, A][],
		merged: Window,
		headers: RecordHeaders
	): Promise<void> {
		const sendOldValues = this.sendOldValues()
		// A merged session of one point is the record's own [ts, ts]: a session found has its bounds and is updated.
		const inPlace = merged.start === merged.end
		if (!inPlace) {
			for (const [session, aggregate] of sessions) {
				await this.store.delete(session)
				const window = { start: session.windowStart, end: session.windowEnd }
				const retraction = { newValue: null, oldValue: sendOldValues ? aggregate : null }
				await this.forward(key, window, retraction, window.end, headers)
			}
		}
		const aggregate = this.merge(
			key,
			value,
			sessions.map(([, sessionAggregate]) => sessionAggregate)
		)
		const mergedKey: WindowedKey<K> = { key, windowStart: merged.start, windowEnd: merged.end }
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(mergedKey)
		} else {
			await this.store.put(mergedKey, aggregate)
		}
		const oldValue = inPlace && sendOldValues ? (sessions[0]?.[1] ?? null) : null
		await this.forward(key, merged, { newValue: aggregate ?? null, oldValue }, merged.end, headers)
	}

	/**
	 * The sessions of every key that ended since the last emission: ends in `[lowerEnd, closeTime - 1]` (KS
	 * `emitRangeLowerBound`, `emitRangeUpperBound`, `shouldRangeFetch`).
	 */
	protected closedWindows(lowerEnd: number, closeTime: number): Promise<FinalResult<K, A>[]> | undefined {
		const upper = closeTime - 1
		return upper < lowerEnd ? undefined : scanClosedWindows(this.store.unclipped, lowerEnd, upper, atEnd)
	}
}
