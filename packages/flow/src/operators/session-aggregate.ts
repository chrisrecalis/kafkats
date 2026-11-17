/**
 * Session-window count, reduce and aggregate (KS KStreamSessionWindowAggregate; 6.5), ported with today's semantics:
 * a record merges every session of its key that ends at most `gap` before it or starts at most `gap` after it; the
 * sessions merged into a different window are retracted with tombstones, then the merged session is emitted; results
 * carry the timestamp of the record. A record that joins no session is dropped once it is older than the processor
 * clock (record timestamps, persisted under the store's name) minus the retention of 24 gaps. Stage 3 switches to grace
 * periods, retracts every merged session and emits at the session end (WP3.4).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { aggregatable } from '@/operators/kstream-aggregate.js'
import type { KTableNode, KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { windowedValueGetter, WINDOW_STORE_RETENTION_MULTIPLIER } from '@/operators/window-aggregate.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { SessionStore, WindowedKey } from '@/state.js'

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

/** reduce(): the reducer over the sessions' aggregates, then over the record's value. */
export function sessionReduceWith<V>(reducer: (aggregate: V, value: V) => V): SessionMerge<unknown, V, V> {
	return (_key, value, sessions) => [...sessions, value].reduce((merged, next) => reducer(merged, next))
}

export class SessionWindowAggregate<K, V, A> implements KTableNode<Windowed<K>, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, Windowed<K>, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<Windowed<K>, A>
	private sendOldValues = false

	constructor(storeName: string, gapMs: number, merge: SessionMerge<K, V, A>) {
		this.supplier = () => new SessionWindowAggregateProcessor(storeName, gapMs, merge, () => this.sendOldValues)
		// The session's end: the timestamp of its last record (KS).
		this.valueGetter = windowedValueGetter(storeName, window => window.end)
	}

	/** Always materialized: the store has the old values. */
	enableSendingOldValues(): boolean {
		this.sendOldValues = true
		return true
	}
}

class SessionWindowAggregateProcessor<K, V, A> implements Processor<K | null, V | null, Windowed<K>, Change<A>> {
	private context!: InternalProcessorContext<Windowed<K>, Change<A>>
	private store!: SessionStore<K, A>
	private clock!: ObservedStreamTime

	constructor(
		private readonly storeName: string,
		private readonly gapMs: number,
		private readonly merge: SessionMerge<K, V, A>,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<Windowed<K>, Change<A>>): void {
		this.context = context as InternalProcessorContext<Windowed<K>, Change<A>>
		this.store = context.getStateStore(this.storeName)
		this.clock = new ObservedStreamTime(this.context, this.storeName)
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		if (!aggregatable(this.context, this.storeName, record)) {
			return
		}
		const { key, value, timestamp, headers } = record
		// Advanced before the late check (6.0): a record that moves the clock is never late.
		const streamTime = this.clock.advance(timestamp)
		// In (start, end) order (store contract), the order the merge sees them in.
		const sessions: [WindowedKey<K>, A][] = []
		for await (const session of this.store.findSessions(key, timestamp - this.gapMs, timestamp + this.gapMs)) {
			sessions.push(session)
		}
		// A record that joins a live session is merged however old it is; one that would start a session already
		// past retention is dropped, so an expired session is not re-created.
		if (sessions.length === 0 && timestamp < streamTime - this.gapMs * WINDOW_STORE_RETENTION_MULTIPLIER) {
			this.context.recordDropped('expired-window', { store: this.storeName })
			return
		}

		let start = timestamp
		let end = timestamp
		for (const [session] of sessions) {
			start = Math.min(start, session.windowStart)
			end = Math.max(end, session.windowEnd)
			await this.store.delete(session)
		}
		const aggregate = this.merge(
			key,
			value,
			sessions.map(([, sessionAggregate]) => sessionAggregate)
		)
		const merged: WindowedKey<K> = { key, windowStart: start, windowEnd: end }
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(merged)
		} else {
			await this.store.put(merged, aggregate)
		}

		// Retract the sessions merged into a different window, so that no table keyed by the window keeps them; a
		// session with the merged window is updated in place.
		const sendOldValues = this.sendOldValues()
		let replaced: A | null = null
		for (const [session, sessionAggregate] of sessions) {
			if (session.windowStart === start && session.windowEnd === end) {
				replaced = sessionAggregate
				continue
			}
			const window = { start: session.windowStart, end: session.windowEnd }
			const retraction = { newValue: null, oldValue: sendOldValues ? sessionAggregate : null }
			await this.context.forward(new StreamRecord({ key, window }, retraction, timestamp, headers))
		}
		const change = { newValue: aggregate ?? null, oldValue: sendOldValues ? replaced : null }
		await this.context.forward(new StreamRecord({ key, window: { start, end } }, change, timestamp, headers))

		if (this.clock.expiryDue()) {
			await this.store.expireOldSessions(streamTime)
		}
	}
}
