import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import { maybeExpire, type CleanupState } from '@/processors/aggregation.js'
import type { WindowStore } from '@/state.js'

const newCleanupState = (): CleanupState => ({ lastCleanupStreamTimeMs: 0, streamTimeMs: 0 })

/**
 * Insert a record into a join buffer store, retaining duplicates: two records with the same key
 * AND timestamp must both survive (Kafka Streams stores join records with retainDuplicates plus a
 * sequence number). The store's key/value codecs are fixed by the caller, so the sequence is
 * folded into windowEnd: the Nth duplicate for a (key, timestamp) gets
 * windowEnd = timestamp + joinWindowMs + N. Join lookups range-scan by windowStart, so all
 * duplicates are returned; expiry compares windowEnd, where the +N millisecond skew is negligible.
 * Works with any WindowStore implementation unchanged.
 */
async function putRetainingDuplicates<K, V>(
	store: WindowStore<K, V>,
	key: K,
	timestamp: number,
	joinWindowMs: number,
	value: V
): Promise<void> {
	let seq = 0
	for await (const entry of store.fetch(key, timestamp, timestamp)) {
		void entry
		seq++
	}
	await store.put({ key, windowStart: timestamp, windowEnd: timestamp + joinWindowMs + seq }, value)
}

/**
 * A record buffered by a left/outer join node because it has not (yet) found a join partner.
 * If no partner arrives before the join window closes, the owning node emits the null-padded
 * result exactly once (modern Kafka Streams / KIP-633 semantics — no eager spurious results).
 */
type PendingJoinRecord<K, V> = { record: StreamRecord<K, V>; key: K; value: V; timestampMs: number }

/**
 * Registry of pending-buffer flushers keyed by the join-buffer storeRef object. Both nodes of a
 * join share the same two storeRef objects (with sides swapped), so either side can drive the
 * other side's window-close flush as its stream time advances — even when that side's input is
 * idle. Flushing runs BEFORE store expiry in every process() call, which guarantees that a
 * buffered record's potential partners are still in the other store when matched-ness is checked.
 */
const pendingFlushers = new WeakMap<object, { flush: (streamTimeMs: number) => Promise<void> }>()

async function flushPendingFor(refs: object[], streamTimeMs: number): Promise<void> {
	for (const ref of refs) {
		const flusher = pendingFlushers.get(ref)
		if (flusher) {
			await flusher.flush(streamTimeMs)
		}
	}
}

/**
 * Processor node for Stream-Stream join (inner join).
 * Stores records in a window store and looks up matching records from the other stream.
 */
export class StreamStreamJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: CleanupState = newCleanupState()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)

		// Close any left/outer join windows this record's stream time has passed — on both sides —
		// before expiring stores (see pendingFlushers). In a leftJoin the inner node on the
		// secondary stream drives the primary side's close this way.
		await flushPendingFor([this.myStoreRef, this.otherStoreRef], timestamp)

		await putRetainingDuplicates(myStore, key, timestamp, this.joinWindowMs, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		// Stream-time-driven retention: expire both stores so neither grows unbounded
		// (the other stream's node may be idle, so each node expires both).
		await maybeExpire(this.cleanupState, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}

/**
 * Processor node for Stream-Stream left join.
 *
 * A record with no match is buffered; the null-padded result is emitted only when the join window
 * closes without a match (stream-time driven), never eagerly — otherwise downstream would see both
 * a spurious joiner(value, null) AND the real result when the match arrives later in the window.
 */
export class StreamStreamLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: CleanupState = newCleanupState(),
		private readonly pending: Array<PendingJoinRecord<K, V1>> = []
	) {
		super()
		pendingFlushers.set(this.myStoreRef, { flush: streamTimeMs => this.flushClosedWindows(streamTimeMs) })
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamLeftJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState,
			this.pending
		)
	}

	/** Emit joiner(value, null) for buffered records whose window closed without a match. */
	private async flushClosedWindows(streamTimeMs: number): Promise<void> {
		const otherStore = this.otherStoreRef.store
		if (!otherStore) return

		for (let i = 0; i < this.pending.length; ) {
			const entry = this.pending[i]!
			if (streamTimeMs <= entry.timestampMs + this.joinWindowMs) {
				i++
				continue
			}
			this.pending.splice(i, 1)

			// A partner in the other store means the other side's node already emitted the real
			// join result — the buffered record was matched, so no null-padded result is due.
			let matched = false
			for await (const partner of otherStore.fetch(
				entry.key,
				entry.timestampMs - this.joinWindowMs,
				entry.timestampMs + this.joinWindowMs
			)) {
				void partner
				matched = true
				break
			}
			if (!matched) {
				await this.forward({ ...entry.record, value: this.joiner(entry.value, null) })
			}
		}
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)

		// Close overdue windows (both sides) before processing this record and before expiry.
		await flushPendingFor([this.myStoreRef, this.otherStoreRef], timestamp)

		await putRetainingDuplicates(myStore, key, timestamp, this.joinWindowMs, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		if (!hasMatch) {
			// Do NOT emit joiner(value, null) eagerly — buffer until the window closes.
			this.pending.push({ record, key, value, timestampMs: timestamp })
		}

		// Stream-time-driven retention: expire both stores so neither grows unbounded
		// (the other stream's node may be idle, so each node expires both).
		await maybeExpire(this.cleanupState, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}

/**
 * Processor node for Stream-Stream outer join.
 *
 * Each side buffers its unmatched records and emits the null-padded result only when the join
 * window closes without a match (see StreamStreamLeftJoinNode).
 */
export class StreamStreamOuterJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1 | null, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: CleanupState = newCleanupState(),
		private readonly pending: Array<PendingJoinRecord<K, V1>> = []
	) {
		super()
		pendingFlushers.set(this.myStoreRef, { flush: streamTimeMs => this.flushClosedWindows(streamTimeMs) })
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		void worker
		return new StreamStreamOuterJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState,
			this.pending
		)
	}

	/** Emit joiner(value, null) for buffered records whose window closed without a match. */
	private async flushClosedWindows(streamTimeMs: number): Promise<void> {
		const otherStore = this.otherStoreRef.store
		if (!otherStore) return

		for (let i = 0; i < this.pending.length; ) {
			const entry = this.pending[i]!
			if (streamTimeMs <= entry.timestampMs + this.joinWindowMs) {
				i++
				continue
			}
			this.pending.splice(i, 1)

			let matched = false
			for await (const partner of otherStore.fetch(
				entry.key,
				entry.timestampMs - this.joinWindowMs,
				entry.timestampMs + this.joinWindowMs
			)) {
				void partner
				matched = true
				break
			}
			if (!matched) {
				await this.forward({ ...entry.record, value: this.joiner(entry.value, null) })
			}
		}
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) return

		const value = record.value
		if (value === null) return

		const timestamp = Number(record.timestamp)

		// Close overdue windows (both sides) before processing this record and before expiry.
		await flushPendingFor([this.myStoreRef, this.otherStoreRef], timestamp)

		await putRetainingDuplicates(myStore, key, timestamp, this.joinWindowMs, value)

		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			const joinedValue = this.joiner(value, otherValue)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		if (!hasMatch) {
			// Do NOT emit joiner(value, null) eagerly — buffer until the window closes.
			this.pending.push({ record, key, value, timestampMs: timestamp })
		}

		// Stream-time-driven retention: expire both stores so neither grows unbounded
		// (the other stream's node may be idle, so each node expires both).
		await maybeExpire(this.cleanupState, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}
