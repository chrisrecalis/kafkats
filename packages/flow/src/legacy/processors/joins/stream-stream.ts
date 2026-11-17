import {
	Processor,
	requireStore,
	type StoreRef,
	type StreamRecord,
	type WorkerContext,
} from '@/legacy/processors/base.js'
import { StreamTimeTracker } from '@/legacy/processors/stream-time.js'
import type { WindowStore } from '@/state.js'

/**
 * Buffer a join record without overwriting an earlier record of the same key and timestamp.
 *
 * The window store is keyed by (key, windowStart, windowEnd), so two records sharing a key and
 * timestamp would collide and the first would silently lose all of its future matches. Lookups
 * only use windowStart, so windowEnd is bumped to the first free slot to keep duplicates distinct
 * (the store format and changelog layout stay unchanged).
 */
async function putRetainingDuplicates<K, V>(
	store: WindowStore<K, V>,
	key: K,
	windowStart: number,
	windowEnd: number,
	value: V
): Promise<void> {
	let end = windowEnd
	while ((await store.get({ key, windowStart, windowEnd: end })) !== undefined) {
		end += 1
	}
	await store.put({ key, windowStart, windowEnd: end }, value)
}

/**
 * One side of a stream-stream join: buffers the record in this side's window store and joins it
 * with every record of the other side's store within the join window. A record without a match
 * is passed to `unmatched` when given (left/outer joins), otherwise dropped (inner join).
 */
export class StreamStreamJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: StoreRef<WindowStore<K, V1>>,
		private readonly otherStoreRef: StoreRef<WindowStore<K, V2>>,
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly joinWindowMs: number,
		private readonly unmatched?: (value: V1) => VR,
		private readonly streamTime: StreamTimeTracker = new StreamTimeTracker()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		return new StreamStreamJoinNode(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.unmatched,
			this.streamTime.attach(worker)
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = requireStore(this.myStoreRef, 'stream-stream join window')
		const otherStore = requireStore(this.otherStoreRef, 'stream-stream join window')
		const { key, value } = record
		if (key === null || value === null) return

		const timestamp = Number(record.timestamp)
		await putRetainingDuplicates(myStore, key, timestamp, timestamp + this.joinWindowMs, value)

		let hasMatch = false
		for await (const [, otherValue] of otherStore.fetch(
			key,
			timestamp - this.joinWindowMs,
			timestamp + this.joinWindowMs
		)) {
			hasMatch = true
			await this.forward({ ...record, value: this.joiner(value, otherValue) })
		}
		if (!hasMatch && this.unmatched) {
			await this.forward({ ...record, value: this.unmatched(value) })
		}

		// Expire both stores so neither grows unbounded (the other stream's node may be idle, so
		// each node expires both).
		await this.streamTime.advance(record, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}

/** Left side of a stream-stream left join: an unmatched record is joined with null. */
export class StreamStreamLeftJoinNode<K, V1, V2, VR> extends StreamStreamJoinNode<K, V1, V2, VR> {
	constructor(
		myStoreRef: StoreRef<WindowStore<K, V1>>,
		otherStoreRef: StoreRef<WindowStore<K, V2>>,
		joiner: (value: V1, otherValue: V2 | null) => VR,
		joinWindowMs: number
	) {
		super(myStoreRef, otherStoreRef, joiner, joinWindowMs, value => joiner(value, null))
	}
}

/**
 * Either side of a stream-stream outer join. Each side emits its unmatched records like a left
 * join does; the outer join is the pair of them with the joiner's arguments swapped on one side.
 */
export class StreamStreamOuterJoinNode<K, V1, V2, VR> extends StreamStreamLeftJoinNode<K, V1, V2, VR> {}
