import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import { maybeExpire, StreamTimeTracker } from '@/processors/aggregation.js'
import type { WindowStore } from '@/state.js'

const newCleanupState = (): StreamTimeTracker => new StreamTimeTracker()

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
 * Processor node for Stream-Stream join (inner join).
 * Stores records in a window store and looks up matching records from the other stream.
 */
export class StreamStreamJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: StreamTimeTracker = newCleanupState()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		return new StreamStreamJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState.attach(worker)
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
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await putRetainingDuplicates(myStore, key, windowStart, windowEnd, value)

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
		await maybeExpire(this.cleanupState, record, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}

/**
 * Processor node for Stream-Stream left join.
 */
export class StreamStreamLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: StreamTimeTracker = newCleanupState()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		return new StreamStreamLeftJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState.attach(worker)
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
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await putRetainingDuplicates(myStore, key, windowStart, windowEnd, value)

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
			const joinedValue = this.joiner(value, null)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		// Stream-time-driven retention: expire both stores so neither grows unbounded
		// (the other stream's node may be idle, so each node expires both).
		await maybeExpire(this.cleanupState, record, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}

/**
 * Processor node for Stream-Stream outer join.
 */
export class StreamStreamOuterJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1 | null, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number,
		private readonly cleanupState: StreamTimeTracker = newCleanupState()
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1, K, VR> {
		return new StreamStreamOuterJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.cleanupState.attach(worker)
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
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		await putRetainingDuplicates(myStore, key, windowStart, windowEnd, value)

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
			const joinedValue = this.joiner(value, null)
			const next: StreamRecord<K, VR> = { ...record, value: joinedValue }
			await this.forward(next)
		}

		// Stream-time-driven retention: expire both stores so neither grows unbounded
		// (the other stream's node may be idle, so each node expires both).
		await maybeExpire(this.cleanupState, record, timestamp, async cutoff => {
			await myStore.expireOldWindows(cutoff)
			await otherStore.expireOldWindows(cutoff)
		})
	}
}
