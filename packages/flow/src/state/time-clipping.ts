/**
 * Store clocks and the clipping layer of window and session(-layout) stores (6.2): Kafka Streams' segmented stores
 * without segments (`AbstractRocksDBSegmentedBytesStore.java:126-128,240-279`, `InMemorySessionStore.java:202-205,284`).
 *
 * A store's clock is the max key time its processor wrote. Reads return only the entries whose key time is at least
 * `time - retention + 1`, so an entry past the store's retention is absent whether or not an expiry pass deleted it yet,
 * and whether it was written or restored: results never depend on the expiry cadence (W18). Expiry deletes the entries
 * below that floor, but never one the next emit-on-window-close scan still has to read (KF1), and locally only (5.3).
 *
 * The key time of an entry is its window end. Kafka Streams keys time and sliding windows by their start, but every
 * window of such a store has the same size (end = start + size), so its clock, floor and decisions are the same shifted
 * by the size; with ends, one rule covers windows, sessions and join buffers (`[ts, ts]`), and a restore finds the key
 * time of a changelog record in its key.
 */
import type { ProcessorMetadata } from '@/processor/api.js'
import type { KeyValueStore, SessionStore, WindowedKey, WindowStore } from '@/state.js'
import type { EmitFinalWindows } from '@/topology/nodes.js'

/** The key time of a store entry: its window end. */
export const keyTime = (key: WindowedKey<unknown>): number => key.windowEnd

/**
 * The clock of a window or session(-layout) store of one task: the clipping layer of the store's stack advances it,
 * persisting it as `<store>.observed` in the task's processor metadata (so it is committed with the offsets); the task
 * seeds it when it starts running (4.4).
 */
export interface StoreClock {
	/** Max key time written; -1 unknown. */
	readonly time: number
	/** Processor writes through the clipping layer only (restore writes go to the provider store). */
	advance(keyTime: number): void
	/** `time = max(time, seed)`, when the task starts running. */
	seed(time: number): void
	/** Lowest key time a read may return: `time - retention + 1`. */
	readonly readFloor: number
	/** Expiry deletes below this: the read floor, or the next emit-on-window-close scan's lower bound if lower. */
	readonly expiryCutoff: number
}

/** The store clock of one store of one task, persisted in the task's processor metadata. */
export class TaskStoreClock implements StoreClock {
	private current = -1
	private readonly observedKey: string
	private readonly emitKey: string

	constructor(
		private readonly metadata: ProcessorMetadata,
		store: string,
		private readonly retentionMs: number,
		private readonly emitFinal?: EmitFinalWindows
	) {
		this.observedKey = `${store}.observed`
		this.emitKey = `${store}.emit`
	}

	get time(): number {
		return this.current
	}

	/** An increase is written to the metadata, which marks the task for a commit. */
	advance(keyTime: number): void {
		if (keyTime > this.current) {
			this.current = keyTime
			this.metadata.put(this.observedKey, keyTime)
		}
	}

	/** Not a change to commit: the seed comes from the commits and the restored store. */
	seed(time: number): void {
		this.current = Math.max(this.current, time)
	}

	/**
	 * A retention of 0 (sessions of gap and grace 0, join windows of 0) still keeps the clock's own key time readable,
	 * where Kafka Streams reads nothing: records of one timestamp merge into one session and join each other.
	 */
	get readFloor(): number {
		return this.current - Math.max(this.retentionMs, 1) + 1
	}

	get expiryCutoff(): number {
		if (this.emitFinal === undefined) {
			return this.readFloor
		}
		return Math.min(this.readFloor, emitFinalBound(this.emitFinal, this.metadata.get(this.emitKey) ?? -1))
	}
}

/**
 * The lowest key time the next emit-on-window-close scan reads (6.8), from the close time of the last scan (`last`, -1
 * before the first): its lower bound on window starts plus the window size, or on session ends.
 */
function emitFinalBound(windows: EmitFinalWindows, last: number): number {
	if (windows.kind === 'session') {
		return last === -1 ? 0 : last
	}
	if (last === -1) {
		return windows.sizeMs
	}
	const lowerStart = Math.max(0, last - windows.sizeMs) + (windows.kind === 'time' ? 1 : 0)
	return lowerStart + windows.sizeMs
}

type KeyValueReads = 'name' | 'get' | 'all' | 'range' | 'approximateNumEntries'
/** The reads of a window store. */
export type WindowStoreReads<K, V> = Pick<WindowStore<K, V>, KeyValueReads | 'fetch' | 'fetchAll' | 'fetchRange'>
/** The reads of a session store. */
export type SessionStoreReads<K, V> = Pick<SessionStore<K, V>, KeyValueReads | 'findSessions'>

/**
 * The top of the stack of a store with a clock: what its processor gets. Writes advance the clock once the layer below
 * took them; reads are clipped at the clock's read floor; the clock, not the caller, decides what `expire()` deletes.
 */
abstract class ClockedStore<K, V, S extends KeyValueStore<WindowedKey<K>, V>> implements KeyValueStore<
	WindowedKey<K>,
	V
> {
	constructor(
		protected readonly inner: S,
		protected readonly clock: StoreClock,
		/** The retention the layers below expire by (the builder's): they delete the windows ending before `t - retention`. */
		protected readonly retentionMs: number
	) {}

	get name(): string {
		return this.inner.name
	}

	get(key: WindowedKey<K>): Promise<V | undefined> {
		return keyTime(key) < this.clock.readFloor ? Promise.resolve(undefined) : this.inner.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await this.inner.put(key, value)
		this.clock.advance(keyTime(key))
	}

	delete(key: WindowedKey<K>): Promise<void> {
		return this.inner.delete(key)
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return this.clip(this.inner.all())
	}

	range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		return this.clip(this.inner.range(from, to))
	}

	/** Counts the entries below the read floor that no expiry deleted yet too. */
	approximateNumEntries(): Promise<number> {
		return this.inner.approximateNumEntries()
	}

	init(): Promise<void> {
		return this.inner.init()
	}

	flush(): Promise<void> {
		return this.inner.flush()
	}

	close(): Promise<void> {
		return this.inner.close()
	}

	/**
	 * Deletes every entry below the clock's expiry cutoff from this task's store, writing no tombstones: the changelog's
	 * retention drops them there, and after a restore clipping hides them until the next expiry (5.3).
	 */
	abstract expire(): Promise<void>

	/**
	 * This layer with the same clock over `wrap(inner)`: a handle on a clocked store (state/handles.ts) wraps the stack
	 * below and keeps the clipping layer, and its expiry, on top.
	 */
	abstract over(wrap: (inner: S) => S): ClockedStore<K, V, S>

	/** The time the layers below expire at, so that exactly the entries below the expiry cutoff go. */
	protected get expiryTime(): number {
		return this.clock.expiryCutoff + this.retentionMs
	}

	protected async *clip(entries: AsyncIterable<[WindowedKey<K>, V]>): AsyncIterable<[WindowedKey<K>, V]> {
		const floor = this.clock.readFloor
		for await (const entry of entries) {
			if (keyTime(entry[0]) >= floor) yield entry
		}
	}
}

export class ClockedWindowStore<K, V>
	extends ClockedStore<K, V, WindowStore<K, V>>
	implements Omit<WindowStore<K, V>, 'expireOldWindows'>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.clip(this.inner.fetch(key, timeFrom, timeTo))
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.clip(this.inner.fetchAll(timeFrom, timeTo))
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.clip(this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo))
	}

	/** Reads below the read floor too: the scans of emit on window close, bounded by their own progress (6.8). */
	get unclipped(): WindowStoreReads<K, V> {
		return this.inner
	}

	async expire(): Promise<void> {
		await this.inner.expireOldWindows(this.expiryTime)
	}

	over(wrap: (inner: WindowStore<K, V>) => WindowStore<K, V>): ClockedWindowStore<K, V> {
		return new ClockedWindowStore(wrap(this.inner), this.clock, this.retentionMs)
	}
}

export class ClockedSessionStore<K, V>
	extends ClockedStore<K, V, SessionStore<K, V>>
	implements Omit<SessionStore<K, V>, 'expireOldSessions'>
{
	/** The sessions of `key` ending at `earliestEnd` (and the read floor) or later and starting at `latestStart` or earlier. */
	findSessions(key: K, earliestEnd: number, latestStart: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.inner.findSessions(key, Math.max(earliestEnd, this.clock.readFloor), latestStart)
	}

	remove(key: K): Promise<void> {
		return this.inner.remove(key)
	}

	/** Reads below the read floor too: the scans of emit on window close, bounded by their own progress (6.8). */
	get unclipped(): SessionStoreReads<K, V> {
		return this.inner
	}

	async expire(): Promise<void> {
		await this.inner.expireOldSessions(this.expiryTime)
	}

	over(wrap: (inner: SessionStore<K, V>) => SessionStore<K, V>): ClockedSessionStore<K, V> {
		return new ClockedSessionStore(wrap(this.inner), this.clock, this.retentionMs)
	}
}
