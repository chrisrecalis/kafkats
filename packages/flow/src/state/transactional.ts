import { windowedKeyCodec } from '@/changelog/writer.js'
import type { Codec } from '@/codec.js'
import { yieldIfDue } from '@/runtime/signals.js'
import type { KeyValueStore, SessionStore, WindowedKey, WindowStore } from '@/state.js'

/** The exactly-once write buffer of one store of one task. */
export interface TransactionalLayer {
	/** Buffered writes. */
	readonly size: number
	/** Applies the buffer to the store below: every write is issued, all settle, the first failure is thrown. */
	commitBuffer(): Promise<void>
	discardBuffer(): void
}

/**
 * A buffered write, as the key and value bytes it had when written (the caller's objects may change afterwards, and
 * the changelog record of the write carries those bytes). A null value is a delete. `key` is decoded on first use.
 */
interface Entry<SK> {
	readonly keyBytes: Buffer
	readonly value: Buffer | null
	key?: SK
}

const BUFFERED = Promise.resolve()

/** Entries handled between two checks for a yield to the event loop. */
const YIELD_EVERY = 1000

const idOf = (keyBytes: Buffer): string => keyBytes.toString('latin1')

/**
 * Makes a task's store transactional under exactly-once: every write of its processors is held in memory (and logged to
 * the changelog inside the transaction) instead of reaching the store below, and reads see the buffered writes. The
 * buffer is applied once the transaction has committed and dropped when it aborts, so the provider store only ever
 * holds committed state: an aborted transaction leaves nothing behind that a retry would apply twice, and after a crash
 * the store is at or behind the committed changelog, which a replay from its checkpoint then converges on.
 *
 * One instance per store per task: a buffer belongs to exactly one task. Restoration writes the provider store directly.
 */
abstract class TransactionalStore<SK, V, S extends KeyValueStore<SK, V>>
	implements KeyValueStore<SK, V>, TransactionalLayer
{
	private buffer = new Map<string, Entry<SK>>()

	constructor(
		protected readonly inner: S,
		protected readonly storeKeyCodec: Codec<SK>,
		protected readonly valueCodec: Codec<V>
	) {}

	get name(): string {
		return this.inner.name
	}

	get size(): number {
		return this.buffer.size
	}

	// get/put/delete are not async: they run for every record and only touch the buffer or pass the call through.
	get(key: SK): Promise<V | undefined> {
		if (this.buffer.size > 0) {
			const entry = this.buffer.get(idOf(this.storeKeyCodec.encode(key)))
			if (entry) {
				return Promise.resolve(entry.value === null ? undefined : this.valueCodec.decode(entry.value))
			}
		}
		return this.inner.get(key)
	}

	put(key: SK, value: V): Promise<void> {
		this.write(this.storeKeyCodec.encode(key), this.valueCodec.encode(value))
		return BUFFERED
	}

	delete(key: SK): Promise<void> {
		this.write(this.storeKeyCodec.encode(key), null)
		return BUFFERED
	}

	all(): AsyncIterable<[SK, V]> {
		return this.scan(this.inner.all(), () => true)
	}

	range(from: SK, to: SK): AsyncIterable<[SK, V]> {
		const fromBytes = this.storeKeyCodec.encode(from)
		const toBytes = this.storeKeyCodec.encode(to)
		return this.scan(
			this.inner.range(from, to),
			keyBytes => Buffer.compare(keyBytes, fromBytes) >= 0 && Buffer.compare(keyBytes, toBytes) <= 0,
			Buffer.compare
		)
	}

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
		this.buffer.clear()
		return this.inner.close()
	}

	async commitBuffer(): Promise<void> {
		const entries = this.buffer
		this.buffer = new Map()
		// Every write is issued before any is awaited: a provider that batches the writes of an event-loop turn (LMDB
		// commits them in one transaction) applies the buffer in a few batches instead of one commit per entry. The
		// keys are distinct, so the order the writes land in is irrelevant. All of them settle before this returns or
		// throws, so none lands afterwards.
		const writes: Promise<void>[] = []
		for (const entry of entries.values()) {
			const write = this.apply(entry)
			// Settled below; marked handled so that a failure is never reported as unhandled while the loop yields.
			write.catch(() => {})
			writes.push(write)
			if (writes.length % YIELD_EVERY === 0) await yieldIfDue()
		}
		for (const result of await Promise.allSettled(writes)) {
			if (result.status === 'rejected') throw result.reason
		}
	}

	discardBuffer(): void {
		this.buffer.clear()
	}

	/**
	 * `inner`, a scan of the store below, as if the buffer had been applied. `matches` selects, by stored key bytes,
	 * the buffered keys `inner` would have returned: they replace or hide the inner entries of the same key. With
	 * `order` (the order `inner` iterates in) the buffered entries are merged into the inner ones in that order, without
	 * reading `inner` ahead; without it they follow the inner entries. The buffered entries are taken when the scan is
	 * created, so writes made while iterating do not disturb it.
	 */
	protected scan(
		inner: AsyncIterable<[SK, V]>,
		matches: (keyBytes: Buffer) => boolean,
		order?: (a: Buffer, b: Buffer) => number
	): AsyncIterable<[SK, V]> {
		const hidden = new Set<string>()
		const added: Entry<SK>[] = []
		for (const [id, entry] of this.buffer) {
			if (!matches(entry.keyBytes)) continue
			hidden.add(id)
			if (entry.value !== null) added.push(entry)
		}
		if (hidden.size === 0) return inner
		if (order) added.sort((a, b) => order(a.keyBytes, b.keyBytes))
		return this.merge(inner, hidden, added, order)
	}

	/** Deletes, through the buffer, the entries of `scan` whose key passes `matches`; returns their keys. */
	protected async deleteWhere(scan: AsyncIterable<[SK, V]>, matches: (key: SK) => boolean): Promise<SK[]> {
		const keys: SK[] = []
		let scanned = 0
		for await (const [key] of scan) {
			if (matches(key)) keys.push(key)
			if (++scanned % YIELD_EVERY === 0) await yieldIfDue()
		}
		for (const key of keys) {
			this.write(this.storeKeyCodec.encode(key), null)
		}
		return keys
	}

	private async *merge(
		inner: AsyncIterable<[SK, V]>,
		hidden: ReadonlySet<string>,
		added: readonly Entry<SK>[],
		order: ((a: Buffer, b: Buffer) => number) | undefined
	): AsyncIterable<[SK, V]> {
		let next = 0
		for await (const pair of inner) {
			const keyBytes = this.storeKeyCodec.encode(pair[0])
			while (order && next < added.length && order(added[next]!.keyBytes, keyBytes) < 0) {
				yield this.decode(added[next++]!)
			}
			if (!hidden.has(idOf(keyBytes))) yield pair
		}
		while (next < added.length) {
			yield this.decode(added[next++]!)
		}
	}

	private write(keyBytes: Buffer, value: Buffer | null): void {
		this.buffer.set(idOf(keyBytes), { keyBytes, value })
	}

	/** async: an entry that cannot be decoded rejects like a failed write. */
	private async apply(entry: Entry<SK>): Promise<void> {
		if (entry.value === null) {
			await this.inner.delete(this.keyOf(entry))
		} else {
			await this.inner.put(this.keyOf(entry), this.valueCodec.decode(entry.value))
		}
	}

	private decode(entry: Entry<SK>): [SK, V] {
		return [this.keyOf(entry), this.valueCodec.decode(entry.value!)]
	}

	private keyOf(entry: Entry<SK>): SK {
		return (entry.key ??= this.storeKeyCodec.decode(entry.keyBytes))
	}
}

export class TransactionalKeyValueStore<K, V>
	extends TransactionalStore<K, V, KeyValueStore<K, V>>
	implements KeyValueStore<K, V> {}

// Buffered keys of window and session stores are windowedKeyCodec keys: [user key][start int64BE][end int64BE].
const TIMES_BYTES = 16
const startOf = (keyBytes: Buffer): number => Number(keyBytes.readBigInt64BE(keyBytes.length - TIMES_BYTES))
const endOf = (keyBytes: Buffer): number => Number(keyBytes.readBigInt64BE(keyBytes.length - 8))
const userKeyOf = (keyBytes: Buffer): Buffer => keyBytes.subarray(0, keyBytes.length - TIMES_BYTES)
const isOfKey = (keyBytes: Buffer, userKey: Buffer): boolean =>
	keyBytes.length === userKey.length + TIMES_BYTES && userKey.compare(keyBytes, 0, userKey.length) === 0
/** The order in which window and session stores return the windows of one key. */
const byTime = (a: Buffer, b: Buffer): number => startOf(a) - startOf(b) || endOf(a) - endOf(b)

/**
 * Buffer of a window or session store: keys are windowed, entries expire after `retentionMs`. `fetch()` and
 * `findSessions()` merge in time order; `range()` keeps the key-value semantics on windowedKeyCodec bytes (its order is
 * provider-specific for windowed stores, and no operator relies on it).
 */
abstract class TransactionalWindowedStore<K, V, S extends KeyValueStore<WindowedKey<K>, V>> extends TransactionalStore<
	WindowedKey<K>,
	V,
	S
> {
	constructor(
		inner: S,
		protected readonly keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		protected readonly retentionMs: number
	) {
		super(inner, windowedKeyCodec(keyCodec), valueCodec)
	}

	/** `inner`, a scan of the windows of `key` that pass `matches`, through the buffer, in time order. */
	protected windowsOf(
		inner: AsyncIterable<[WindowedKey<K>, V]>,
		key: K,
		matches: (start: number, end: number) => boolean
	): AsyncIterable<[WindowedKey<K>, V]> {
		const userKey = this.keyCodec.encode(key)
		return this.scan(
			inner,
			keyBytes => isOfKey(keyBytes, userKey) && matches(startOf(keyBytes), endOf(keyBytes)),
			byTime
		)
	}
}

export class TransactionalWindowStore<K, V>
	extends TransactionalWindowedStore<K, V, WindowStore<K, V>>
	implements WindowStore<K, V>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.windowsOf(
			this.inner.fetch(key, timeFrom, timeTo),
			key,
			start => start >= timeFrom && start <= timeTo
		)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(this.inner.fetchAll(timeFrom, timeTo), keyBytes => {
			const start = startOf(keyBytes)
			return start >= timeFrom && start <= timeTo
		})
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const fromBytes = this.keyCodec.encode(keyFrom)
		const toBytes = this.keyCodec.encode(keyTo)
		return this.scan(this.inner.fetchRange(keyFrom, keyTo, timeFrom, timeTo), keyBytes => {
			const start = startOf(keyBytes)
			const userKey = userKeyOf(keyBytes)
			return (
				start >= timeFrom &&
				start <= timeTo &&
				Buffer.compare(userKey, fromBytes) >= 0 &&
				Buffer.compare(userKey, toBytes) <= 0
			)
		})
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		const cutoff = currentTime - this.retentionMs
		// A window that ended before the cutoff also started before it, and windows are ordered by their start: only
		// that part of the store is read, not the windows still in use.
		const startedBefore = this.fetchAll(Number.MIN_SAFE_INTEGER, cutoff - 1)
		return (await this.deleteWhere(startedBefore, window => window.windowEnd < cutoff)).length
	}
}

export class TransactionalSessionStore<K, V>
	extends TransactionalWindowedStore<K, V, SessionStore<K, V>>
	implements SessionStore<K, V>
{
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.windowsOf(
			this.inner.findSessions(key, earliestStart, latestEnd),
			key,
			(start, end) => start <= latestEnd && end >= earliestStart
		)
	}

	async remove(key: K): Promise<void> {
		await this.deleteWhere(this.findSessions(key, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER), () => true)
	}

	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		const cutoff = currentTime - this.retentionMs
		// Sessions are ordered by key, not by time: like the store itself, this reads all of them.
		return this.deleteWhere(this.all(), session => session.windowEnd < cutoff)
	}
}
