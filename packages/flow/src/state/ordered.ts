import type { Codec } from '@/codec.js'
import type {
	ChangelogCheckpointStore,
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	WindowedKey,
	WindowStore,
	WindowStoreOptions,
} from '@/state.js'
import { sessionKeys, windowKeys } from '@/state/ordered-keys.js'

/**
 * What a storage engine has to offer to back flow's state stores: a map of byte keys to byte
 * values that iterates in unsigned byte order (LMDB, RocksDB, ...).
 *
 * The stores in this file implement flow's store interfaces on top of it, so a state store
 * provider for a new engine only has to adapt its database to this interface. The engine opens
 * and closes its database; the stores never do.
 */
export interface OrderedByteStore {
	/** The value of `key`. May answer synchronously when the engine has it in memory. */
	get(key: Buffer): Buffer | undefined | Promise<Buffer | undefined>
	/** Resolves once the write is visible to `get` and `range`. */
	put(key: Buffer, value: Buffer): Promise<void>
	remove(key: Buffer): Promise<void>
	/**
	 * Entries with `start <= key < end` in byte order; unbounded where a bound is missing.
	 * Entries may be removed while iterating.
	 */
	range(bounds: { start?: Buffer; end?: Buffer }): Iterable<{ key: Buffer; value: Buffer }>
	/** Number of entries. May be an estimate, but must be 0 exactly when the store is empty. */
	approximateCount(): number
	/** Resolves once every write made so far is durable. */
	flush(): Promise<void>
}

type Bounds = Parameters<OrderedByteStore['range']>[0]

/** The smallest key after `key`: turns an inclusive upper bound into range()'s exclusive one. */
const justAfter = (key: Buffer): Buffer => Buffer.concat([key, Buffer.from([0])])

/**
 * A key-value store over an {@link OrderedByteStore}, for store keys `SK` that a codec maps to
 * bytes whose order is the order the store iterates in.
 */
abstract class OrderedStore<SK, V> implements KeyValueStore<SK, V> {
	protected constructor(
		readonly name: string,
		protected readonly db: OrderedByteStore,
		protected readonly storeKeyCodec: Codec<SK>,
		protected readonly valueCodec: Codec<V>
	) {}

	get(key: SK): Promise<V | undefined> {
		const decode = (bytes: Buffer | undefined): V | undefined =>
			bytes === undefined ? undefined : this.valueCodec.decode(bytes)
		const bytes = this.db.get(this.storeKeyCodec.encode(key))
		return bytes instanceof Promise ? bytes.then(decode) : Promise.resolve(decode(bytes))
	}

	put(key: SK, value: V): Promise<void> {
		return this.db.put(this.storeKeyCodec.encode(key), this.valueCodec.encode(value))
	}

	delete(key: SK): Promise<void> {
		return this.db.remove(this.storeKeyCodec.encode(key))
	}

	all(): AsyncIterable<[SK, V]> {
		return this.scan({})
	}

	/** Entries from `from` to `to`, both inclusive. */
	range(from: SK, to: SK): AsyncIterable<[SK, V]> {
		return this.scan({ start: this.storeKeyCodec.encode(from), end: justAfter(this.storeKeyCodec.encode(to)) })
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.approximateCount())
	}

	init(): Promise<void> {
		return Promise.resolve()
	}

	flush(): Promise<void> {
		return this.db.flush()
	}

	close(): Promise<void> {
		return Promise.resolve()
	}

	/** The entries in `bounds` whose stored key passes `matches`, decoded. */
	protected async *scan(bounds: Bounds, matches?: (storedKey: Buffer) => boolean): AsyncIterable<[SK, V]> {
		await Promise.resolve()
		for (const { key, value } of this.db.range(bounds)) {
			if (matches && !matches(key)) continue
			yield [this.storeKeyCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	/** Remove the entries in `bounds` whose decoded key passes `matches`; returns their keys. */
	protected async removeWhere(bounds: Bounds, matches: (key: SK, storedKey: Buffer) => boolean): Promise<SK[]> {
		const removed: SK[] = []
		const removals: Promise<void>[] = []
		for (const { key: storedKey } of this.db.range(bounds)) {
			const key = this.storeKeyCodec.decode(storedKey)
			if (!matches(key, storedKey)) continue
			removed.push(key)
			// Issued together: an engine that commits per event-loop turn then commits them at once.
			removals.push(this.db.remove(storedKey))
		}
		await Promise.all(removals)
		return removed
	}
}

/** KeyValueStore over an {@link OrderedByteStore}. */
export class OrderedKeyValueStore<K, V> extends OrderedStore<K, V> {
	constructor(name: string, db: OrderedByteStore, options: KeyValueStoreOptions<K, V>) {
		super(name, db, options.keyCodec, options.valueCodec)
	}
}

/**
 * WindowStore over an {@link OrderedByteStore}. Keys are ordered by window start (see
 * {@link windowKeys}), which makes time-range queries and expiry cheap.
 */
export class OrderedWindowStore<K, V> extends OrderedStore<WindowedKey<K>, V> implements WindowStore<K, V> {
	private readonly keyCodec: Codec<K>
	private readonly keys: ReturnType<typeof windowKeys<K>>
	private readonly retentionMs: number
	/**
	 * No window starting before this is in the store: where an expiry pass starts scanning. A
	 * log-structured engine would otherwise walk the deletion markers of all earlier passes again.
	 * In memory only: the first pass after a start scans from the beginning.
	 */
	private expiredBefore = Number.MIN_SAFE_INTEGER

	constructor(name: string, db: OrderedByteStore, options: WindowStoreOptions<K, V>) {
		const keys = windowKeys(options.keyCodec)
		super(name, db, keys.codec, options.valueCodec)
		this.keys = keys
		this.keyCodec = options.keyCodec
		this.retentionMs = options.retentionMs
	}

	override put(key: WindowedKey<K>, value: V): Promise<void> {
		if (key.windowStart < this.expiredBefore) this.expiredBefore = key.windowStart
		return super.put(key, value)
	}

	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		// Compared as bytes, so the windows of other keys in the time range are never decoded.
		const keyBytes = this.keyCodec.encode(key)
		return this.scan(this.keys.startingIn(timeFrom, timeTo), stored => this.keys.userKey(stored).equals(keyBytes))
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.scan(this.keys.startingIn(timeFrom, timeTo))
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const from = this.keyCodec.encode(keyFrom)
		const to = this.keyCodec.encode(keyTo)
		return this.scan(this.keys.startingIn(timeFrom, timeTo), stored => {
			const userKey = this.keys.userKey(stored)
			return userKey.compare(from) >= 0 && userKey.compare(to) <= 0
		})
	}

	/** Remove the windows that ended before the retention period; returns how many. */
	async expireOldWindows(currentTime: number): Promise<number> {
		const cutoff = currentTime - this.retentionMs
		// The earliest window this pass leaves in the store: where the next pass begins.
		let earliestKept = cutoff
		// Windows that started before the cutoff but have not ended yet stay.
		const startedBeforeCutoff = this.keys.startingIn(this.expiredBefore, cutoff - 1)
		const removed = await this.removeWhere(startedBeforeCutoff, window => {
			if (window.windowEnd < cutoff) return true
			earliestKept = Math.min(earliestKept, window.windowStart)
			return false
		})
		this.expiredBefore = Math.max(this.expiredBefore, earliestKept)
		return removed.length
	}
}

/**
 * SessionStore over an {@link OrderedByteStore}. Keys are ordered by user key (see
 * {@link sessionKeys}), which makes the sessions of one key cheap to find.
 */
export class OrderedSessionStore<K, V> extends OrderedStore<WindowedKey<K>, V> implements SessionStore<K, V> {
	private readonly keyCodec: Codec<K>
	private readonly keys: ReturnType<typeof sessionKeys<K>>
	private readonly retentionMs: number

	constructor(name: string, db: OrderedByteStore, options: SessionStoreOptions<K, V>) {
		const keys = sessionKeys(options.keyCodec)
		super(name, db, keys.codec, options.valueCodec)
		this.keys = keys
		this.keyCodec = options.keyCodec
		this.retentionMs = options.retentionMs
	}

	/** The sessions of `key` that overlap `[earliestStart, latestEnd]`. */
	async *findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.keyCodec.encode(key)
		for await (const entry of this.scan(this.keys.of(keyBytes), stored => this.keys.isOf(stored, keyBytes))) {
			const [session] = entry
			if (session.windowStart <= latestEnd && session.windowEnd >= earliestStart) yield entry
		}
	}

	/** Remove every session of `key`. */
	async remove(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		await this.removeWhere(this.keys.of(keyBytes), (_, stored) => this.keys.isOf(stored, keyBytes))
	}

	/**
	 * Remove the sessions that ended before the retention period; returns their keys. Sessions are
	 * ordered by key, not by time, so this walks the whole store (it runs once per cleanup interval
	 * of stream time, not per record).
	 */
	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		const cutoff = currentTime - this.retentionMs
		return this.removeWhere({}, session => session.windowEnd < cutoff)
	}
}

/** Changelog checkpoints (topic, partition -> offset) in an {@link OrderedByteStore}. */
export class OrderedChangelogCheckpointStore implements ChangelogCheckpointStore {
	constructor(private readonly db: OrderedByteStore) {}

	async get(topic: string, partition: number): Promise<bigint | undefined> {
		return (await this.db.get(checkpointKey(topic, partition)))?.readBigInt64BE(0)
	}

	set(topic: string, partition: number, offset: bigint): Promise<void> {
		const value = Buffer.alloc(8)
		value.writeBigInt64BE(offset, 0)
		return this.db.put(checkpointKey(topic, partition), value)
	}

	/**
	 * Flow calls this after writing a round of checkpoints, not per write: a checkpoint must not
	 * survive a crash that loses the store data it stands for.
	 */
	flush(): Promise<void> {
		return this.db.flush()
	}
}

const checkpointKey = (topic: string, partition: number): Buffer => Buffer.from(`${topic}\u0000${partition}`, 'utf8')
