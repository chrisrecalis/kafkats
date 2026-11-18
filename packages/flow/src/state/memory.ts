import type { Codec } from '@/codec.js'
import type {
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	StateStoreProvider,
	WindowedKey,
	WindowStore,
	WindowStoreOptions,
} from '@/state.js'

/**
 * In-memory implementation of KeyValueStore.
 *
 * Uses a Map for storage. Keys are serialized to Buffer for comparison.
 * Suitable for development, testing, and small state sizes.
 */
export class InMemoryKeyValueStore<K, V> implements KeyValueStore<K, V> {
	private readonly store = new Map<string, { keyBytes: Buffer; valueBytes: Buffer }>()
	private readonly keyCodec: Codec<K>
	private readonly valueCodec: Codec<V>

	constructor(
		readonly name: string,
		options: KeyValueStoreOptions<K, V>
	) {
		this.keyCodec = options.keyCodec
		this.valueCodec = options.valueCodec
	}

	get(key: K): Promise<V | undefined> {
		const keyBytes = this.keyCodec.encode(key)
		const entry = this.store.get(this.keyId(keyBytes))
		if (!entry) {
			return Promise.resolve(undefined)
		}
		return Promise.resolve(this.valueCodec.decode(entry.valueBytes))
	}

	put(key: K, value: V): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		const valueBytes = this.valueCodec.encode(value)
		this.store.set(this.keyId(keyBytes), { keyBytes, valueBytes })
		return Promise.resolve()
	}

	delete(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		this.store.delete(this.keyId(keyBytes))
		return Promise.resolve()
	}

	async *all(): AsyncIterable<[K, V]> {
		await Promise.resolve()
		for (const entry of this.store.values()) {
			const key = this.keyCodec.decode(entry.keyBytes)
			const value = this.valueCodec.decode(entry.valueBytes)
			yield [key, value]
		}
	}

	async *range(from: K, to: K): AsyncIterable<[K, V]> {
		await Promise.resolve()
		const fromBytes = this.keyCodec.encode(from)
		const toBytes = this.keyCodec.encode(to)

		// Sort by raw key bytes to match serialized key ordering
		const sortedEntries = [...this.store.values()].sort((a, b) => Buffer.compare(a.keyBytes, b.keyBytes))

		for (const entry of sortedEntries) {
			if (Buffer.compare(entry.keyBytes, fromBytes) >= 0 && Buffer.compare(entry.keyBytes, toBytes) <= 0) {
				const key = this.keyCodec.decode(entry.keyBytes)
				const value = this.valueCodec.decode(entry.valueBytes)
				yield [key, value]
			}
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.store.size)
	}

	init(): Promise<void> {
		return Promise.resolve()
	}

	flush(): Promise<void> {
		return Promise.resolve()
	}

	close(): Promise<void> {
		this.store.clear()
		return Promise.resolve()
	}

	private keyId(keyBytes: Buffer): string {
		return keyBytes.toString('base64')
	}
}

// Bias signed times by 2^63 so unsigned-byte lex order matches signed numeric order.
// range() selects entries by serialized-key byte comparison, so the time component must
// be order-preserving; plain signed big-endian would sort negative starts after positive
// ones and break ranges that span the sign boundary. Matches the LMDB store's encoding.
const SIGNED_TIME_BIAS = 0x8000000000000000n

/** Serialized windowed key, for range(): [keyBytes][windowStart: 8 bytes][windowEnd: 8 bytes]. */
function windowedKeyBytes(keyBytes: Buffer, windowStart: number, windowEnd: number): Buffer {
	const buf = Buffer.alloc(keyBytes.length + 16)
	keyBytes.copy(buf, 0)
	buf.writeBigUInt64BE(BigInt(windowStart) + SIGNED_TIME_BIAS, keyBytes.length)
	buf.writeBigUInt64BE(BigInt(windowEnd) + SIGNED_TIME_BIAS, keyBytes.length + 8)
	return buf
}

type Window = { windowStart: number; windowEnd: number; valueBytes: Buffer }

/** One entry of a windowed store: a window (or session) of the user key `keyBytes`. */
type WindowEntry = Window & { keyBytes: Buffer }

/** The windows (or sessions) stored for one user key, by `${windowStart}:${windowEnd}`. */
type KeyWindows = { keyBytes: Buffer; windows: Map<string, Window> }

const keyId = (keyBytes: Buffer): string => keyBytes.toString('base64')

const windowId = (key: { windowStart: number; windowEnd: number }): string => `${key.windowStart}:${key.windowEnd}`

const byTime = (a: Window, b: Window): number => a.windowStart - b.windowStart || a.windowEnd - b.windowEnd

/**
 * Shared part of the in-memory window and session stores: a key-value store of windowed keys.
 *
 * Entries are grouped by user key, so what runs per record (get, put, fetch, findSessions,
 * remove) only touches the windows of that key instead of scanning the whole store. fetch and
 * findSessions return the key's windows in time order, like the LMDB stores.
 */
abstract class InMemoryWindowedStore<K, V, O extends SessionStoreOptions<K, V>> implements KeyValueStore<
	WindowedKey<K>,
	V
> {
	/** By the user key's serialized form (see keyId). */
	private readonly byKey = new Map<string, KeyWindows>()
	private size = 0
	protected readonly keyCodec: Codec<K>
	protected readonly valueCodec: Codec<V>
	protected readonly retentionMs: number

	constructor(
		readonly name: string,
		options: O
	) {
		this.keyCodec = options.keyCodec
		this.valueCodec = options.valueCodec
		this.retentionMs = options.retentionMs
	}

	get(key: WindowedKey<K>): Promise<V | undefined> {
		const window = this.byKey.get(keyId(this.keyCodec.encode(key.key)))?.windows.get(windowId(key))
		return Promise.resolve(window ? this.valueCodec.decode(window.valueBytes) : undefined)
	}

	put(key: WindowedKey<K>, value: V): Promise<void> {
		const keyBytes = this.keyCodec.encode(key.key)
		const valueBytes = this.valueCodec.encode(value)
		const id = keyId(keyBytes)
		let group = this.byKey.get(id)
		if (!group) {
			group = { keyBytes, windows: new Map() }
			this.byKey.set(id, group)
		}
		const window = windowId(key)
		if (!group.windows.has(window)) this.size += 1
		group.windows.set(window, { windowStart: key.windowStart, windowEnd: key.windowEnd, valueBytes })
		return Promise.resolve()
	}

	delete(key: WindowedKey<K>): Promise<void> {
		const id = keyId(this.keyCodec.encode(key.key))
		const group = this.byKey.get(id)
		if (group?.windows.delete(windowId(key))) {
			this.size -= 1
			if (group.windows.size === 0) this.byKey.delete(id)
		}
		return Promise.resolve()
	}

	all(): AsyncIterable<[WindowedKey<K>, V]> {
		return this.where(() => true)
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = windowedKeyBytes(this.keyCodec.encode(from.key), from.windowStart, from.windowEnd)
		const toBytes = windowedKeyBytes(this.keyCodec.encode(to.key), to.windowStart, to.windowEnd)
		// Select and order by serialized key bytes, like a byte-ordered store.
		const selected = [...this.entries()]
			.map(entry => ({ entry, bytes: windowedKeyBytes(entry.keyBytes, entry.windowStart, entry.windowEnd) }))
			.filter(({ bytes }) => Buffer.compare(bytes, fromBytes) >= 0 && Buffer.compare(bytes, toBytes) <= 0)
			.sort((a, b) => Buffer.compare(a.bytes, b.bytes))
		for (const { entry } of selected) {
			yield this.decode(entry)
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.size)
	}

	init(): Promise<void> {
		return Promise.resolve()
	}

	flush(): Promise<void> {
		return Promise.resolve()
	}

	close(): Promise<void> {
		this.byKey.clear()
		this.size = 0
		return Promise.resolve()
	}

	/** Entries that match, grouped by user key. */
	protected async *where(matches: (entry: WindowEntry) => boolean): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		for (const entry of this.entries()) {
			if (matches(entry)) yield this.decode(entry)
		}
	}

	/** The windows of one user key that match, in time order. */
	protected async *whereKey(key: K, matches: (window: Window) => boolean): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const group = this.byKey.get(keyId(this.keyCodec.encode(key)))
		if (!group) return
		const selected = [...group.windows.values()].filter(matches).sort(byTime)
		for (const window of selected) {
			yield this.decode({ keyBytes: group.keyBytes, ...window })
		}
	}

	/** Delete the windows that match, of one user key or (without `key`) of all; returns their keys. */
	protected deleteWhere(matches: (window: Window) => boolean, key?: K): WindowedKey<K>[] {
		const ids = key === undefined ? [...this.byKey.keys()] : [keyId(this.keyCodec.encode(key))]
		const deleted: WindowedKey<K>[] = []
		for (const id of ids) {
			const group = this.byKey.get(id)
			if (!group) continue
			let userKey: K | undefined
			for (const [windowKey, window] of group.windows) {
				if (!matches(window)) continue
				group.windows.delete(windowKey)
				this.size -= 1
				userKey ??= this.keyCodec.decode(group.keyBytes)
				deleted.push({ key: userKey, windowStart: window.windowStart, windowEnd: window.windowEnd })
			}
			if (group.windows.size === 0) this.byKey.delete(id)
		}
		return deleted
	}

	protected deleteExpired(currentTime: number): WindowedKey<K>[] {
		const cutoff = currentTime - this.retentionMs
		return this.deleteWhere(window => window.windowEnd < cutoff)
	}

	private *entries(): Iterable<WindowEntry> {
		for (const { keyBytes, windows } of this.byKey.values()) {
			for (const window of windows.values()) {
				yield { keyBytes, ...window }
			}
		}
	}

	private decode(entry: WindowEntry): [WindowedKey<K>, V] {
		const { keyBytes, windowStart, windowEnd, valueBytes } = entry
		return [{ key: this.keyCodec.decode(keyBytes), windowStart, windowEnd }, this.valueCodec.decode(valueBytes)]
	}
}

/** In-memory implementation of WindowStore. */
export class InMemoryWindowStore<K, V>
	extends InMemoryWindowedStore<K, V, WindowStoreOptions<K, V>>
	implements WindowStore<K, V>
{
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.whereKey(key, w => w.windowStart >= timeFrom && w.windowStart <= timeTo)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.where(w => w.windowStart >= timeFrom && w.windowStart <= timeTo)
	}

	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyFromBytes = this.keyCodec.encode(keyFrom)
		const keyToBytes = this.keyCodec.encode(keyTo)
		return this.where(
			entry =>
				Buffer.compare(entry.keyBytes, keyFromBytes) >= 0 &&
				Buffer.compare(entry.keyBytes, keyToBytes) <= 0 &&
				entry.windowStart >= timeFrom &&
				entry.windowStart <= timeTo
		)
	}

	expireOldWindows(currentTime: number): Promise<number> {
		return Promise.resolve(this.deleteExpired(currentTime).length)
	}
}

/** In-memory implementation of SessionStore. */
export class InMemorySessionStore<K, V>
	extends InMemoryWindowedStore<K, V, SessionStoreOptions<K, V>>
	implements SessionStore<K, V>
{
	findSessions(key: K, earliestSessionEnd: number, latestSessionStart: number): AsyncIterable<[WindowedKey<K>, V]> {
		// The sessions that end at or after earliestSessionEnd and start at or before latestSessionStart.
		return this.whereKey(key, s => s.windowStart <= latestSessionStart && s.windowEnd >= earliestSessionEnd)
	}

	remove(key: K): Promise<void> {
		this.deleteWhere(() => true, key)
		return Promise.resolve()
	}

	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		return Promise.resolve(this.deleteExpired(currentTime))
	}
}

/**
 * Configuration options for InMemoryStateStoreProvider.
 */
export interface InMemoryProviderOptions {
	/** Optional base directory (ignored for in-memory, but kept for API compatibility) */
	stateDir?: string
}

/**
 * In-memory state store provider.
 *
 * Creates in-memory stores suitable for development, testing, and small state.
 * State is lost when the application stops.
 */
export class InMemoryStateStoreProvider implements StateStoreProvider {
	readonly name = 'in-memory'
	private readonly stores: Array<{ close(): Promise<void> }> = []

	constructor(options?: InMemoryProviderOptions) {
		// Options reserved for future use
		void options
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const store = new InMemoryKeyValueStore(name, options)
		this.stores.push(store)
		return store
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		const store = new InMemoryWindowStore(name, options)
		this.stores.push(store)
		return store
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		const store = new InMemorySessionStore(name, options)
		this.stores.push(store)
		return store
	}

	async close(): Promise<void> {
		await Promise.all(this.stores.map(store => store.close()))
		this.stores.length = 0
	}
}

/**
 * Create an in-memory state store provider.
 */
export function inMemory(options?: InMemoryProviderOptions): StateStoreProvider {
	return new InMemoryStateStoreProvider(options)
}
