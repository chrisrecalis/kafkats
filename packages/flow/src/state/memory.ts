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

/** Store key of window and session stores: [keyBytes][windowStart: 8 bytes][windowEnd: 8 bytes]. */
function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode(key: WindowedKey<K>): Buffer {
			const keyBytes = keyCodec.encode(key.key)
			const buf = Buffer.alloc(keyBytes.length + 16)
			keyBytes.copy(buf, 0)
			buf.writeBigUInt64BE(BigInt(key.windowStart) + SIGNED_TIME_BIAS, keyBytes.length)
			buf.writeBigUInt64BE(BigInt(key.windowEnd) + SIGNED_TIME_BIAS, keyBytes.length + 8)
			return buf
		},
		decode(buf: Buffer): WindowedKey<K> {
			return {
				key: keyCodec.decode(buf.subarray(0, buf.length - 16)),
				windowStart: Number(buf.readBigUInt64BE(buf.length - 16) - SIGNED_TIME_BIAS),
				windowEnd: Number(buf.readBigUInt64BE(buf.length - 8) - SIGNED_TIME_BIAS),
			}
		},
	}
}

/** Shared part of the in-memory window and session stores: a key-value store of windowed keys. */
abstract class InMemoryWindowedStore<K, V, O extends SessionStoreOptions<K, V>> implements KeyValueStore<
	WindowedKey<K>,
	V
> {
	protected readonly store: InMemoryKeyValueStore<WindowedKey<K>, V>
	protected readonly keyCodec: Codec<K>
	protected readonly retentionMs: number

	constructor(
		readonly name: string,
		options: O
	) {
		this.keyCodec = options.keyCodec
		this.retentionMs = options.retentionMs
		this.store = new InMemoryKeyValueStore(name, {
			keyCodec: windowedKeyCodec(options.keyCodec),
			valueCodec: options.valueCodec,
		})
	}

	async get(key: WindowedKey<K>): Promise<V | undefined> {
		return this.store.get(key)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		await this.store.put(key, value)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		await this.store.delete(key)
	}

	async *all(): AsyncIterable<[WindowedKey<K>, V]> {
		yield* this.store.all()
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		yield* this.store.range(from, to)
	}

	async approximateNumEntries(): Promise<number> {
		return this.store.approximateNumEntries()
	}

	async init(): Promise<void> {
		await this.store.init()
	}

	async flush(): Promise<void> {
		await this.store.flush()
	}

	async close(): Promise<void> {
		await this.store.close()
	}

	/** Entries whose windowed key matches. */
	protected async *where(matches: (key: WindowedKey<K>) => boolean): AsyncIterable<[WindowedKey<K>, V]> {
		for await (const entry of this.store.all()) {
			if (matches(entry[0])) yield entry
		}
	}

	/** Delete every entry whose windowed key matches; returns the deleted keys. */
	protected async deleteWhere(matches: (key: WindowedKey<K>) => boolean): Promise<WindowedKey<K>[]> {
		const keys: WindowedKey<K>[] = []
		for await (const [key] of this.where(matches)) {
			keys.push(key)
		}
		for (const key of keys) {
			await this.store.delete(key)
		}
		return keys
	}

	protected deleteExpired(currentTime: number): Promise<WindowedKey<K>[]> {
		const cutoff = currentTime - this.retentionMs
		return this.deleteWhere(key => key.windowEnd < cutoff)
	}

	/** Matches windowed keys of the user key with the given serialized form. */
	protected hasKey(keyBytes: Buffer): (key: WindowedKey<K>) => boolean {
		return windowedKey => this.keyCodec.encode(windowedKey.key).equals(keyBytes)
	}
}

/** In-memory implementation of WindowStore. */
export class InMemoryWindowStore<K, V>
	extends InMemoryWindowedStore<K, V, WindowStoreOptions<K, V>>
	implements WindowStore<K, V>
{
	async *fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const hasKey = this.hasKey(this.keyCodec.encode(key))
		yield* this.where(k => hasKey(k) && k.windowStart >= timeFrom && k.windowStart <= timeTo)
	}

	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		return this.where(k => k.windowStart >= timeFrom && k.windowStart <= timeTo)
	}

	async *fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyFromBytes = this.keyCodec.encode(keyFrom)
		const keyToBytes = this.keyCodec.encode(keyTo)
		yield* this.where(k => {
			const keyBytes = this.keyCodec.encode(k.key)
			return (
				Buffer.compare(keyBytes, keyFromBytes) >= 0 &&
				Buffer.compare(keyBytes, keyToBytes) <= 0 &&
				k.windowStart >= timeFrom &&
				k.windowStart <= timeTo
			)
		})
	}

	async expireOldWindows(currentTime: number): Promise<number> {
		return (await this.deleteExpired(currentTime)).length
	}
}

/** In-memory implementation of SessionStore. */
export class InMemorySessionStore<K, V>
	extends InMemoryWindowedStore<K, V, SessionStoreOptions<K, V>>
	implements SessionStore<K, V>
{
	async *findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const hasKey = this.hasKey(this.keyCodec.encode(key))
		// A session overlaps if it starts before latestEnd and ends after earliestStart.
		yield* this.where(k => hasKey(k) && k.windowStart <= latestEnd && k.windowEnd >= earliestStart)
	}

	async remove(key: K): Promise<void> {
		await this.deleteWhere(this.hasKey(this.keyCodec.encode(key)))
	}

	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		return this.deleteExpired(currentTime)
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
