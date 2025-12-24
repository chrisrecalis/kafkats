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
		const keyBytes = this.serializeKey(key)
		const entry = this.store.get(this.keyId(keyBytes))
		if (!entry) {
			return Promise.resolve(undefined)
		}
		return Promise.resolve(this.valueCodec.decode(entry.valueBytes))
	}

	put(key: K, value: V): Promise<void> {
		const keyBytes = this.serializeKey(key)
		const valueBytes = this.valueCodec.encode(value)
		this.store.set(this.keyId(keyBytes), { keyBytes, valueBytes })
		return Promise.resolve()
	}

	delete(key: K): Promise<void> {
		const keyBytes = this.serializeKey(key)
		this.store.delete(this.keyId(keyBytes))
		return Promise.resolve()
	}

	async *all(): AsyncIterable<[K, V]> {
		await Promise.resolve()
		for (const entry of this.store.values()) {
			const key = this.deserializeKey(entry.keyBytes)
			const value = this.valueCodec.decode(entry.valueBytes)
			yield [key, value]
		}
	}

	async *range(from: K, to: K): AsyncIterable<[K, V]> {
		await Promise.resolve()
		const fromBytes = this.serializeKey(from)
		const toBytes = this.serializeKey(to)

		// Sort by raw key bytes to match serialized key ordering
		const sortedEntries = [...this.store.values()].sort((a, b) => Buffer.compare(a.keyBytes, b.keyBytes))

		for (const entry of sortedEntries) {
			if (Buffer.compare(entry.keyBytes, fromBytes) >= 0 && Buffer.compare(entry.keyBytes, toBytes) <= 0) {
				const key = this.deserializeKey(entry.keyBytes)
				const value = this.valueCodec.decode(entry.valueBytes)
				yield [key, value]
			}
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.store.size)
	}

	init(): Promise<void> {
		// No initialization needed for in-memory store
		return Promise.resolve()
	}

	flush(): Promise<void> {
		// No flushing needed for in-memory store
		return Promise.resolve()
	}

	close(): Promise<void> {
		this.store.clear()
		return Promise.resolve()
	}

	private serializeKey(key: K): Buffer {
		return this.keyCodec.encode(key)
	}

	private keyId(keyBytes: Buffer): string {
		return keyBytes.toString('base64')
	}

	private deserializeKey(keyBytes: Buffer): K {
		return this.keyCodec.decode(keyBytes)
	}
}

/**
 * Encodes a windowed key to a buffer for storage.
 * Format: [keyBytes][windowStart:8bytes][windowEnd:8bytes]
 */
function encodeWindowedKey<K>(key: WindowedKey<K>, keyCodec: Codec<K>): Buffer {
	const keyBytes = keyCodec.encode(key.key)
	const buf = Buffer.alloc(keyBytes.length + 16)
	keyBytes.copy(buf, 0)
	buf.writeBigInt64BE(BigInt(key.windowStart), keyBytes.length)
	buf.writeBigInt64BE(BigInt(key.windowEnd), keyBytes.length + 8)
	return buf
}

/**
 * Decodes a windowed key from a buffer.
 */
function decodeWindowedKey<K>(buf: Buffer, keyCodec: Codec<K>): WindowedKey<K> {
	const keyBytes = buf.subarray(0, buf.length - 16)
	const windowStart = Number(buf.readBigInt64BE(buf.length - 16))
	const windowEnd = Number(buf.readBigInt64BE(buf.length - 8))
	return {
		key: keyCodec.decode(keyBytes),
		windowStart,
		windowEnd,
	}
}

/**
 * Creates a codec for windowed keys.
 */
function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode: (key: WindowedKey<K>) => encodeWindowedKey(key, keyCodec),
		decode: (buf: Buffer) => decodeWindowedKey(buf, keyCodec),
	}
}

/**
 * In-memory implementation of WindowStore.
 *
 * Stores windowed key-value pairs with time-based querying support.
 */
export class InMemoryWindowStore<K, V> implements WindowStore<K, V> {
	private readonly store: InMemoryKeyValueStore<WindowedKey<K>, V>
	private readonly keyCodec: Codec<K>
	private readonly retentionMs: number

	constructor(
		readonly name: string,
		options: WindowStoreOptions<K, V>
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

	async *fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.keyCodec.encode(key)
		for await (const [windowedKey, value] of this.store.all()) {
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)
			if (
				keyBytes.equals(entryKeyBytes) &&
				windowedKey.windowStart >= timeFrom &&
				windowedKey.windowStart <= timeTo
			) {
				yield [windowedKey, value]
			}
		}
	}

	async *fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		for await (const [windowedKey, value] of this.store.all()) {
			if (windowedKey.windowStart >= timeFrom && windowedKey.windowStart <= timeTo) {
				yield [windowedKey, value]
			}
		}
	}

	async *fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyFromBytes = this.keyCodec.encode(keyFrom)
		const keyToBytes = this.keyCodec.encode(keyTo)

		for await (const [windowedKey, value] of this.store.all()) {
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)
			if (
				Buffer.compare(entryKeyBytes, keyFromBytes) >= 0 &&
				Buffer.compare(entryKeyBytes, keyToBytes) <= 0 &&
				windowedKey.windowStart >= timeFrom &&
				windowedKey.windowStart <= timeTo
			) {
				yield [windowedKey, value]
			}
		}
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

	/**
	 * Remove expired windows based on retention period.
	 */
	async expireOldWindows(currentTime: number): Promise<number> {
		const cutoff = currentTime - this.retentionMs
		const toDelete: WindowedKey<K>[] = []

		for await (const [windowedKey] of this.store.all()) {
			if (windowedKey.windowEnd < cutoff) {
				toDelete.push(windowedKey)
			}
		}

		for (const key of toDelete) {
			await this.store.delete(key)
		}

		return toDelete.length
	}
}

/**
 * In-memory implementation of SessionStore.
 *
 * Stores session windows that can be merged when activity resumes.
 */
export class InMemorySessionStore<K, V> implements SessionStore<K, V> {
	private readonly store: InMemoryKeyValueStore<WindowedKey<K>, V>
	private readonly keyCodec: Codec<K>
	private readonly retentionMs: number

	constructor(
		readonly name: string,
		options: SessionStoreOptions<K, V>
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

	async *findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyBytes = this.keyCodec.encode(key)

		for await (const [windowedKey, value] of this.store.all()) {
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)
			// Session overlaps if it starts before latestEnd and ends after earliestStart
			if (
				keyBytes.equals(entryKeyBytes) &&
				windowedKey.windowStart <= latestEnd &&
				windowedKey.windowEnd >= earliestStart
			) {
				yield [windowedKey, value]
			}
		}
	}

	async remove(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		const toDelete: WindowedKey<K>[] = []

		for await (const [windowedKey] of this.store.all()) {
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)
			if (keyBytes.equals(entryKeyBytes)) {
				toDelete.push(windowedKey)
			}
		}

		for (const k of toDelete) {
			await this.store.delete(k)
		}
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
	private readonly stores: Array<KeyValueStore<unknown, unknown>> = []

	constructor(options?: InMemoryProviderOptions) {
		// Options reserved for future use
		void options
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const store = new InMemoryKeyValueStore(name, options)
		this.stores.push(store as KeyValueStore<unknown, unknown>)
		return store
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		const store = new InMemoryWindowStore(name, options)
		this.stores.push(store as KeyValueStore<unknown, unknown>)
		return store
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		const store = new InMemorySessionStore(name, options)
		this.stores.push(store as KeyValueStore<unknown, unknown>)
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
