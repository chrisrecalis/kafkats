import { open, type Database, type RootDatabase } from 'lmdb'
import type {
	Codec,
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	StateStoreProvider,
	WindowedKey,
	WindowStore,
	WindowStoreOptions,
} from '@kafkats/flow'

/**
 * LMDB implementation of KeyValueStore.
 *
 * Uses LMDB for persistent, memory-mapped storage with ACID transactions.
 * Suitable for production workloads with large state.
 */
export class LMDBKeyValueStore<K, V> implements KeyValueStore<K, V> {
	private readonly db: Database<Buffer, Buffer>
	private readonly keyCodec: Codec<K>
	private readonly valueCodec: Codec<V>

	constructor(
		readonly name: string,
		db: Database<Buffer, Buffer>,
		options: KeyValueStoreOptions<K, V>
	) {
		this.db = db
		this.keyCodec = options.keyCodec
		this.valueCodec = options.valueCodec
	}

	get(key: K): Promise<V | undefined> {
		const keyBytes = this.keyCodec.encode(key)
		const valueBytes = this.db.get(keyBytes)
		if (valueBytes === undefined) {
			return Promise.resolve(undefined)
		}
		return Promise.resolve(this.valueCodec.decode(valueBytes))
	}

	async put(key: K, value: V): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		const valueBytes = this.valueCodec.encode(value)
		await this.db.put(keyBytes, valueBytes)
	}

	async delete(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		await this.db.remove(keyBytes)
	}

	async *all(): AsyncIterable<[K, V]> {
		await Promise.resolve()
		for (const { key, value } of this.db.getRange({})) {
			yield [this.keyCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: K, to: K): AsyncIterable<[K, V]> {
		await Promise.resolve()
		const fromBytes = this.keyCodec.encode(from)
		const toBytes = this.keyCodec.encode(to)

		for (const { key, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			yield [this.keyCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.getKeysCount())
	}

	init(): Promise<void> {
		// LMDB database is already initialized in the provider
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flushed
	}

	close(): Promise<void> {
		// Closing is handled by the provider
		return Promise.resolve()
	}
}

/**
 * Encodes a windowed key to a buffer for storage.
 * Format: [windowStart:8bytes][windowEnd:8bytes][keyBytes]
 *
 * This format allows efficient range queries by time.
 */
function encodeWindowedKey<K>(key: WindowedKey<K>, keyCodec: Codec<K>): Buffer {
	const keyBytes = keyCodec.encode(key.key)
	const buf = Buffer.alloc(16 + keyBytes.length)
	buf.writeBigInt64BE(BigInt(key.windowStart), 0)
	buf.writeBigInt64BE(BigInt(key.windowEnd), 8)
	keyBytes.copy(buf, 16)
	return buf
}

/**
 * Decodes a windowed key from a buffer.
 */
function decodeWindowedKey<K>(buf: Buffer, keyCodec: Codec<K>): WindowedKey<K> {
	const windowStart = Number(buf.readBigInt64BE(0))
	const windowEnd = Number(buf.readBigInt64BE(8))
	const keyBytes = buf.subarray(16)
	return {
		key: keyCodec.decode(keyBytes),
		windowStart,
		windowEnd,
	}
}

/**
 * Creates a codec for windowed keys with time-prefix ordering.
 */
function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode: (key: WindowedKey<K>) => encodeWindowedKey(key, keyCodec),
		decode: (buf: Buffer) => decodeWindowedKey(buf, keyCodec),
	}
}

/**
 * Encodes a windowed key for key-first ordering (used in session stores).
 * Format: [keyBytes][windowStart:8bytes][windowEnd:8bytes]
 */
function encodeSessionKey<K>(key: WindowedKey<K>, keyCodec: Codec<K>): Buffer {
	const keyBytes = keyCodec.encode(key.key)
	const buf = Buffer.alloc(keyBytes.length + 16)
	keyBytes.copy(buf, 0)
	buf.writeBigInt64BE(BigInt(key.windowStart), keyBytes.length)
	buf.writeBigInt64BE(BigInt(key.windowEnd), keyBytes.length + 8)
	return buf
}

/**
 * Decodes a session key from a buffer.
 */
function decodeSessionKey<K>(buf: Buffer, keyCodec: Codec<K>): WindowedKey<K> {
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
 * Creates a codec for session keys with key-prefix ordering.
 */
function sessionKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode: (key: WindowedKey<K>) => encodeSessionKey(key, keyCodec),
		decode: (buf: Buffer) => decodeSessionKey(buf, keyCodec),
	}
}

/**
 * LMDB implementation of WindowStore.
 *
 * Keys are ordered by [windowStart, windowEnd, key] for efficient time-range queries.
 */
export class LMDBWindowStore<K, V> implements WindowStore<K, V> {
	private readonly db: Database<Buffer, Buffer>
	private readonly keyCodec: Codec<K>
	private readonly windowedCodec: Codec<WindowedKey<K>>
	private readonly valueCodec: Codec<V>
	private readonly retentionMs: number

	constructor(
		readonly name: string,
		db: Database<Buffer, Buffer>,
		options: WindowStoreOptions<K, V>
	) {
		this.db = db
		this.keyCodec = options.keyCodec
		this.windowedCodec = windowedKeyCodec(options.keyCodec)
		this.valueCodec = options.valueCodec
		this.retentionMs = options.retentionMs
	}

	get(key: WindowedKey<K>): Promise<V | undefined> {
		const keyBytes = this.windowedCodec.encode(key)
		const valueBytes = this.db.get(keyBytes)
		if (valueBytes === undefined) {
			return Promise.resolve(undefined)
		}
		return Promise.resolve(this.valueCodec.decode(valueBytes))
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		const keyBytes = this.windowedCodec.encode(key)
		const valueBytes = this.valueCodec.encode(value)
		await this.db.put(keyBytes, valueBytes)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		const keyBytes = this.windowedCodec.encode(key)
		await this.db.remove(keyBytes)
	}

	async *all(): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		for (const { key, value } of this.db.getRange({})) {
			yield [this.windowedCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = this.windowedCodec.encode(from)
		const toBytes = this.windowedCodec.encode(to)

		for (const { key, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			yield [this.windowedCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const keyBytes = this.keyCodec.encode(key)

		// Scan the time range and filter by key
		const fromBytes = Buffer.alloc(16)
		fromBytes.writeBigInt64BE(BigInt(timeFrom), 0)
		fromBytes.writeBigInt64BE(BigInt(timeFrom), 8)

		const toBytes = Buffer.alloc(16)
		toBytes.writeBigInt64BE(BigInt(timeTo), 0)
		toBytes.writeBigInt64BE(BigInt(timeTo + 1), 8) // Inclusive upper bound

		for (const { key: storedKey, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			const windowedKey = this.windowedCodec.decode(storedKey)
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)

			if (keyBytes.equals(entryKeyBytes)) {
				yield [windowedKey, this.valueCodec.decode(value)]
			}
		}
	}

	async *fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = Buffer.alloc(16)
		fromBytes.writeBigInt64BE(BigInt(timeFrom), 0)
		fromBytes.writeBigInt64BE(BigInt(timeFrom), 8)

		const toBytes = Buffer.alloc(16)
		toBytes.writeBigInt64BE(BigInt(timeTo), 0)
		toBytes.writeBigInt64BE(BigInt(timeTo + 1), 8)

		for (const { key, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			yield [this.windowedCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		const keyFromBytes = this.keyCodec.encode(keyFrom)
		const keyToBytes = this.keyCodec.encode(keyTo)

		for await (const [windowedKey, value] of this.fetchAll(timeFrom, timeTo)) {
			const entryKeyBytes = this.keyCodec.encode(windowedKey.key)
			if (entryKeyBytes.compare(keyFromBytes) >= 0 && entryKeyBytes.compare(keyToBytes) <= 0) {
				yield [windowedKey, value]
			}
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.getKeysCount())
	}

	init(): Promise<void> {
		// Already initialized
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flushed
	}

	close(): Promise<void> {
		// Handled by provider
		return Promise.resolve()
	}

	/**
	 * Remove expired windows based on retention period.
	 */
	async expireOldWindows(currentTime: number): Promise<number> {
		const cutoff = currentTime - this.retentionMs
		const toDelete: Buffer[] = []

		const cutoffBytes = Buffer.alloc(16)
		cutoffBytes.writeBigInt64BE(BigInt(0), 0)
		cutoffBytes.writeBigInt64BE(BigInt(cutoff), 8)

		for (const { key } of this.db.getRange({ end: cutoffBytes })) {
			toDelete.push(key)
		}

		for (const key of toDelete) {
			await this.db.remove(key)
		}

		return toDelete.length
	}
}

/**
 * LMDB implementation of SessionStore.
 *
 * Keys are ordered by [key, windowStart, windowEnd] for efficient key-based queries.
 */
export class LMDBSessionStore<K, V> implements SessionStore<K, V> {
	private readonly db: Database<Buffer, Buffer>
	private readonly keyCodec: Codec<K>
	private readonly sessionCodec: Codec<WindowedKey<K>>
	private readonly valueCodec: Codec<V>

	constructor(
		readonly name: string,
		db: Database<Buffer, Buffer>,
		options: SessionStoreOptions<K, V>
	) {
		this.db = db
		this.keyCodec = options.keyCodec
		this.sessionCodec = sessionKeyCodec(options.keyCodec)
		this.valueCodec = options.valueCodec
	}

	get(key: WindowedKey<K>): Promise<V | undefined> {
		const keyBytes = this.sessionCodec.encode(key)
		const valueBytes = this.db.get(keyBytes)
		if (valueBytes === undefined) {
			return Promise.resolve(undefined)
		}
		return Promise.resolve(this.valueCodec.decode(valueBytes))
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		const keyBytes = this.sessionCodec.encode(key)
		const valueBytes = this.valueCodec.encode(value)
		await this.db.put(keyBytes, valueBytes)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		const keyBytes = this.sessionCodec.encode(key)
		await this.db.remove(keyBytes)
	}

	async *all(): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		for (const { key, value } of this.db.getRange({})) {
			yield [this.sessionCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = this.sessionCodec.encode(from)
		const toBytes = this.sessionCodec.encode(to)

		for (const { key, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			yield [this.sessionCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const keyBytes = this.keyCodec.encode(key)

		// Create range bounds for the key prefix
		const fromBytes = Buffer.alloc(keyBytes.length + 16)
		keyBytes.copy(fromBytes, 0)
		fromBytes.writeBigInt64BE(BigInt(0), keyBytes.length)
		fromBytes.writeBigInt64BE(BigInt(0), keyBytes.length + 8)

		const toBytes = Buffer.alloc(keyBytes.length + 16)
		keyBytes.copy(toBytes, 0)
		toBytes.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER), keyBytes.length)
		toBytes.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER), keyBytes.length + 8)

		for (const { key: storedKey, value } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			const sessionKey = this.sessionCodec.decode(storedKey)

			// Check if session overlaps with the time range
			if (sessionKey.windowStart <= latestEnd && sessionKey.windowEnd >= earliestStart) {
				yield [sessionKey, this.valueCodec.decode(value)]
			}
		}
	}

	async remove(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		const toDelete: Buffer[] = []

		// Create range bounds for the key prefix
		const fromBytes = Buffer.alloc(keyBytes.length + 16)
		keyBytes.copy(fromBytes, 0)
		fromBytes.writeBigInt64BE(BigInt(0), keyBytes.length)
		fromBytes.writeBigInt64BE(BigInt(0), keyBytes.length + 8)

		const toBytes = Buffer.alloc(keyBytes.length + 16)
		keyBytes.copy(toBytes, 0)
		toBytes.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER), keyBytes.length)
		toBytes.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER), keyBytes.length + 8)

		for (const { key: storedKey } of this.db.getRange({ start: fromBytes, end: toBytes })) {
			toDelete.push(storedKey)
		}

		for (const k of toDelete) {
			await this.db.remove(k)
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.getKeysCount())
	}

	init(): Promise<void> {
		// Already initialized
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flushed
	}

	close(): Promise<void> {
		// Handled by provider
		return Promise.resolve()
	}
}

/**
 * Configuration options for LMDBStateStoreProvider.
 */
export interface LMDBProviderOptions {
	/** Directory for LMDB database files */
	stateDir: string
	/** Maximum database size in bytes (default: 1GB) */
	mapSize?: number
	/** Maximum number of named databases (default: 100) */
	maxDbs?: number
}

/**
 * LMDB state store provider.
 *
 * Creates persistent, memory-mapped stores backed by LMDB.
 * Suitable for production workloads with large state.
 */
export class LMDBStateStoreProvider implements StateStoreProvider {
	readonly name = 'lmdb'
	private readonly rootDb: RootDatabase<Buffer, Buffer>
	private readonly stores = new Map<string, Database<Buffer, Buffer>>()

	constructor(options: LMDBProviderOptions) {
		this.rootDb = open({
			path: options.stateDir,
			mapSize: options.mapSize ?? 1024 * 1024 * 1024, // 1GB default
			maxDbs: options.maxDbs ?? 100,
			keyEncoding: 'binary',
			encoding: 'binary',
		})
	}

	private getOrCreateDb(name: string): Database<Buffer, Buffer> {
		let db = this.stores.get(name)
		if (!db) {
			db = this.rootDb.openDB(name, {
				keyEncoding: 'binary',
				encoding: 'binary',
			})
			this.stores.set(name, db)
		}
		return db
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBKeyValueStore(name, db, options)
	}

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBWindowStore(name, db, options)
	}

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V> {
		const db = this.getOrCreateDb(name)
		return new LMDBSessionStore(name, db, options)
	}

	async close(): Promise<void> {
		await this.rootDb.close()
		this.stores.clear()
	}
}

/**
 * Create an LMDB state store provider.
 */
export function lmdb(options: LMDBProviderOptions): StateStoreProvider {
	return new LMDBStateStoreProvider(options)
}
