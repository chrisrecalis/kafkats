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

/**
 * What a storage engine has to offer to back flow's state stores: a map of byte keys to byte
 * values that iterates in unsigned byte order (LMDB, RocksDB, ...).
 *
 * The stores in this file implement flow's store interfaces on top of it (key layout of window
 * and session stores, range and expiry queries, changelog checkpoints), so a state store provider
 * for a new engine only has to adapt its database to this interface.
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

function readValue<V>(
	bytes: Buffer | undefined | Promise<Buffer | undefined>,
	valueCodec: Codec<V>
): Promise<V | undefined> {
	if (bytes instanceof Promise) return bytes.then(b => (b === undefined ? undefined : valueCodec.decode(b)))
	return Promise.resolve(bytes === undefined ? undefined : valueCodec.decode(bytes))
}

// range()'s end is exclusive; the public range() contract of the stores is inclusive.
const KEY_TERMINATOR = Buffer.from([0])
const inclusiveEnd = (b: Buffer): Buffer => Buffer.concat([b, KEY_TERMINATOR])

// Bias signed times by 2^63 so unsigned-byte lex order matches signed numeric order.
// Buffer.alloc's zero-fill represents bias(-2^63) = lex-smallest; LEX_MAX_TIME = bias(+2^63-1).
const SIGNED_BIAS = 0x8000000000000000n
const LEX_MAX_TIME = 0xff

export function writeSignedTime(buf: Buffer, value: number, offset: number): void {
	buf.writeBigUInt64BE(BigInt(value) + SIGNED_BIAS, offset)
}

export function readSignedTime(buf: Buffer, offset: number): number {
	return Number(buf.readBigUInt64BE(offset) - SIGNED_BIAS)
}

// WindowStore key prefix [biasedStart][biasedEnd]; second half is implicit LEX_MIN_TIME (= -2^63)
// so half-open [timeBound(from), timeBound(to+1)) selects every windowStart in [from, to].
const timeBound = (t: number): Buffer => {
	const b = Buffer.alloc(16)
	writeSignedTime(b, t, 0)
	return b
}

/** KeyValueStore over an {@link OrderedByteStore}. */
export class OrderedKeyValueStore<K, V> implements KeyValueStore<K, V> {
	private readonly db: OrderedByteStore
	private readonly keyCodec: Codec<K>
	private readonly valueCodec: Codec<V>

	constructor(
		readonly name: string,
		db: OrderedByteStore,
		options: KeyValueStoreOptions<K, V>
	) {
		this.db = db
		this.keyCodec = options.keyCodec
		this.valueCodec = options.valueCodec
	}

	get(key: K): Promise<V | undefined> {
		const keyBytes = this.keyCodec.encode(key)
		return readValue(this.db.get(keyBytes), this.valueCodec)
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
		for (const { key, value } of this.db.range({})) {
			yield [this.keyCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: K, to: K): AsyncIterable<[K, V]> {
		await Promise.resolve()
		const fromBytes = this.keyCodec.encode(from)
		const toBytes = this.keyCodec.encode(to)

		for (const { key, value } of this.db.range({ start: fromBytes, end: inclusiveEnd(toBytes) })) {
			yield [this.keyCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.approximateCount())
	}

	init(): Promise<void> {
		// Opened by the provider
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flush()
	}

	close(): Promise<void> {
		// Closed by the provider
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
	writeSignedTime(buf, key.windowStart, 0)
	writeSignedTime(buf, key.windowEnd, 8)
	keyBytes.copy(buf, 16)
	return buf
}

/**
 * Decodes a windowed key from a buffer.
 */
function decodeWindowedKey<K>(buf: Buffer, keyCodec: Codec<K>): WindowedKey<K> {
	const windowStart = readSignedTime(buf, 0)
	const windowEnd = readSignedTime(buf, 8)
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
	writeSignedTime(buf, key.windowStart, keyBytes.length)
	writeSignedTime(buf, key.windowEnd, keyBytes.length + 8)
	return buf
}

/**
 * Decodes a session key from a buffer.
 */
function decodeSessionKey<K>(buf: Buffer, keyCodec: Codec<K>): WindowedKey<K> {
	const keyBytes = buf.subarray(0, buf.length - 16)
	const windowStart = readSignedTime(buf, buf.length - 16)
	const windowEnd = readSignedTime(buf, buf.length - 8)
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
 * WindowStore over an {@link OrderedByteStore}.
 *
 * Keys are ordered by [windowStart, windowEnd, key] for efficient time-range queries.
 */
export class OrderedWindowStore<K, V> implements WindowStore<K, V> {
	private readonly db: OrderedByteStore
	private readonly keyCodec: Codec<K>
	private readonly windowedCodec: Codec<WindowedKey<K>>
	private readonly valueCodec: Codec<V>
	private readonly retentionMs: number
	/**
	 * No window starting before this is in the store: where an expiry pass starts scanning. A
	 * log-structured engine would otherwise walk the deletion markers of all earlier passes again.
	 * In memory only: the first pass after a start scans from the beginning.
	 */
	private expiredBefore = Number.MIN_SAFE_INTEGER

	constructor(
		readonly name: string,
		db: OrderedByteStore,
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
		return readValue(this.db.get(keyBytes), this.valueCodec)
	}

	async put(key: WindowedKey<K>, value: V): Promise<void> {
		const keyBytes = this.windowedCodec.encode(key)
		if (key.windowStart < this.expiredBefore) this.expiredBefore = key.windowStart
		const valueBytes = this.valueCodec.encode(value)
		await this.db.put(keyBytes, valueBytes)
	}

	async delete(key: WindowedKey<K>): Promise<void> {
		const keyBytes = this.windowedCodec.encode(key)
		await this.db.remove(keyBytes)
	}

	async *all(): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		for (const { key, value } of this.db.range({})) {
			yield [this.windowedCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = this.windowedCodec.encode(from)
		const toBytes = this.windowedCodec.encode(to)

		for (const { key, value } of this.db.range({ start: fromBytes, end: inclusiveEnd(toBytes) })) {
			yield [this.windowedCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const keyBytes = this.keyCodec.encode(key)

		for (const { key: storedKey, value } of this.db.range({
			start: timeBound(timeFrom),
			end: timeBound(timeTo + 1),
		})) {
			// Compare raw key-suffix bytes (after the 16-byte windowStart/End prefix) instead of
			// decoding then re-encoding — saves a full codec round-trip per row on the join hot path.
			if (storedKey.length === 16 + keyBytes.length && storedKey.subarray(16).equals(keyBytes)) {
				yield [this.windowedCodec.decode(storedKey), this.valueCodec.decode(value)]
			}
		}
	}

	async *fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()

		for (const { key, value } of this.db.range({
			start: timeBound(timeFrom),
			end: timeBound(timeTo + 1),
		})) {
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
		return Promise.resolve(this.db.approximateCount())
	}

	init(): Promise<void> {
		// Already initialized
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flush()
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
		// The earliest windowStart this pass leaves in the store, where the next pass begins.
		let earliestKept = cutoff

		// Scan windowStart < cutoff, then drop only those whose windowEnd < cutoff (still-overlapping windows stay).
		for (const { key } of this.db.range({ start: timeBound(this.expiredBefore), end: timeBound(cutoff) })) {
			const windowedKey = this.windowedCodec.decode(key)
			if (windowedKey.windowEnd < cutoff) {
				toDelete.push(key)
			} else {
				earliestKept = Math.min(earliestKept, windowedKey.windowStart)
			}
		}

		await Promise.all(toDelete.map(key => this.db.remove(key)))
		this.expiredBefore = Math.max(this.expiredBefore, earliestKept)

		return toDelete.length
	}
}

/**
 * SessionStore over an {@link OrderedByteStore}.
 *
 * Keys are ordered by [key, windowStart, windowEnd] for efficient key-based queries.
 */
export class OrderedSessionStore<K, V> implements SessionStore<K, V> {
	private readonly db: OrderedByteStore
	private readonly keyCodec: Codec<K>
	private readonly sessionCodec: Codec<WindowedKey<K>>
	private readonly valueCodec: Codec<V>
	private readonly retentionMs: number

	constructor(
		readonly name: string,
		db: OrderedByteStore,
		options: SessionStoreOptions<K, V>
	) {
		this.db = db
		this.keyCodec = options.keyCodec
		this.sessionCodec = sessionKeyCodec(options.keyCodec)
		this.valueCodec = options.valueCodec
		this.retentionMs = options.retentionMs
	}

	get(key: WindowedKey<K>): Promise<V | undefined> {
		const keyBytes = this.sessionCodec.encode(key)
		return readValue(this.db.get(keyBytes), this.valueCodec)
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
		for (const { key, value } of this.db.range({})) {
			yield [this.sessionCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *range(from: WindowedKey<K>, to: WindowedKey<K>): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const fromBytes = this.sessionCodec.encode(from)
		const toBytes = this.sessionCodec.encode(to)

		for (const { key, value } of this.db.range({ start: fromBytes, end: inclusiveEnd(toBytes) })) {
			yield [this.sessionCodec.decode(key), this.valueCodec.decode(value)]
		}
	}

	async *findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]> {
		await Promise.resolve()
		const keyBytes = this.keyCodec.encode(key)
		const { from, to } = this.sessionPrefixRange(keyBytes)

		for (const { key: storedKey, value } of this.db.range({ start: from, end: to })) {
			// Variable-length keys with shared prefixes can sort inside [from, to) — e.g. stored "ka"
			// inside a "k" range. Filter to exact key match (length + prefix) before yielding.
			if (!this.matchesKeyPrefix(storedKey, keyBytes)) continue
			const sessionKey = this.sessionCodec.decode(storedKey)
			if (sessionKey.windowStart <= latestEnd && sessionKey.windowEnd >= earliestStart) {
				yield [sessionKey, this.valueCodec.decode(value)]
			}
		}
	}

	async remove(key: K): Promise<void> {
		const keyBytes = this.keyCodec.encode(key)
		const { from, to } = this.sessionPrefixRange(keyBytes)
		const toDelete: Buffer[] = []

		for (const { key: storedKey } of this.db.range({ start: from, end: to })) {
			// See findSessions: prefix-scan can include longer keys that share the prefix.
			if (!this.matchesKeyPrefix(storedKey, keyBytes)) continue
			toDelete.push(storedKey)
		}

		await Promise.all(toDelete.map(k => this.db.remove(k)))
	}

	private sessionPrefixRange(keyBytes: Buffer): { from: Buffer; to: Buffer } {
		// Sessions are stored as [keyBytes][biasedStart:8][biasedEnd:8]. Range covers all
		// time pairs for this key prefix: lex-min biased time = 0x00, lex-max = 0xff.
		const from = Buffer.alloc(keyBytes.length + 16) // zero-filled = LEX_MIN_TIME
		keyBytes.copy(from, 0)
		const to = Buffer.alloc(keyBytes.length + 16)
		keyBytes.copy(to, 0)
		to.fill(LEX_MAX_TIME, keyBytes.length)
		return { from, to }
	}

	private matchesKeyPrefix(storedKey: Buffer, keyBytes: Buffer): boolean {
		return storedKey.length === keyBytes.length + 16 && storedKey.subarray(0, keyBytes.length).equals(keyBytes)
	}

	approximateNumEntries(): Promise<number> {
		return Promise.resolve(this.db.approximateCount())
	}

	async expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]> {
		// Sessions are key-prefix-ordered, not time-ordered, so we can't scan a time prefix —
		// have to walk the whole store and filter. Cleanup is a cold path (per N records, not per record).
		const cutoff = currentTime - this.retentionMs
		const toDelete: Array<{ raw: Buffer; decoded: WindowedKey<K> }> = []
		for (const { key } of this.db.range({})) {
			const sessionKey = this.sessionCodec.decode(key)
			if (sessionKey.windowEnd < cutoff) {
				toDelete.push({ raw: key, decoded: sessionKey })
			}
		}
		await Promise.all(toDelete.map(({ raw }) => this.db.remove(raw)))
		return toDelete.map(e => e.decoded)
	}

	init(): Promise<void> {
		// Already initialized
		return Promise.resolve()
	}

	async flush(): Promise<void> {
		await this.db.flush()
	}

	close(): Promise<void> {
		// Handled by provider
		return Promise.resolve()
	}
}

/** Changelog checkpoints (topic, partition -> offset) in an {@link OrderedByteStore}. */
export class OrderedChangelogCheckpointStore implements ChangelogCheckpointStore {
	/** `getDb` is called per operation, so the provider may reopen the database in between. */
	constructor(private readonly getDb: () => OrderedByteStore) {}

	private get db(): OrderedByteStore {
		return this.getDb()
	}

	get(topic: string, partition: number): Promise<bigint | undefined> {
		const key = this.encodeKey(topic, partition)
		return Promise.resolve(this.db.get(key)).then(value => value?.readBigInt64BE(0))
	}

	async set(topic: string, partition: number, offset: bigint): Promise<void> {
		const key = this.encodeKey(topic, partition)
		const value = Buffer.alloc(8)
		value.writeBigInt64BE(offset, 0)
		await this.db.put(key, value)
	}

	// Made durable after the put-loop, not per write: an engine may resolve put() at visibility, and
	// a checkpoint must not survive a crash that loses the store data it stands for.
	async flush(): Promise<void> {
		await this.db.flush()
	}

	private encodeKey(topic: string, partition: number): Buffer {
		return Buffer.from(`${topic}\u0000${partition}`, 'utf8')
	}
}
