import type { Codec } from './codec.js'

/**
 * Configuration for creating a key-value state store.
 */
export interface KeyValueStoreOptions<K, V> {
	/** Codec for serializing/deserializing keys */
	keyCodec: Codec<K>
	/** Codec for serializing/deserializing values */
	valueCodec: Codec<V>
}

/**
 * Configuration for creating a window state store.
 */
export interface WindowStoreOptions<K, V> extends KeyValueStoreOptions<K, V> {
	/** Window retention period in milliseconds */
	retentionMs: number
	/** Window size in milliseconds */
	windowSizeMs: number
}

/**
 * Configuration for creating a session state store.
 */
export interface SessionStoreOptions<K, V> extends KeyValueStoreOptions<K, V> {
	/** Session retention period in milliseconds */
	retentionMs: number
}

/**
 * A key-value state store for maintaining local state in stream processing.
 *
 * State stores are used by stateful operations like aggregations, joins, and
 * deduplication. Each store has a unique name within the application.
 */
export interface KeyValueStore<K, V> {
	/** Unique name of this store */
	readonly name: string

	/**
	 * Get the value for a key.
	 * @returns The value, or undefined if not found
	 */
	get(key: K): Promise<V | undefined>

	/**
	 * Put a key-value pair into the store.
	 */
	put(key: K, value: V): Promise<void>

	/**
	 * Delete a key from the store.
	 */
	delete(key: K): Promise<void>

	/**
	 * Iterate over all key-value pairs in the store.
	 */
	all(): AsyncIterable<[K, V]>

	/**
	 * Iterate over key-value pairs in a key range (inclusive).
	 * Keys are compared using their serialized (Buffer) form.
	 */
	range(from: K, to: K): AsyncIterable<[K, V]>

	/**
	 * Get approximate number of entries in the store.
	 */
	approximateNumEntries(): Promise<number>

	/**
	 * Initialize the store. Called before first use.
	 */
	init(): Promise<void>

	/**
	 * Flush any pending writes to durable storage.
	 */
	flush(): Promise<void>

	/**
	 * Close the store and release resources.
	 */
	close(): Promise<void>
}

/**
 * A windowed key with time boundaries.
 */
export interface WindowedKey<K> {
	key: K
	windowStart: number
	windowEnd: number
}

/**
 * A state store for windowed aggregations.
 *
 * Keys are composite: (key, windowStart, windowEnd). This allows efficient
 * querying of values within time ranges.
 */
export interface WindowStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	/**
	 * Fetch all values for a key within a time range.
	 * @param key The key to fetch
	 * @param timeFrom Start of time range (inclusive)
	 * @param timeTo End of time range (inclusive)
	 */
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>

	/**
	 * Fetch all key-value pairs within a time range.
	 * @param timeFrom Start of time range (inclusive)
	 * @param timeTo End of time range (inclusive)
	 */
	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>

	/**
	 * Fetch all values for a key within a time range, across all keys.
	 * @param keyFrom Start of key range (inclusive)
	 * @param keyTo End of key range (inclusive)
	 * @param timeFrom Start of time range (inclusive)
	 * @param timeTo End of time range (inclusive)
	 */
	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>

	/**
	 * Remove expired windows based on retention period.
	 * @param currentTime Current timestamp in milliseconds
	 * @returns Number of expired entries removed
	 */
	expireOldWindows(currentTime: number): Promise<number>
}

/**
 * A state store for session window aggregations.
 *
 * Sessions are variable-length windows that close after a period of inactivity.
 */
export interface SessionStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	/**
	 * Find all sessions for a key that overlap with the given time range.
	 * @param key The key to search
	 * @param earliestStart Earliest session start time (inclusive)
	 * @param latestEnd Latest session end time (inclusive)
	 */
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]>

	/**
	 * Remove all sessions for a key.
	 */
	remove(key: K): Promise<void>
}

/**
 * Durable store for tracking changelog restore/write progress.
 *
 * Implementations should be colocated with state storage so offsets are not
 * reused across incompatible local state directories.
 */
export interface ChangelogCheckpointStore {
	/**
	 * Get the next offset to restore from for a changelog topic-partition.
	 * Returns undefined if no checkpoint exists.
	 */
	get(topic: string, partition: number): Promise<bigint | undefined>

	/**
	 * Set the next offset to restore from for a changelog topic-partition.
	 *
	 * The offset should be the "next" offset (i.e. last-applied offset + 1).
	 */
	set(topic: string, partition: number, offset: bigint): Promise<void>
}

/**
 * Factory for creating state stores.
 *
 * Implementations can provide different storage backends (in-memory, LMDB,
 * RocksDB, etc.) while maintaining the same interface.
 */
export interface StateStoreProvider {
	/** Provider name for debugging */
	readonly name: string
	/**
	 * Optional changelog checkpoint store.
	 *
	 * If provided, Flow uses it to resume changelog restoration from the last
	 * known applied offset, avoiding full replays when local state is durable.
	 */
	getChangelogCheckpointStore?(): ChangelogCheckpointStore | undefined

	/**
	 * Create a key-value store.
	 */
	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V>

	/**
	 * Create a window store for time-based windowing.
	 */
	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V>

	/**
	 * Create a session store for session windowing.
	 */
	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V>

	/**
	 * Close all stores and release resources.
	 */
	close(): Promise<void>
}
