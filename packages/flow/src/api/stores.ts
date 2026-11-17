import type { WindowDuration } from '@/api/options.js'
import type { ChangelogConfig } from '@/changelog/config.js'
import type { Codec } from '@/codec.js'
import type {
	KeyValueStore,
	SessionStore,
	StateStore,
	TimestampedKeyValueStore,
	TimestampedWindowStore,
	WindowStore,
} from '@/state.js'

/**
 * A typed store of the Processor API, made by `Stores` (Kafka Streams' StoreBuilder, as a definition). Pass it to
 * `process()`, `processValues()` or `KTable.transformValues()` in `{ stores: [definition] }` to connect the processor to
 * it (the store is registered with the first processor that names it), or register it first with
 * `app.addStateStore(definition)` and connect processors by name. Processors read it typed with
 * `context.getStateStore(definition)`.
 *
 * Every task has its own instance of the store, for its partition, logged to the same partition of the store's
 * changelog and restored from it. Processors connected to one store run in one task per partition, so the topics they
 * read must be co-partitioned.
 */
export interface StoreDefinition<S extends StateStore> {
	readonly name: string
	/** Phantom: carries the store type. */
	readonly __store?: S
}

export interface StoreOptions<K, V> {
	/**
	 * The store's name, unique in the app: its persisted state is keyed by it and its changelog topic is
	 * `<applicationId>-<name>-changelog`. 1 to 249 ASCII alphanumerics, '.', '_' and '-'.
	 */
	name: string
	key: Codec<K>
	value: Codec<V>
	/**
	 * Changelog backing for fault-tolerant state. Default: true (enabled). A ChangelogConfig names the topic, sets its
	 * configs and replication factor, or skips restoration.
	 */
	logging?: boolean | ChangelogConfig
}

/**
 * A window store. As in Kafka Streams, reads return only the windows ending less than `retention` ms before the newest
 * window end written to (or deleted from) the store. A processor reclaims the space of older windows with
 * `expireOldWindows(time)` (from a stream-time punctuator, say), which deletes the windows ending before
 * `time - retention` from the task's store, but never one that reads may still return; the changelog drops them once
 * they are `retention` + 1 day old (`cleanup.policy=delete,compact`).
 */
export interface WindowStoreDefinitionOptions<K, V> extends StoreOptions<K, V> {
	/** How long windows are kept, at least `windowSize`. */
	retention: WindowDuration
	/** The size of the windows the store holds (Kafka Streams' window store argument; not checked on puts). */
	windowSize: WindowDuration
}

/**
 * A session store. As in Kafka Streams, reads return only the sessions ending less than `retention` ms before the
 * newest session end written to (or deleted from) the store. A processor reclaims the space of older sessions with
 * `expireOldSessions(time)`, which deletes the sessions ending before `time - retention` from the task's store, but
 * never one that reads may still return; the changelog drops them once they are `retention` + 1 day old.
 */
export interface SessionStoreDefinitionOptions<K, V> extends StoreOptions<K, V> {
	/** How long sessions are kept after they end. */
	retention: WindowDuration
}

/**
 * The `Stores` factory: store definitions instead of Kafka Streams' builder chains. Durations are validated here
 * (TopologyError); names when the store is registered.
 */
export interface StoresFactory {
	keyValueStore<K, V>(options: StoreOptions<K, V>): StoreDefinition<KeyValueStore<K, V>>
	/** Values carry the timestamp of their record: `{ value, timestamp }`, stored as `[int64BE timestamp][value]`. */
	timestampedKeyValueStore<K, V>(options: StoreOptions<K, V>): StoreDefinition<TimestampedKeyValueStore<K, V>>
	windowStore<K, V>(options: WindowStoreDefinitionOptions<K, V>): StoreDefinition<WindowStore<K, V>>
	/** Values carry the timestamp of their record: `{ value, timestamp }`, stored as `[int64BE timestamp][value]`. */
	timestampedWindowStore<K, V>(
		options: WindowStoreDefinitionOptions<K, V>
	): StoreDefinition<TimestampedWindowStore<K, V>>
	sessionStore<K, V>(options: SessionStoreDefinitionOptions<K, V>): StoreDefinition<SessionStore<K, V>>
}
