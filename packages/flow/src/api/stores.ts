import type { WindowDuration } from '@/api/options.js'
import type { ChangelogConfig } from '@/changelog/config.js'
import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, StateStore, TimestampedKeyValueStore, WindowStore } from '@/state.js'

/**
 * A typed store declaration for the Processor API: register it with `app.addStateStore(definition)`,
 * connect it with `process(supplier, { stores: [definition] })` and get it typed with
 * `context.getStateStore(definition)`.
 */
export interface StoreDefinition<S extends StateStore> {
	readonly name: string
	/** Phantom: carries the store type. */
	readonly __store?: S
}

export interface StoreOptions<K, V> {
	name: string
	key: Codec<K>
	value: Codec<V>
	/** Changelog logging; default true. */
	logging?: boolean | ChangelogConfig
}

/** The `Stores` factory: store definitions instead of Kafka Streams' builder chains. */
export interface StoresFactory {
	keyValueStore<K, V>(options: StoreOptions<K, V>): StoreDefinition<KeyValueStore<K, V>>
	timestampedKeyValueStore<K, V>(options: StoreOptions<K, V>): StoreDefinition<TimestampedKeyValueStore<K, V>>
	windowStore<K, V>(
		options: StoreOptions<K, V> & { retention: WindowDuration; windowSize: WindowDuration }
	): StoreDefinition<WindowStore<K, V>>
	sessionStore<K, V>(options: StoreOptions<K, V> & { retention: WindowDuration }): StoreDefinition<SessionStore<K, V>>
}
