import type { ChangelogConfig } from '@/changelog.js'
import type { Codec } from '@/codec.js'
import type { RecordSender } from '@/processors/index.js'
import type { KeyValueStore, SessionStore, WindowStore } from '@/state.js'
import type { Consumed, KStream } from '@/types.js'

/**
 * What the DSL (`streams/`) may ask of the app while it builds a topology: state stores for the
 * operations it materializes, registration of internal topics and co-partitioning constraints,
 * the sink that records are produced through, and new streams over internal topics.
 *
 * The runtime satisfies each part directly (StoreRegistry, Topology, RecordOutput); the DSL sees
 * only this narrow view of them.
 */
export interface TopologyBuilder {
	readonly stores: StoreFactory
	readonly topology: TopologyRegistry
	/** Where ProduceNodes write (`to()`, `through()` and repartition topics). */
	readonly output: RecordSender
	/** Create a stream from a topic (used to re-consume repartition and through topics). */
	stream<K = Buffer, V = Buffer>(source: string, options?: Consumed<K, V>): KStream<K, V>
}

/** How a state store is serialized, logged and partitioned. */
export type StoreOptions<K, V> = {
	keyCodec: Codec<K>
	valueCodec: Codec<V>
	/** Changelog settings (from Materialized); `false` disables the changelog. */
	changelog?: boolean | ChangelogConfig
	/**
	 * Source topics the store's keys are co-partitioned with. With any, the store keeps one state
	 * partition per source partition; with none, it is a single app-wide store.
	 */
	sourceTopics: Set<string>
}

export type WindowStoreOptions<K, V> = StoreOptions<K, V> & { retentionMs: number; windowSizeMs: number }

export type SessionStoreOptions<K, V> = StoreOptions<K, V> & { retentionMs: number }

/**
 * Creates the state stores of the topology, each backed by a changelog unless disabled.
 * Asking again for an existing name returns the same store; asking for it as another kind of store
 * (key-value, window, session) throws.
 */
export interface StoreFactory {
	getOrCreateKeyValueStore<K, V>(name: string, options: StoreOptions<K, V>): KeyValueStore<K, V>
	getOrCreateWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V>
	getOrCreateSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V>
}

/** Records the parts of the topology that need setting up at start (internal topics, co-partitioning). */
export interface TopologyRegistry {
	/** A number unique within the topology, for generated store names. */
	nextStoreId(): number
	/** Declare topics that must have the same partition count (they share state partitions). */
	requireCopartitioned(topics: Set<string>): void
	/** Register an internal repartition topic fed by the given topics; returns its name. */
	createRepartitionTopic(name: string | undefined, upstreamTopics: Set<string>): string
	/** Mark a user topic written by through() as one that must be read from the beginning. */
	registerThroughTopic(topic: string): void
}
