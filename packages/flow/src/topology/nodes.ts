import type { Codec } from '@/codec.js'
import type { ProcessorSupplier, RecordMetadata } from '@/processor/api.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { StateStore, StateStoreProvider } from '@/state.js'

export type NodeName = string
export type ResetPolicy = 'earliest' | 'latest' | 'none'

export interface SourceDef {
	readonly kind: 'source'
	readonly name: NodeName
	readonly topics: readonly string[]
	/** Absent: the raw Buffer. */
	readonly keyCodec?: Codec<unknown>
	readonly valueCodec?: Codec<unknown>
	readonly timestampExtractor?: TimestampExtractor
	/** Per source (Kafka Streams' Consumed.withOffsetResetPolicy); repartition sources always read from 'earliest'. */
	readonly offsetReset?: ResetPolicy
	/** repartition: an internal topic; through: a user topic written and read by the app. */
	readonly internal: false | 'repartition' | 'through'
}

export interface ProcessorDef {
	readonly kind: 'processor'
	readonly name: NodeName
	readonly supplier: ProcessorSupplier<unknown, unknown, unknown, unknown>
	readonly parents: readonly NodeName[]
	readonly stores: readonly string[]
	/** Set by DSL joins that pass null keys (left/outer stream-stream, left stream-table, left global). */
	readonly relaxedNullKeyJoin?: true
}

/** Picks the partition of a record; undefined: the producer's default partitioning of the encoded key. */
export type StreamPartitioner<K, V> = (topic: string, key: K, value: V, partitionCount: number) => number | undefined
/** Picks the topic of a record (Kafka Streams' TopicNameExtractor). */
export type TopicNameExtractor<K, V> = (key: K, value: V, context: RecordMetadata | null) => string

export interface SinkDef {
	readonly kind: 'sink'
	readonly name: NodeName
	readonly parents: readonly NodeName[]
	readonly topic: string | TopicNameExtractor<unknown, unknown>
	readonly keyCodec?: Codec<unknown>
	readonly valueCodec?: Codec<unknown>
	/** Set by the DSL (the windowed partitioner for windowed keys); undefined: the producer default. */
	readonly partitioner?: StreamPartitioner<unknown, unknown>
	readonly internal: false | 'repartition'
	/** Repartition sinks only, computed at build: 'drop' unless a relaxed join is downstream. */
	readonly nullKeys?: 'drop' | 'keep'
}

export type NodeDef = SourceDef | ProcessorDef | SinkDef

/** How a store is logged to its changelog topic. */
export type ResolvedChangelogConfig =
	| { readonly enabled: false }
	| {
			readonly enabled: true
			/** Decorated: `<applicationId>-<store>-changelog` unless a topic name was given. */
			readonly topic: string
			readonly configs: Readonly<Record<string, string>>
			readonly replicationFactor?: number
			readonly skipRestoration: boolean
	  }

export interface StoreBuilder<S extends StateStore = StateStore> {
	readonly name: string
	readonly kind: 'key-value' | 'window' | 'session'
	/** Values wrapped as ValueAndTimestamp. */
	readonly timestamped: boolean
	readonly logging: ResolvedChangelogConfig
	/** Window and session stores. */
	readonly retentionMs?: number
	/** Window stores. */
	readonly windowSizeMs?: number
	/**
	 * Clock and clipping layer; undefined for key-value stores. A builder is shared by every task, so it
	 * carries no per-task callback: `emitFinal` makes reads unclipped and bounds expiry by the task's emit
	 * progress, read from the task's metadata key `<store>.emit`.
	 */
	readonly clock?: {
		readonly emitFinal?: { readonly kind: 'time' | 'session' | 'sliding'; readonly sizeMs?: number }
	}
	/** The provider store (bottom of the stack). */
	create(provider: StateStoreProvider, providerName: string): S
	/** As seen by processors. */
	readonly keyCodec: Codec<unknown>
	readonly valueCodec: Codec<unknown>
}

export interface GlobalStoreDef {
	readonly topic: string
	readonly sourceName: NodeName
	readonly processorName: NodeName
	/** Key-value, timestamped, logging disabled. */
	readonly store: StoreBuilder
	readonly supplier: ProcessorSupplier<unknown, unknown, never, never>
	readonly keyCodec?: Codec<unknown>
	readonly valueCodec?: Codec<unknown>
	readonly timestampExtractor?: TimestampExtractor
}

export interface InternalTopicSpec {
	/** Decorated: `<applicationId>-<name>-repartition` or `-changelog`. */
	readonly name: string
	readonly kind: 'repartition' | 'changelog'
	/** Repartition topics: only an explicit Repartitioned.numberOfPartitions. */
	readonly partitions?: number
	readonly configs: Readonly<Record<string, string>>
	readonly replicationFactor?: number
	/** Changelogs with `changelog.autoCreate: false`. */
	readonly validateOnly: boolean
}

/** Partition count of every source, internal and changelog topic. */
export type PartitionCounts = ReadonlyMap<string, number>
