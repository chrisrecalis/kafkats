import type { KStream } from '@/api/kstream.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { ChangelogConfig } from '@/changelog/config.js'
import type { Codec } from '@/codec.js'
import type { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { StateStore } from '@/state.js'
import type { ResetPolicy } from '@/topology/nodes.js'

/** Milliseconds, or a number with a unit: `'500ms'`, `'30s'`, `'5m'`, `'1h'`, `'1d'`. */
export type WindowDuration = number | `${number}ms` | `${number}s` | `${number}m` | `${number}h` | `${number}d`

/**
 * Every operator takes a name: it fixes the node name and, where the operator has them, store and topic names (an
 * operator registering several names them `<name>-<suffix>`, as Kafka Streams does). Without one, names are generated
 * in the order of the DSL calls, so adding or naming an operator can change the generated names after it.
 */
export interface NamedOptions {
	/**
	 * 1 to 249 ASCII alphanumerics, '.', '_' and '-', as Kafka topic names (names end up in topic and store names), and
	 * unique in the app: an illegal or duplicate name is a TopologyError.
	 */
	name?: string
}

/** How a source reads its topic. */
export interface Consumed<K, V> extends NamedOptions {
	key?: Codec<K>
	value?: Codec<V>
	/** Default: `FlowConfig.defaultTimestampExtractor`. */
	timestampExtractor?: TimestampExtractor
	/** Where to start without a committed offset. Default: `FlowConfig.consumer.autoOffsetReset ?? 'earliest'`. */
	offsetReset?: ResetPolicy
}

/** Picks the partition of a record; undefined: the producer's default partitioning of the encoded key. */
export type RecordPartitioner<K, V> = (key: K | null, value: V | null, partitionCount: number) => number | undefined

/** How a stream is written to a topic. */
export interface Produced<K, V> extends NamedOptions {
	key?: Codec<K>
	value?: Codec<V>
	partitioner?: RecordPartitioner<K, V>
}

/**
 * How a stream is grouped: `name` names its repartition topic `<applicationId>-<name>-repartition`, shared by every
 * aggregation of the grouping, and, for groupBy(), its key-selecting node. Without a name, every aggregation that needs
 * to repartition has its own topic, named after its `storeName` if it has one.
 */
export interface Grouped<K, V> extends NamedOptions {
	key?: Codec<K>
	value?: Codec<V>
}

/** How an operator's result is stored. */
export interface Materialized<K, V> {
	/**
	 * Names the store and so its changelog topic `<applicationId>-<storeName>-changelog` and its persisted state; same
	 * rule as NamedOptions.name. Default: generated in the order of the DSL calls, so prefer a name for state that must
	 * survive changes of the topology.
	 */
	storeName?: string
	key?: Codec<K>
	value?: Codec<V>
	/** Changelog backing for fault-tolerant state. Default: true (enabled). */
	changelog?: boolean | ChangelogConfig
	/**
	 * Windowed aggregations: how long the store keeps windows. Default and minimum: size + grace for time windows,
	 * gap + grace for sessions, twice the time difference + grace for sliding windows.
	 */
	retention?: WindowDuration
}

/** Options of an aggregation. */
export interface AggregateOptions<K, V> extends Materialized<K, V>, NamedOptions {}

/** 'on-update': every update of a window is forwarded; 'on-window-close': only the final result, once the window closed. */
export type EmitStrategy = 'on-update' | 'on-window-close'

/** Options of a windowed aggregation. */
export interface WindowedAggregateOptions<K, V> extends AggregateOptions<K, V> {
	/** Default 'on-update'. */
	emit?: EmitStrategy
}

/** Options of a table operation whose result can be materialized (filter, mapValues, joins). */
export interface TableOptions<K, V> extends NamedOptions {
	/** With a store name the result table is materialized. */
	materialized?: Materialized<K, V>
}

/**
 * The stores a processor or transformer uses: definitions made by `Stores` (registered with the app the first time one
 * is named) or the names of registered stores (`app.addStateStore()`, a `Materialized.storeName`). Processors sharing
 * a store run in one task per partition, so the topics they read must be co-partitioned.
 */
export type ConnectedStores = readonly (StoreDefinition<StateStore> | string)[]

/** Options of KTable.transformValues. */
export interface TransformValuesOptions<K, V> extends TableOptions<K, V> {
	/**
	 * The stores the transformer reads through its context. A table that is not materialized is read by running another
	 * transformer instance in the processors that look it up (joins): they are connected to these stores too.
	 */
	stores?: ConnectedStores
}

/** Options of process() and processValues(). */
export interface ProcessOptions extends NamedOptions {
	/** The stores the processor reads and writes through its context. */
	stores?: ConnectedStores
}

/**
 * Codecs and stores of a stream-stream join. `name` names its nodes after it (`<name>-this-join`, `<name>-merge`, ...)
 * and the repartition topics of re-keyed sides, `<name>-left` and `<name>-right`.
 */
export interface StreamJoined<K, V, VO> extends NamedOptions {
	/**
	 * Base name of the join stores: `<storeName>-this-join-store` / `-other-join-store`. Default: `name`, else
	 * generated.
	 */
	storeName?: string
	key?: Codec<K>
	value?: Codec<V>
	otherValue?: Codec<VO>
	/** Changelog backing of the join stores. Default: true (enabled). */
	changelog?: boolean | ChangelogConfig
}

/**
 * Codecs of a stream-table join; also the deprecated options of a stream-stream join. `name` names the join node and
 * the repartition topic of a re-keyed stream.
 */
export interface Joined<K, V, VO> extends NamedOptions {
	key?: Codec<K>
	value?: Codec<V>
	otherValue?: Codec<VO>
	/**
	 * @deprecated Stream-stream joins: pass `JoinWindows` as the third argument. TimeWindows w is
	 * `JoinWindows.of(w.size)`, SessionWindows s is `JoinWindows.of(s.gap)`; SlidingWindows are rejected.
	 */
	within?: TimeWindows | SessionWindows | SlidingWindows | JoinWindows
	/** @deprecated Stream-stream joins: use StreamJoined.changelog. */
	changelog?: boolean | ChangelogConfig
}

/**
 * How `repartition()` writes and reads back its internal topic `<applicationId>-<name>-repartition` (generated
 * `<applicationId>-repartition-<n>-repartition` without a name).
 */
export interface Repartitioned<K, V> extends NamedOptions {
	/**
	 * Default: the largest partition count of the topics the stream comes from, or the count of the topics it is
	 * co-partitioned with downstream (joins, aggregations), which must equal an explicit count.
	 */
	numberOfPartitions?: number
	/** Default: the stream's codecs. */
	key?: Codec<K>
	value?: Codec<V>
	/** Default: the producer's partitioning of the encoded key; the base key's partition for a windowed key codec. */
	partitioner?: RecordPartitioner<K, V>
}

/**
 * When `suppress()` emits. 'window-close' emits only final results of windowed tables and never early: a full
 * buffer stops the app. 'time-limit' emits the latest value of a key once `timeLimit` passed since its first
 * buffered update; a full buffer emits early by default. `name` names the processor and its buffer store
 * `<name>-store`.
 */
export type SuppressOptions =
	| { until: 'window-close'; name?: string; maxRecords?: number; maxBytes?: number }
	| {
			until: 'time-limit'
			timeLimit: WindowDuration
			name?: string
			maxRecords?: number
			maxBytes?: number
			/** Default 'emit-early'. */
			whenFull?: 'emit-early' | 'shutdown'
	  }

/** Options of `split()`. */
export interface SplitOptions {
	/** Names the split node and prefixes every branch name. Default: `KSTREAM-BRANCH-<n>`. */
	name?: string
}

/** Options of one branch of `split()`. */
export interface BranchOptions<K, V> {
	/**
	 * Appended to the split name, it names the branch node and its key in the result. Default: the 1-based position of
	 * the branch, `0` for the default branch.
	 */
	name?: string
	/**
	 * Builds on the branch; what it returns replaces the branch in the result, and a chain that returns nothing (for
	 * example one ending in `to()`) leaves the branch out of the result.
	 */
	chain?: (stream: KStream<K, V>) => KStream<K, V> | void
}
