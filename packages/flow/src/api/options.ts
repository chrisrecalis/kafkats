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

/** Every operator takes a name: it fixes the node name and, where the operator has them, store and topic names. */
export interface NamedOptions {
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

/** How a stream is grouped: `name` names its repartition topic, shared by every aggregation of the grouping. */
export interface Grouped<K, V> extends NamedOptions {
	key?: Codec<K>
	value?: Codec<V>
}

/** How an operator's result is stored. */
export interface Materialized<K, V> {
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

/** Options of KTable.transformValues. */
export interface TransformValuesOptions<K, V> extends TableOptions<K, V> {
	/** Stores the transformer may use. */
	stores?: readonly string[]
}

/** Options of process() and processValues(). */
export interface ProcessOptions extends NamedOptions {
	/** Stores the processor may use. */
	stores?: readonly StoreDefinition<StateStore>[] | readonly string[]
}

/** Codecs and stores of a stream-stream join. */
export interface StreamJoined<K, V, VO> extends NamedOptions {
	/** Base name of the join stores: `<storeName>-this-join-store` / `-other-join-store`. */
	storeName?: string
	key?: Codec<K>
	value?: Codec<V>
	otherValue?: Codec<VO>
	/** Changelog backing of the join stores. Default: true (enabled). */
	changelog?: boolean | ChangelogConfig
}

/** Codecs of a stream-table join; also the deprecated options of a stream-stream join. */
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

/** How `repartition()` writes and reads back its internal topic. */
export interface Repartitioned<K, V> extends NamedOptions {
	/** Default: the partition count of the upstream topics. */
	numberOfPartitions?: number
	key?: Codec<K>
	value?: Codec<V>
	partitioner?: RecordPartitioner<K, V>
}

/**
 * When `suppress()` emits. 'window-close' emits only final results of windowed tables and never early: a full
 * buffer stops the app. 'time-limit' emits the latest value of a key once `timeLimit` passed since its first
 * buffered update; a full buffer emits early by default.
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

/** Options of `split()`: the name prefixes every branch name in the result. */
export interface SplitOptions {
	name?: string
}

/** Options of one branch of `split()`. */
export interface BranchOptions<K, V> {
	/** Default: the 1-based position of the branch. */
	name?: string
	/** Applied to the branch; a chain that returns nothing omits the branch from the result. */
	chain?: (stream: KStream<K, V>) => KStream<K, V> | void
}
