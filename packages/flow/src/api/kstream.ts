import type { KGroupedStream } from '@/api/grouped.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type {
	BranchOptions,
	Grouped,
	Joined,
	Materialized,
	NamedOptions,
	ProcessOptions,
	Produced,
	Repartitioned,
	SplitOptions,
	StreamJoined,
} from '@/api/options.js'
import type { JoinWindows } from '@/dsl/windows.js'
import type { FixedKeyProcessorSupplier, ProcessorSupplier } from '@/processor/api.js'
import type { TopicNameExtractor } from '@/topology/nodes.js'

export type KeyValue<K, V> = readonly [K, V]

/**
 * A value mapper with or without the key. At runtime a mapper declaring two or more parameters receives
 * `(key, value)`, otherwise `(value)` (parameters with defaults do not count). TypeScript infers the
 * parameters of a one-parameter lambda; give a two-parameter lambda explicit parameter types.
 */
export type ValueMapper<K, V, VR> = ((value: V) => VR) | ((key: K, value: V) => VR)
/** A value joiner with or without the key (same arity rule as ValueMapper). */
export type ValueJoiner<K, V, VO, VR> = ((value: V, otherValue: VO) => VR) | ((key: K, value: V, otherValue: VO) => VR)

/**
 * An unbounded stream of key-value records. Keys and values may be null (a null value is a tombstone in a
 * topic read as a table). Every callback takes `(key, value)`.
 */
export interface KStream<K, V> {
	/** The records the predicate accepts. */
	filter(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V>
	/** The records the predicate rejects. */
	filterNot(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V>
	/** Changes keys: a key-based operation downstream repartitions first. */
	map<KR, VR>(mapper: (key: K | null, value: V | null) => KeyValue<KR, VR>, options?: NamedOptions): KStream<KR, VR>
	/**
	 * One new value per record; the key is kept. A mapper declaring two parameters gets the key too (ValueMapper:
	 * give a two-parameter lambda explicit parameter types).
	 */
	mapValues<VR>(mapper: (value: V | null) => VR, options?: NamedOptions): KStream<K, VR>
	mapValues<VR>(mapper: (key: K | null, value: V | null) => VR, options?: NamedOptions): KStream<K, VR>
	/** Zero or more records per record. Changes keys: a key-based operation downstream repartitions first. */
	flatMap<KR, VR>(
		mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
		options?: NamedOptions
	): KStream<KR, VR>
	/** Zero or more values per record, each with the record's key; the mapper's arity as for mapValues(). */
	flatMapValues<VR>(mapper: (value: V | null) => Iterable<VR>, options?: NamedOptions): KStream<K, VR>
	flatMapValues<VR>(mapper: (key: K | null, value: V | null) => Iterable<VR>, options?: NamedOptions): KStream<K, VR>
	/** Calls the action for every record, then passes the record on. */
	peek(action: (key: K | null, value: V | null) => void, options?: NamedOptions): KStream<K, V>
	/** Calls the action for every record. Terminal. */
	foreach(action: (key: K | null, value: V | null) => void, options?: NamedOptions): void
	/** Changes keys: a key-based operation downstream repartitions first. */
	selectKey<KR>(mapper: (key: K | null, value: V | null) => KR, options?: NamedOptions): KStream<KR, V>
	merge(other: KStream<K, V>, options?: NamedOptions): KStream<K, V>
	/**
	 * Splits the stream: each record goes to the first branch whose predicate matches, else to the default branch, if
	 * any. Add branches with `branch()`, then end with `defaultBranch()` or `noDefaultBranch()`:
	 * ```ts
	 * const { 'kind-a': a, 'kind-other': other } = stream
	 * 	.split({ name: 'kind-' })
	 * 	.branch((key, value) => value?.type === 'a', { name: 'a' })
	 * 	.defaultBranch({ name: 'other' })
	 * ```
	 */
	split(options?: SplitOptions): BranchedKStream<K, V>
	/** @deprecated Use split(): records go to the first matching predicate; unmatched records are dropped. */
	branch(...predicates: ((key: K | null, value: V | null) => boolean)[]): KStream<K, V>[]
	/**
	 * Writes every record to `topic`, or to the topic the extractor names for the record. Codecs: `options` first,
	 * then the stream's; without either, only Buffers can be written.
	 */
	to(topic: string | TopicNameExtractor<K | null, V | null>, options?: Produced<K, V>): void
	/** @deprecated Use repartition(), or to() and stream(). Writes to a user topic and reads it back. */
	through(topic: string, options?: Produced<K, V>): KStream<K, V>
	/**
	 * Writes to and reads back the internal topic `<applicationId>-<name | repartition-N>-repartition`, even if the keys
	 * did not change; key-based operations downstream do not repartition again. Records with a null key are dropped
	 * unless a left or outer join is downstream.
	 */
	repartition(options?: Repartitioned<K, V>): KStream<K, V>
	toTable(options?: Materialized<K, V> & NamedOptions): KTable<K, V>
	/** Always repartitions. */
	groupBy<KR>(selector: (key: K | null, value: V | null) => KR, options?: Grouped<KR, V>): KGroupedStream<KR, V>
	/** Repartitions if the key changed upstream. */
	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V>

	/** Windowed stream-stream inner join. */
	join<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V, otherValue: VO) => VR,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>
	/** @deprecated Pass JoinWindows as the third argument and StreamJoined options. */
	join<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V, otherValue: VO) => VR,
		options: Joined<K, V, VO>
	): KStream<K, VR>
	/** Stream-table inner join: looks up the table's current value; only stream records produce results. */
	join<VT, VR>(
		table: KTable<K, VT>,
		joiner: (value: V, tableValue: VT) => VR,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	join<VT, VR>(
		table: KTable<K, VT>,
		joiner: (key: K, value: V, tableValue: VT) => VR,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	/** Stream-global table inner join on the key `keySelector` picks; no repartitioning. */
	join<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: (key: K | null, value: V) => GK | null,
		joiner: (value: V, globalValue: GV) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	join<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: (key: K | null, value: V) => GK | null,
		joiner: (key: K | null, value: V, globalValue: GV) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** @deprecated Pass a key selector: `join(globalTable, (key) => key, joiner)`. */
	join<GV, VR>(globalTable: GlobalKTable<K, GV>, joiner: (value: V, globalValue: GV) => VR): KStream<K, VR>

	/** Windowed stream-stream left join. */
	leftJoin<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V, otherValue: VO | null) => VR,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>
	/** @deprecated Pass JoinWindows as the third argument and StreamJoined options. */
	leftJoin<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V, otherValue: VO | null) => VR,
		options: Joined<K, V, VO>
	): KStream<K, VR>
	leftJoin<VT, VR>(
		table: KTable<K, VT>,
		joiner: (value: V, tableValue: VT | null) => VR,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	leftJoin<VT, VR>(
		table: KTable<K, VT>,
		joiner: (key: K | null, value: V, tableValue: VT | null) => VR,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	leftJoin<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: (key: K | null, value: V) => GK | null,
		joiner: (value: V, globalValue: GV | null) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	leftJoin<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: (key: K | null, value: V) => GK | null,
		joiner: (key: K | null, value: V, globalValue: GV | null) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** @deprecated Pass a key selector: `leftJoin(globalTable, (key) => key, joiner)`. */
	leftJoin<GV, VR>(globalTable: GlobalKTable<K, GV>, joiner: (value: V, globalValue: GV | null) => VR): KStream<K, VR>

	/** Windowed stream-stream outer join. */
	outerJoin<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V | null, otherValue: VO | null) => VR,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>
	/** @deprecated Pass JoinWindows as the third argument and StreamJoined options. */
	outerJoin<VO, VR>(
		other: KStream<K, VO>,
		joiner: (value: V | null, otherValue: VO | null) => VR,
		options: Joined<K, V, VO>
	): KStream<K, VR>

	/** Processor API. The processor may change keys: a key-based operation downstream repartitions first. */
	process<KR, VR>(supplier: ProcessorSupplier<K, V, KR, VR>, options?: ProcessOptions): KStream<KR, VR>
	/** Processor API with the key kept. */
	processValues<VR>(supplier: FixedKeyProcessorSupplier<K, V, VR>, options?: ProcessOptions): KStream<K, VR>
}

/**
 * Built by `split()` (Kafka Streams' BranchedKStream): add branches, then end with `defaultBranch()` or
 * `noDefaultBranch()`, which return the branches keyed `<split name><branch name>`. Without a split name, the prefix is
 * the generated name of the split node (`KSTREAM-BRANCH-<n>`), as in Kafka Streams: name the split to destructure the
 * result.
 */
export interface BranchedKStream<K, V> {
	/** Adds a branch for the records the predicate matches that no earlier branch took. */
	branch(predicate: (key: K | null, value: V | null) => boolean, options?: BranchOptions<K, V>): this
	/** Ends the split with a branch for the records no predicate matched; returns the branches. */
	defaultBranch(options?: BranchOptions<K, V>): Record<string, KStream<K, V>>
	/** Ends the split; records no predicate matched are dropped. Returns the branches. */
	noDefaultBranch(): Record<string, KStream<K, V>>
}
