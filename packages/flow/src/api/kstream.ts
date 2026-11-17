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
/** Computes something, such as the key a stream-global table join looks up, from a record's key and value. */
export type KeyValueMapper<K, V, R> = (key: K, value: V) => R

/**
 * An unbounded stream of key-value records. Keys and values may be null (a null value is a tombstone in a
 * topic read as a table). Every callback takes `(key, value)`.
 */
export interface KStream<K, V> {
	filter(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V>
	filterNot(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V>
	/** Changes keys: a key-based operation downstream repartitions first. */
	map<KR, VR>(mapper: (key: K | null, value: V | null) => KeyValue<KR, VR>, options?: NamedOptions): KStream<KR, VR>
	mapValues<VR>(mapper: (value: V | null) => VR, options?: NamedOptions): KStream<K, VR>
	mapValues<VR>(mapper: (key: K | null, value: V | null) => VR, options?: NamedOptions): KStream<K, VR>
	/** Changes keys: a key-based operation downstream repartitions first. */
	flatMap<KR, VR>(
		mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
		options?: NamedOptions
	): KStream<KR, VR>
	flatMapValues<VR>(mapper: (value: V | null) => Iterable<VR>, options?: NamedOptions): KStream<K, VR>
	flatMapValues<VR>(mapper: (key: K | null, value: V | null) => Iterable<VR>, options?: NamedOptions): KStream<K, VR>
	peek(action: (key: K | null, value: V | null) => void, options?: NamedOptions): KStream<K, V>
	/** Terminal. */
	foreach(action: (key: K | null, value: V | null) => void, options?: NamedOptions): void
	/** Changes keys: a key-based operation downstream repartitions first. */
	selectKey<KR>(mapper: (key: K | null, value: V | null) => KR, options?: NamedOptions): KStream<KR, V>
	merge(other: KStream<K, V>, options?: NamedOptions): KStream<K, V>
	/** Routes each record to the first branch whose predicate matches. */
	split(options?: SplitOptions): BranchedKStream<K, V>
	/** @deprecated Use split(): records go to the first matching predicate; unmatched records are dropped. */
	branch(...predicates: ((key: K | null, value: V | null) => boolean)[]): KStream<K, V>[]
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
	/**
	 * Stream-global table inner join: each record looks up the global table's current value for the key
	 * `keySelector` picks and is joined with it. No repartitioning: every instance holds the whole table, so the
	 * topics need not be co-partitioned, and a key change upstream is still pending downstream. Records with a null
	 * value, records whose selector returns null and records the table has no value for are dropped. Only stream
	 * records produce results, with their key, timestamp and headers; the table is read as it is when the record is
	 * processed (it is not synchronized with the stream's time).
	 */
	join<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: (value: V, globalValue: GV) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** Stream-global table inner join whose joiner also receives the record's key (not the selected one). */
	join<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: (key: K | null, value: V, globalValue: GV) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** @deprecated Pass a key selector: `join(globalTable, (key) => key, joiner)` (the record key is looked up). */
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
	/**
	 * Stream-global table left join: like the inner join, but records the table has no value for, and records whose
	 * selector returns null (KIP-962), are joined with null. Records with a null value are dropped.
	 */
	leftJoin<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: (value: V, globalValue: GV | null) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** Stream-global table left join whose joiner also receives the record's key (not the selected one). */
	leftJoin<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: (key: K | null, value: V, globalValue: GV | null) => VR,
		options?: NamedOptions
	): KStream<K, VR>
	/** @deprecated Pass a key selector: `leftJoin(globalTable, (key) => key, joiner)` (the record key is looked up). */
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

/** Built by `split()`: add branches, then end with defaultBranch() or noDefaultBranch(). */
export interface BranchedKStream<K, V> {
	branch(predicate: (key: K | null, value: V | null) => boolean, options?: BranchOptions<K, V>): this
	/** Records no predicate matched go to this branch. The result is keyed `<splitName><branchName>`. */
	defaultBranch(options?: BranchOptions<K, V>): Record<string, KStream<K, V>>
	/** Records no predicate matched are dropped. The result is keyed `<splitName><branchName>`. */
	noDefaultBranch(): Record<string, KStream<K, V>>
}
