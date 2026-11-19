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

/** Computes a new value from a value (Kafka Streams' `ValueMapper`): called with the value only, always. */
export type ValueMapper<V, VR> = (value: V) => VR
/** Computes a new value from a record's key and value (Kafka Streams' `ValueMapperWithKey`); the key is read-only. */
export type ValueMapperWithKey<K, V, VR> = (key: K, value: V) => VR
/** Joins a value with the value of the other side (Kafka Streams' `ValueJoiner`): called with the values only, always. */
export type ValueJoiner<V, VO, VR> = (value: V, otherValue: VO) => VR
/**
 * Joins a value with the value of the other side, knowing the record's key (Kafka Streams' `ValueJoinerWithKey`), for
 * `joinWithKey()`, `leftJoinWithKey()` and `outerJoinWithKey()`; the key is read-only.
 */
export type ValueJoinerWithKey<K, V, VO, VR> = (key: K, value: V, otherValue: VO) => VR
/** Computes something, such as the key a stream-global table join looks up, from a record's key and value. */
export type KeyValueMapper<K, V, R> = (key: K, value: V) => R

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
	/** One new value per record, computed from the value alone; the key is kept. */
	mapValues<VR>(mapper: ValueMapper<V | null, VR>, options?: NamedOptions): KStream<K, VR>
	/** One new value per record, computed from the key and the value; the key is kept. */
	mapValuesWithKey<VR>(mapper: ValueMapperWithKey<K | null, V | null, VR>, options?: NamedOptions): KStream<K, VR>
	/** Zero or more records per record. Changes keys: a key-based operation downstream repartitions first. */
	flatMap<KR, VR>(
		mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
		options?: NamedOptions
	): KStream<KR, VR>
	/** Zero or more values per record, computed from the value alone, each with the record's key. */
	flatMapValues<VR>(mapper: ValueMapper<V | null, Iterable<VR>>, options?: NamedOptions): KStream<K, VR>
	/** Zero or more values per record, computed from the key and the value, each with the record's key. */
	flatMapValuesWithKey<VR>(
		mapper: ValueMapperWithKey<K | null, V | null, Iterable<VR>>,
		options?: NamedOptions
	): KStream<K, VR>
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
	join<VT, VR>(table: KTable<K, VT>, joiner: ValueJoiner<V, VT, VR>, options?: Joined<K, V, VT>): KStream<K, VR>
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
		joiner: ValueJoiner<V, GV, VR>,
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
	/** Stream-table left join: records the table has no value for are joined with null. */
	leftJoin<VT, VR>(
		table: KTable<K, VT>,
		joiner: ValueJoiner<V, VT | null, VR>,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	/**
	 * Stream-global table left join: like the inner join, but records the table has no value for, and records whose
	 * selector returns null (KIP-962), are joined with null. Records with a null value are dropped.
	 */
	leftJoin<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: ValueJoiner<V, GV | null, VR>,
		options?: NamedOptions
	): KStream<K, VR>
	/** @deprecated Pass a key selector: `leftJoin(globalTable, (key) => key, joiner)` (the record key is looked up). */
	leftJoin<GV, VR>(globalTable: GlobalKTable<K, GV>, joiner: (value: V, globalValue: GV | null) => VR): KStream<K, VR>

	/** Like the windowed stream-stream `join()`, with a joiner that also receives the records' key (Kafka Streams' `ValueJoinerWithKey`). */
	joinWithKey<VO, VR>(
		other: KStream<K, VO>,
		joiner: ValueJoinerWithKey<K, V, VO, VR>,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>
	/** Like the stream-table `join()`, with a joiner that also receives the record's key (Kafka Streams' `ValueJoinerWithKey`). */
	joinWithKey<VT, VR>(
		table: KTable<K, VT>,
		joiner: ValueJoinerWithKey<K, V, VT, VR>,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	/** Like the stream-global table `join()`, with a joiner that also receives the record's key (not the selected one). */
	joinWithKey<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: ValueJoinerWithKey<K | null, V, GV, VR>,
		options?: NamedOptions
	): KStream<K, VR>
	/**
	 * Like the windowed stream-stream `leftJoin()`, with a joiner that also receives the records' key, null for a record
	 * of this stream without a key (joined with null, KIP-962).
	 */
	leftJoinWithKey<VO, VR>(
		other: KStream<K, VO>,
		joiner: ValueJoinerWithKey<K | null, V, VO | null, VR>,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>
	/** Like the stream-table `leftJoin()`, with a joiner that also receives the record's key, null in a null-key join. */
	leftJoinWithKey<VT, VR>(
		table: KTable<K, VT>,
		joiner: ValueJoinerWithKey<K | null, V, VT | null, VR>,
		options?: Joined<K, V, VT>
	): KStream<K, VR>
	/** Like the stream-global table `leftJoin()`, with a joiner that also receives the record's key (not the selected one). */
	leftJoinWithKey<GK, GV, VR>(
		globalTable: GlobalKTable<GK, GV>,
		keySelector: KeyValueMapper<K | null, V, GK | null>,
		joiner: ValueJoinerWithKey<K | null, V, GV | null, VR>,
		options?: NamedOptions
	): KStream<K, VR>

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
	/**
	 * Like `outerJoin()`, with a joiner that also receives the records' key, null for a record without a key (joined
	 * with null, KIP-962).
	 */
	outerJoinWithKey<VO, VR>(
		other: KStream<K, VO>,
		joiner: ValueJoinerWithKey<K | null, V | null, VO | null, VR>,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR>

	/**
	 * Runs a processor of the Processor API (Kafka Streams' `process()`): every task creates its own instance with
	 * `supplier()` (which must return a new processor per call), calls `init(context)`, then `process(record)` for each
	 * record, and `close()` when the task closes. Through its context the processor forwards any number of records, reads
	 * and writes the stores of `options.stores`, schedules stream-time and wall-clock punctuators, and requests commits.
	 * The processor may change keys, so a key-based operation downstream repartitions first, and neither codec is known
	 * downstream.
	 */
	process<KR, VR>(supplier: ProcessorSupplier<K | null, V | null, KR, VR>, options?: ProcessOptions): KStream<KR, VR>
	/**
	 * Like `process()`, with a processor that cannot change keys: it forwards FixedKeyRecords derived from its input
	 * (`withValue()`, `withTimestamp()`, `withHeaders()`), so the key codec and the repartition flag are kept.
	 */
	processValues<VR>(
		supplier: FixedKeyProcessorSupplier<K | null, V | null, VR>,
		options?: ProcessOptions
	): KStream<K, VR>
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
