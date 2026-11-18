import type { KGroupedTable } from '@/api/grouped.js'
import type { KStream, KeyValue } from '@/api/kstream.js'
import type {
	Grouped,
	Joined,
	NamedOptions,
	SuppressOptions,
	TableOptions,
	TransformValuesOptions,
} from '@/api/options.js'
import type { ValueTransformerWithKeySupplier } from '@/processor/api.js'

/**
 * A changelog stream read as a table: the latest value per key; a null value deletes the key. Deletes are propagated
 * without calling the callbacks of `filter()`, `filterNot()`, `mapValues()`, `groupBy()` and the joins (a joiner gets
 * null only for a side that has no value, as its type says). `transformValues()` and `toStream(mapper)` get them, as a
 * null value (Kafka Streams). Undefined, as a value or as the result of a mapper, transformer or joiner, is no value,
 * as null is.
 */
export interface KTable<K, V> {
	/** The entries the predicate accepts: an update it rejects deletes the key. */
	filter(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V>
	/** The entries the predicate rejects: an update it accepts deletes the key. */
	filterNot(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V>
	/**
	 * A null (or undefined) result deletes the key. A mapper declaring two parameters gets the key too (give a
	 * two-parameter lambda explicit parameter types).
	 */
	mapValues<VR>(mapper: (value: V) => VR | null, options?: TableOptions<K, VR>): KTable<K, VR>
	mapValues<VR>(mapper: (key: K, value: V) => VR | null, options?: TableOptions<K, VR>): KTable<K, VR>
	/**
	 * Kafka Streams' `transformValues()`: every task creates a transformer with `supplier()` (which must return a new
	 * one per call) and calls `transform(key, value)` once per update, tombstones included (`value` null); the result
	 * is the key's new value, null deleting it. The transformer's context reads the stores of `options.stores` and
	 * schedules punctuators, but cannot forward. Without a materialized store, a lookup of the result table (a join)
	 * transforms the parent's current value with a transformer instance of its own.
	 */
	transformValues<VR>(
		supplier: ValueTransformerWithKeySupplier<K, V, VR>,
		options?: TransformValuesOptions<K, VR>
	): KTable<K, VR>
	/** Every update as a record with the key `mapper` picks: a key-based operation downstream repartitions first. */
	toStream<KR>(mapper: (key: K, value: V | null) => KR, options?: NamedOptions): KStream<KR, V>
	/** Every update as a record; deletes become null values. */
	toStream(options?: NamedOptions): KStream<K, V>
	/** Always repartitions; the mapper may change the value type. */
	groupBy<KR, VR>(selector: (key: K, value: V) => KeyValue<KR, VR>, options?: Grouped<KR, VR>): KGroupedTable<KR, VR>
	/** Both tables must be co-partitioned. The old Joined options are accepted; their changelog setting is ignored. */
	join<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V, otherValue: VO) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR>
	leftJoin<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V, otherValue: VO | null) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR>
	outerJoin<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V | null, otherValue: VO | null) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR>
	/** Holds back updates until a window closes or a time limit passed. */
	suppress(options: SuppressOptions): KTable<K, V>
}

/**
 * A table every instance reads in full, outside the consumer group, and keeps up to date while it runs: the
 * latest value per key of its topic (a null value deletes the key, a record without a key is skipped). Only
 * usable as the lookup side of a stream join (`KStream.join/leftJoin(globalTable, keySelector, joiner)`); it
 * cannot be streamed.
 */
export interface GlobalKTable<K, V> {
	/** Name of the table's store: `materialized.storeName`, else `global-table-<topic>-<n>`. */
	readonly storeName: string
	/** Phantom: carries the key and value types. */
	readonly __global?: { readonly key: K; readonly value: V }
}
