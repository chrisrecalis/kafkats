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
 * A changelog stream read as a table: the latest value per key; a null value deletes the key. Callbacks
 * never see null values (tombstones are propagated without calling them).
 */
export interface KTable<K, V> {
	filter(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V>
	filterNot(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V>
	/** A null result deletes the key. */
	mapValues<VR>(mapper: (value: V) => VR | null, options?: TableOptions<K, VR>): KTable<K, VR>
	mapValues<VR>(mapper: (key: K, value: V) => VR | null, options?: TableOptions<K, VR>): KTable<K, VR>
	/** One transformer result per update; tombstones reach the transformer. */
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
 * A table every instance reads in full, outside the consumer group. Only usable as the lookup side of a
 * stream join; it cannot be streamed.
 */
export interface GlobalKTable<K, V> {
	/** Name of the table's store. */
	readonly storeName: string
	/** Phantom: carries the key and value types. */
	readonly __global?: { readonly key: K; readonly value: V }
}
