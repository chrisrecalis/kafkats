import type { KTable, KStream, KGroupedTable, Grouped, KeyValue, Produced, Consumed } from '@/types.js'
import type { KeyValueStore, SessionStore, StateStoreProvider, WindowStore } from '@/state.js'
import type { Codec } from '@/codec.js'
import {
	PassThroughNode,
	TableMapValuesNode,
	TableFilterNode,
	TableGroupByNode,
	TableTableJoinNode,
	TableTableLeftJoinNode,
	TableTableLeftJoinOtherNode,
	TableTableOuterJoinNode,
	groupedTableMappingCodec,
	type GroupedTableMapping,
	type OutputProcessor,
	type Processor,
	type StoreRef,
	type StreamFormat,
	type StreamRecord,
	type WorkerContext,
} from '@/processors/index.js'
import { KGroupedTableImpl } from '@/streams/grouped.js'
// Note: Circular import with kstream.ts - ESM handles this at runtime
import { KStreamImpl } from '@/streams/kstream.js'

// Interface to avoid circular import with FlowAppImpl
// Provides access to app internals needed by stream implementations
export interface FlowAppInterface {
	getOrCreateStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		changelog?: boolean | import('@/changelog.js').ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions?: boolean
	): KeyValueStore<K, V>
	/** Create (or fetch) a window store, wiring up its changelog + restoration like getOrCreateStore. */
	getOrCreateWindowStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		windowOptions: { retentionMs: number; windowSizeMs: number },
		changelog?: boolean | import('@/changelog.js').ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions?: boolean
	): WindowStore<K, V>
	/** Create (or fetch) a session store, wiring up its changelog + restoration like getOrCreateStore. */
	getOrCreateSessionStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		sessionOptions: { retentionMs: number },
		changelog?: boolean | import('@/changelog.js').ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions?: boolean
	): SessionStore<K, V>
	/** Get the next store counter value and increment */
	nextStoreId(): number
	/** State store provider for creating window and session stores */
	readonly stateStoreProvider: StateStoreProvider
	/** Map of all registered state stores */
	readonly stateStores: Map<string, KeyValueStore<unknown, unknown>>
	/** Send a record to a topic (used by ProduceNode) */
	sendToTopic<K, V>(
		worker: WorkerContext,
		topic: string,
		record: StreamRecord<K, V>,
		options: Produced<K, V> | undefined,
		format: StreamFormat<K, V>
	): Promise<void>
	/** True when processingGuarantee is exactly_once. */
	isExactlyOnce(): boolean
	/** Declare topics that must have the same partition count (they share state partitions). */
	requireCopartitioned(topics: Set<string>): void
	/** Register an internal repartition topic fed by the given topics; returns its name. */
	createRepartitionTopic(name: string | undefined, upstreamTopics: Set<string>): string
	/** Mark a user topic written by through() as one that must be read from the beginning. */
	registerThroughTopic(topic: string): void
	/** Create a stream from a topic (used to re-consume repartition topics). */
	stream<K = Buffer, V = Buffer>(source: string, options?: Consumed<K, V>): KStream<K, V>
}

export class KTableImpl<K, V> implements KTable<K, V> {
	readonly storeRef: StoreRef<KeyValueStore<K, V>>

	constructor(
		readonly app: FlowAppInterface,
		readonly node: OutputProcessor<K, V>,
		readonly format: StreamFormat<K, V>,
		storeRef?: StoreRef<KeyValueStore<K, V>>,
		/** Source topics the table's state is co-partitioned with. */
		readonly sourceTopics: Set<string> = new Set(),
		/** True for globalTable(): fully replicated, only usable as the lookup side of a stream join. */
		readonly global: boolean = false
	) {
		this.storeRef = storeRef ?? { store: null }
	}

	assertNotGlobal(operation: string): void {
		if (this.global) {
			throw new Error(
				`${operation} is not supported on a globalTable(); a global table can only be joined from a stream`
			)
		}
	}

	toStream(): KStream<K, V> {
		this.assertNotGlobal('toStream()')
		return new KStreamImpl(this.app, this.node, this.format, this.sourceTopics)
	}

	mapValues<V2>(fn: (value: V | null) => V2 | null): KTable<K, V2> {
		this.assertNotGlobal('mapValues()')
		const node = new TableMapValuesNode<K, V, V2>(fn)
		this.node.connect(node)
		return new KTableImpl<K, V2>(this.app, node, { keyCodec: this.format.keyCodec }, undefined, this.sourceTopics)
	}

	filter(fn: (key: K | null, value: V | null) => boolean): KTable<K, V> {
		this.assertNotGlobal('filter()')
		const node = new TableFilterNode<K, V>(fn)
		this.node.connect(node)
		return new KTableImpl<K, V>(this.app, node, this.format, undefined, this.sourceTopics)
	}

	groupBy<K2>(fn: (key: K, value: V) => KeyValue<K2, V>, options?: Grouped<K2, V>): KGroupedTable<K2, V> {
		this.assertNotGlobal('groupBy()')
		const sourceKeyCodec = this.format.keyCodec
		const groupedKeyCodec = options?.key
		const valueCodec = options?.value ?? this.format.valueCodec

		if (!sourceKeyCodec) {
			throw new Error('KTable.groupBy() requires a source key codec')
		}
		if (!groupedKeyCodec) {
			throw new Error('KTable.groupBy() requires a grouped key codec in options')
		}
		if (!valueCodec) {
			throw new Error('KTable.groupBy() requires a value codec.')
		}

		// Source key -> (grouped key, value) of the row's last contribution, so an update or delete
		// can retract it. Partitioned like the table itself.
		const mappingStore = this.app.getOrCreateStore<K, GroupedTableMapping<K2, V>>(
			`groupby-mapping-${this.app.nextStoreId()}`,
			sourceKeyCodec,
			groupedTableMappingCodec(groupedKeyCodec, valueCodec),
			undefined,
			this.sourceTopics
		)

		// Emits a retraction for the old grouping and an addition for the new one.
		const groupByNode = new TableGroupByNode<K, V, K2>(fn, { store: mappingStore })
		this.node.connect(groupByNode)

		// Rows of one group come from every source partition: route them through a repartition
		// topic keyed by the grouped key so each group is aggregated in exactly one place.
		const deltas = new KStreamImpl<K2, V>(
			this.app,
			groupByNode,
			{ keyCodec: groupedKeyCodec, valueCodec },
			this.sourceTopics,
			true
		).repartitioned(options?.name, groupedKeyCodec, valueCodec)

		return new KGroupedTableImpl<K2, V>(this.app, deltas.node, deltas.format, deltas.sourceTopics)
	}

	join<V2, VR>(other: KTable<K, V2>, joiner: (value: V, otherValue: V2) => VR): KTable<K, VR> {
		const otherTable = this.joinable(other)
		return this.joined(
			otherTable,
			new TableTableJoinNode<K, V, V2, VR>(otherTable.storeRef, joiner),
			new TableTableJoinNode<K, V2, V, VR>(this.storeRef, (value, otherValue) => joiner(otherValue, value))
		)
	}

	leftJoin<V2, VR>(other: KTable<K, V2>, joiner: (value: V, otherValue: V2 | null) => VR): KTable<K, VR> {
		const otherTable = this.joinable(other)
		// An update of the right table re-joins the left row; the result only exists while the left
		// row exists, and a right-side tombstone re-emits joiner(left, null) instead of deleting it.
		return this.joined(
			otherTable,
			new TableTableLeftJoinNode<K, V, V2, VR>(otherTable.storeRef, joiner),
			new TableTableLeftJoinOtherNode<K, V, V2, VR>(this.storeRef, joiner)
		)
	}

	outerJoin<V2, VR>(other: KTable<K, V2>, joiner: (value: V | null, otherValue: V2 | null) => VR): KTable<K, VR> {
		const otherTable = this.joinable(other)
		return this.joined(
			otherTable,
			new TableTableOuterJoinNode<K, V, V2, VR>(otherTable.storeRef, joiner),
			new TableTableOuterJoinNode<K, V2, V, VR>(this.storeRef, (value, otherValue) => joiner(otherValue, value))
		)
	}

	/** Table-table joins look up the other side's state by key, so both sides need a state store. */
	private joinable<V2>(other: KTable<K, V2>): KTableImpl<K, V2> {
		if (!(other instanceof KTableImpl)) {
			throw new Error('Can only join with KTableImpl instances')
		}
		const otherTable = other as KTableImpl<K, V2>
		if (!this.storeRef.store || !otherTable.storeRef.store) {
			throw new Error('Cannot join tables without state stores')
		}
		this.assertNotGlobal('A table-table join')
		otherTable.assertNotGlobal('A table-table join')
		return otherTable
	}

	/**
	 * Connect the join node of each side and merge their results. Both sides must be
	 * co-partitioned: each side's node looks up the other side's state partition.
	 */
	private joined<V2, VR>(
		other: KTableImpl<K, V2>,
		thisSide: Processor<K, V, K, VR>,
		otherSide: Processor<K, V2, K, VR>
	): KTable<K, VR> {
		const topics = new Set([...this.sourceTopics, ...other.sourceTopics])
		this.app.requireCopartitioned(topics)

		const mergeNode = new PassThroughNode<K, VR>()
		this.node.connect(thisSide)
		thisSide.connect(mergeNode)
		other.node.connect(otherSide)
		otherSide.connect(mergeNode)

		return new KTableImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec }, undefined, topics)
	}
}
