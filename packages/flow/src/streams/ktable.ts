import type { KTable, KStream, KGroupedTable, Grouped, Joined, KeyValue, Produced } from '@/types.js'
import type { KeyValueStore, StateStoreProvider } from '@/state.js'
import type { Codec } from '@/codec.js'
import {
	PassThroughNode,
	MapValuesNode,
	FilterNode,
	type StreamFormat,
	type StreamRecord,
	type WorkerContext,
	type OutputProcessor,
} from '@/processors/index.js'
import { TableGroupByNode, type GroupedTableMapping, groupedTableMappingCodec } from '@/processors/table.js'
import {
	TableTableJoinNode,
	TableTableLeftJoinNode,
	TableTableLeftJoinOtherNode,
	TableTableOuterJoinLeftNode,
	TableTableOuterJoinRightNode,
} from '@/processors/joins/index.js'
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
}

export class KTableImpl<K, V> implements KTable<K, V> {
	readonly storeRef: { store: KeyValueStore<K, V> | null }

	constructor(
		readonly app: FlowAppInterface,
		readonly node: OutputProcessor<K, V>,
		readonly format: StreamFormat<K, V>,
		storeRef?: { store: KeyValueStore<K, V> | null }
	) {
		this.storeRef = storeRef ?? { store: null }
	}

	toStream(): KStream<K, V> {
		return new KStreamImpl(this.app, this.node, this.format)
	}

	mapValues<V2>(fn: (value: V | null) => V2 | null): KTable<K, V2> {
		const node = new MapValuesNode<K, V, V2>(fn)
		this.node.connect(node)
		return new KTableImpl<K, V2>(this.app, node, { keyCodec: this.format.keyCodec })
	}

	filter(fn: (key: K | null, value: V | null) => boolean): KTable<K, V> {
		const node = new FilterNode<K, V>(fn)
		this.node.connect(node)
		return new KTableImpl<K, V>(this.app, node, this.format)
	}

	groupBy<K2>(fn: (key: K, value: V) => KeyValue<K2, V>, options?: Grouped<K2, V>): KGroupedTable<K2, V> {
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

		// Create a key mapping store to track source key -> grouped key + source value.
		const mappingStoreName = `groupby-mapping-${this.app.nextStoreId()}`
		const mappingStore = this.app.getOrCreateStore<K, GroupedTableMapping<K2, V>>(
			mappingStoreName,
			sourceKeyCodec,
			groupedTableMappingCodec(groupedKeyCodec, valueCodec)
		)
		const mappingStoreRef = { store: mappingStore }

		// Use TableGroupByNode to properly handle retractions
		const groupByNode = new TableGroupByNode<K, V, K2>(fn, mappingStoreRef, groupedKeyCodec)
		this.node.connect(groupByNode)

		return new KGroupedTableImpl<K2, V>(
			this.app,
			groupByNode,
			{ keyCodec: groupedKeyCodec, valueCodec },
			mappingStoreRef as { store: KeyValueStore<unknown, GroupedTableMapping<K2, V>> | null },
			groupedKeyCodec
		)
	}

	join<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V, otherValue: V2) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR> {
		void options

		if (!(other instanceof KTableImpl)) {
			throw new Error('Can only join with KTableImpl instances')
		}

		const otherTable = other as KTableImpl<K, V2>
		if (!this.storeRef.store || !otherTable.storeRef.store) {
			throw new Error('Cannot join tables without state stores')
		}

		// Create a PassThrough node to merge results from both join paths
		const mergeNode = new PassThroughNode<K, VR>()

		// When this table updates, look up other table
		const leftJoinNode = new TableTableJoinNode<K, V, V2, VR>(otherTable.storeRef, joiner)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// When other table updates, look up this table (reverse joiner args)
		const rightJoinNode = new TableTableJoinNode<K, V2, V, VR>(this.storeRef, (rightVal, leftVal) =>
			joiner(leftVal, rightVal)
		)
		otherTable.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		return new KTableImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec })
	}

	leftJoin<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR> {
		void options

		if (!(other instanceof KTableImpl)) {
			throw new Error('Can only join with KTableImpl instances')
		}

		const otherTable = other as KTableImpl<K, V2>
		if (!this.storeRef.store || !otherTable.storeRef.store) {
			throw new Error('Cannot join tables without state stores')
		}

		// Create a PassThrough node to merge results from both join paths
		const mergeNode = new PassThroughNode<K, VR>()

		// When this (left) table updates, look up other (right) table - left join
		const leftJoinNode = new TableTableLeftJoinNode<K, V, V2, VR>(otherTable.storeRef, joiner)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// When other (right) table updates, look up this (left) table. The result only exists
		// while the left row exists; a right-side tombstone re-emits joiner(left, null) instead
		// of deleting the still-valid left row.
		const rightJoinNode = new TableTableLeftJoinOtherNode<K, V, V2, VR>(this.storeRef, joiner)
		otherTable.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		return new KTableImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec })
	}

	outerJoin<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V | null, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR> {
		void options

		if (!(other instanceof KTableImpl)) {
			throw new Error('Can only join with KTableImpl instances')
		}

		const otherTable = other as KTableImpl<K, V2>
		if (!this.storeRef.store || !otherTable.storeRef.store) {
			throw new Error('Cannot join tables without state stores')
		}

		// Create a PassThrough node to merge results from both join paths
		const mergeNode = new PassThroughNode<K, VR>()

		// When this (left) table updates, look up other (right) table. A left-side tombstone
		// only deletes the result when the right side is also absent; otherwise it re-emits
		// joiner(null, right).
		const leftJoinNode = new TableTableOuterJoinLeftNode<K, V, V2, VR>(otherTable.storeRef, joiner)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// When other (right) table updates, look up this (left) table
		const rightJoinNode = new TableTableOuterJoinRightNode<K, V, V2, VR>(this.storeRef, joiner)
		otherTable.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		return new KTableImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec })
	}
}
