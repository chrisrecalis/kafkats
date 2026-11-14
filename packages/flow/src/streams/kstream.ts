import type { Codec } from '@/codec.js'
import type { KStream, KTable, KGroupedStream, Produced, Grouped, Materialized, Joined, KeyValue } from '@/types.js'
import type { KeyValueStore, WindowStore } from '@/state.js'
import {
	type OutputProcessor,
	type Processor,
	type StoreRef,
	type StreamFormat,
	PassThroughNode,
	MapNode,
	MapValuesNode,
	FlatMapValuesNode,
	FilterNode,
	PeekNode,
	SelectKeyNode,
	BranchNode,
	ProduceNode,
	TableStateNode,
	StreamTableJoinNode,
	StreamTableLeftJoinNode,
	StreamStreamJoinNode,
	StreamStreamLeftJoinNode,
	StreamStreamOuterJoinNode,
} from '@/processors/index.js'
import { joinWindowMs as resolveJoinWindowMs } from '@/helpers.js'
import { KTableImpl } from '@/streams/ktable.js'
import { KGroupedStreamImpl } from '@/streams/grouped.js'
import type { TopologyBuilder } from '@/streams/topology-builder.js'

type WindowJoinNodes<K, V, V2, VR> = (
	leftStoreRef: StoreRef<WindowStore<K, V>>,
	rightStoreRef: StoreRef<WindowStore<K, V2>>,
	joinWindowMs: number
) => [Processor<K, V, K, VR>, Processor<K, V2, K, VR>]

export class KStreamImpl<K, V> implements KStream<K, V> {
	constructor(
		readonly builder: TopologyBuilder,
		readonly node: OutputProcessor<K, V>,
		readonly format: StreamFormat<K, V>,
		/** Source topics that feed into this stream. Used for changelog partition inference. */
		readonly sourceTopics: Set<string> = new Set(),
		/**
		 * True when the key may have changed since the records were read, i.e. records are no
		 * longer guaranteed to sit in the partition their key belongs to. Key-based stateful
		 * operations repartition such a stream first (through an internal topic).
		 */
		readonly repartitionRequired: boolean = false
	) {}

	/**
	 * Write the stream to an internal repartition topic and read it back, so that every record
	 * is processed in the partition its (new) key hashes to. State is kept per partition; without
	 * this, the same key arriving on different source partitions would be aggregated separately.
	 */
	repartitioned(name: string | undefined, keyCodec?: Codec<K>, valueCodec?: Codec<V>): KStreamImpl<K, V> {
		const key = keyCodec ?? this.format.keyCodec
		const value = valueCodec ?? this.format.valueCodec
		if (!key || !value) {
			throw new Error(
				'This stream was re-keyed and must be repartitioned before a key-based stateful operation, ' +
					'which requires key and value codecs. Provide them via the operation options ' +
					'(e.g. groupBy(fn, { key, value })).'
			)
		}
		const topic = this.builder.topology.createRepartitionTopic(name, this.sourceTopics)
		this.node.connect(new ProduceNode<K, V>(this.builder.output, topic, { key, value }, this.format))
		return this.builder.stream<K, V>(topic, { key, value }) as KStreamImpl<K, V>
	}

	private copartitioned(name?: string, keyCodec?: Codec<K>, valueCodec?: Codec<V>): KStreamImpl<K, V> {
		return this.repartitionRequired ? this.repartitioned(name, keyCodec, valueCodec) : this
	}

	/** Continue the stream through `node`, keeping its source topics. */
	private chain<K2, V2>(
		node: Processor<K, V, K2, V2>,
		format: StreamFormat<K2, V2>,
		repartitionRequired = this.repartitionRequired
	): KStreamImpl<K2, V2> {
		this.node.connect(node)
		return new KStreamImpl<K2, V2>(this.builder, node, format, this.sourceTopics, repartitionRequired)
	}

	map<K2, V2>(fn: (key: K | null, value: V | null) => KeyValue<K2, V2>): KStream<K2, V2> {
		return this.chain(new MapNode<K, V, K2, V2>(fn), {}, true)
	}

	mapValues<V2>(fn: (value: V | null) => V2 | null): KStream<K, V2> {
		return this.chain(new MapValuesNode<K, V, V2>(fn), { keyCodec: this.format.keyCodec })
	}

	flatMapValues<V2>(fn: (value: V | null) => Iterable<V2>): KStream<K, V2> {
		return this.chain(new FlatMapValuesNode<K, V, V2>(fn), { keyCodec: this.format.keyCodec })
	}

	filter(fn: (key: K | null, value: V | null) => boolean): KStream<K, V> {
		return this.chain(new FilterNode<K, V>(fn), this.format)
	}

	peek(fn: (key: K | null, value: V | null) => void): KStream<K, V> {
		return this.chain(new PeekNode<K, V>(fn), this.format)
	}

	selectKey<K2>(fn: (value: V | null, key: K | null) => K2): KStream<K2, V> {
		return this.chain(new SelectKeyNode<K, V, K2>(fn), { valueCodec: this.format.valueCodec }, true)
	}

	merge(other: KStream<K, V>): KStream<K, V> {
		const otherStream = ensureStream(other, this.builder)
		const node = new PassThroughNode<K, V>()
		this.node.connect(node)
		otherStream.node.connect(node)

		const keyCodec = this.format.keyCodec === otherStream.format.keyCodec ? this.format.keyCodec : undefined
		const valueCodec = this.format.valueCodec === otherStream.format.valueCodec ? this.format.valueCodec : undefined

		return new KStreamImpl<K, V>(
			this.builder,
			node,
			{ keyCodec, valueCodec },
			new Set([...this.sourceTopics, ...otherStream.sourceTopics]),
			this.repartitionRequired || otherStream.repartitionRequired
		)
	}

	branch(...predicates: Array<(key: K | null, value: V | null) => boolean>): KStream<K, V>[] {
		const node = new BranchNode<K, V>()
		this.node.connect(node)

		return predicates.map(predicate => {
			const branchNode = new PassThroughNode<K, V>()
			node.addBranch(predicate, branchNode)
			return new KStreamImpl<K, V>(
				this.builder,
				branchNode,
				this.format,
				this.sourceTopics,
				this.repartitionRequired
			)
		})
	}

	to(topic: string, options?: Produced<K, V>): void {
		this.node.connect(new ProduceNode<K, V>(this.builder.output, topic, options, this.format))
	}

	through(topic: string, options?: Produced<K, V>): KStream<K, V> {
		// Write to the topic, then continue from what is read back: downstream operations see
		// records in the partition they were written to (and after a restart, replayed from it).
		this.to(topic, options)
		this.builder.topology.registerThroughTopic(topic)
		return this.builder.stream<K, V>(topic, {
			key: options?.key ?? this.format.keyCodec,
			value: options?.value ?? this.format.valueCodec,
		})
	}

	toTable(options?: Materialized<K, V>): KTable<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('toTable() requires both key and value codecs for stateful operations')
		}

		const self = this.copartitioned(undefined, keyCodec, valueCodec)
		const storeName = options?.storeName ?? `toTable-store-${this.builder.topology.nextStoreId()}`
		const store = this.builder.stores.getOrCreateKeyValueStore<K, V>(storeName, {
			keyCodec,
			valueCodec,
			changelog: options?.changelog,
			sourceTopics: self.sourceTopics,
		})
		const storeRef = { store }

		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		self.node.connect(tableStateNode)

		return new KTableImpl<K, V>(this.builder, tableStateNode, self.format, storeRef, self.sourceTopics)
	}

	groupBy<K2>(fn: (key: K | null, value: V | null) => K2, options?: Grouped<K2, V>): KGroupedStream<K2, V> {
		const keyCodec = options?.key
		const valueCodec = options?.value ?? this.format.valueCodec
		if (!keyCodec) {
			throw new Error('groupBy() requires a key codec for the new key. Provide one via groupBy(fn, { key }).')
		}

		// Re-key, then repartition so all records of a new key meet in one state partition.
		const stream = this.chain(
			new SelectKeyNode<K, V, K2>((value, key) => fn(key, value)),
			{ keyCodec, valueCodec },
			true
		).repartitioned(options?.name, keyCodec, valueCodec)
		return new KGroupedStreamImpl(this.builder, stream.node, stream.format, stream.sourceTopics)
	}

	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		const stream = this.copartitioned(options?.name, keyCodec, valueCodec)
		return new KGroupedStreamImpl(this.builder, stream.node, { keyCodec, valueCodec }, stream.sourceTopics)
	}

	join<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		if (other instanceof KTableImpl) {
			return this.tableJoin(other, options, storeRef => new StreamTableJoinNode<K, V, V2, VR>(storeRef, joiner))
		}
		return this.windowedJoin(other, options, 'join', 'stream-join', (left, right, windowMs) => [
			new StreamStreamJoinNode<K, V, V2, VR>(left, right, joiner, windowMs),
			new StreamStreamJoinNode<K, V2, V, VR>(
				right,
				left,
				(value, otherValue) => joiner(otherValue, value),
				windowMs
			),
		])
	}

	leftJoin<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		if (other instanceof KTableImpl) {
			return this.tableJoin(
				other,
				options,
				storeRef => new StreamTableLeftJoinNode<K, V, V2, VR>(storeRef, joiner)
			)
		}
		// The left side emits unmatched records with null; the right side only emits matches.
		return this.windowedJoin(other, options, 'left join', 'stream-left-join', (left, right, windowMs) => [
			new StreamStreamLeftJoinNode<K, V, V2, VR>(left, right, joiner, windowMs),
			new StreamStreamJoinNode<K, V2, V, VR>(
				right,
				left,
				(value, otherValue) => joiner(otherValue, value),
				windowMs
			),
		])
	}

	outerJoin<V2, VR>(
		other: KStream<K, V2>,
		joiner: (value: V | null, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		return this.windowedJoin(other, options, 'outer join', 'stream-outer-join', (left, right, windowMs) => [
			new StreamStreamOuterJoinNode<K, V, V2, VR>(left, right, joiner, windowMs),
			new StreamStreamOuterJoinNode<K, V2, V, VR>(
				right,
				left,
				(value, otherValue) => joiner(otherValue, value),
				windowMs
			),
		])
	}

	private tableJoin<V2, VR>(
		table: KTableImpl<K, V2>,
		options: Joined<K, V, V2> | undefined,
		createNode: (storeRef: StoreRef<KeyValueStore<K, V2>>) => Processor<K, V, K, VR>
	): KStream<K, VR> {
		// A global table holds every key on every instance, so its lookups need no repartitioning.
		const self = table.global ? this : this.copartitioned(undefined, options?.key, options?.value)
		if (!table.storeRef.store) {
			throw new Error('Cannot join with a KTable that has no state store')
		}
		if (!table.global) {
			// The lookup hits the table's state partition of the stream record's partition.
			this.builder.topology.requireCopartitioned(new Set([...self.sourceTopics, ...table.sourceTopics]))
		}
		// Only the stream's source topics affect the partitioning of the result.
		return self.chain(createNode(table.storeRef), { keyCodec: self.format.keyCodec }, false)
	}

	/** Stream-stream join: each side buffers its records in a window store and looks up the other's. */
	private windowedJoin<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		options: Joined<K, V, V2> | undefined,
		kind: string,
		storePrefix: string,
		createNodes: WindowJoinNodes<K, V, V2, VR>
	): KStream<K, VR> {
		const self = this.copartitioned(undefined, options?.key, options?.value)
		if (!(other instanceof KStreamImpl)) {
			throw new Error('Can only join with KStreamImpl or KTableImpl instances')
		}

		const otherStream = (other as KStreamImpl<K, V2>).copartitioned(undefined, options?.key, options?.otherValue)
		if (!options?.within) {
			throw new Error(`Stream-stream ${kind} requires a join window. Provide options.within.`)
		}
		const joinWindowMs = resolveJoinWindowMs(options.within)

		const keyCodec = self.format.keyCodec
		const valueCodec = self.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec
		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		const mergedTopics = new Set([...self.sourceTopics, ...otherStream.sourceTopics])
		const { stores, topology } = this.builder
		topology.requireCopartitioned(mergedTopics)

		// Back both side stores with a changelog so a restarted/rebalanced task rebuilds its
		// in-flight join window instead of silently dropping matches. Each side's store is
		// co-partitioned with its own stream's source topics, so restoration is restricted to the
		// assigned source partitions.
		const windowOptions = {
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
			changelog: options.changelog,
		}
		const leftStore = stores.getOrCreateWindowStore<K, V>(`${storePrefix}-left-${topology.nextStoreId()}`, {
			...windowOptions,
			keyCodec,
			valueCodec,
			sourceTopics: self.sourceTopics,
		})
		const rightStore = stores.getOrCreateWindowStore<K, V2>(`${storePrefix}-right-${topology.nextStoreId()}`, {
			...windowOptions,
			keyCodec,
			valueCodec: otherValueCodec,
			sourceTopics: otherStream.sourceTopics,
		})

		const [leftNode, rightNode] = createNodes({ store: leftStore }, { store: rightStore }, joinWindowMs)
		const mergeNode = new PassThroughNode<K, VR>()
		self.node.connect(leftNode)
		leftNode.connect(mergeNode)
		otherStream.node.connect(rightNode)
		rightNode.connect(mergeNode)

		return new KStreamImpl<K, VR>(this.builder, mergeNode, { keyCodec: self.format.keyCodec }, mergedTopics)
	}
}

export function ensureStream<K, V>(stream: KStream<K, V>, builder: TopologyBuilder): KStreamImpl<K, V> {
	if (stream instanceof KStreamImpl) {
		if (stream.builder !== builder) {
			throw new Error('Cannot merge streams from different Flow apps')
		}
		return stream as KStreamImpl<K, V>
	}
	throw new Error('Unsupported stream implementation')
}
