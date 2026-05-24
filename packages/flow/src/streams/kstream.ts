import type { KStream, KTable, KGroupedStream, Produced, Grouped, Materialized, Joined, KeyValue } from '@/types.js'
import {
	type OutputProcessor,
	PassThroughNode,
	MapNode,
	MapValuesNode,
	FlatMapValuesNode,
	FilterNode,
	PeekNode,
	SelectKeyNode,
	BranchNode,
	ProduceNode,
	type StreamFormat,
} from '@/processors/index.js'
import { TableStateNode } from '@/processors/table.js'
import { StreamTableJoinNode, StreamTableLeftJoinNode } from '@/processors/joins/stream-table.js'
import {
	StreamStreamJoinNode,
	StreamStreamLeftJoinNode,
	StreamStreamOuterJoinNode,
} from '@/processors/joins/stream-stream.js'
import { joinWindowMs as resolveJoinWindowMs } from '@/helpers.js'
import { KTableImpl, type FlowAppInterface } from '@/streams/ktable.js'
import { KGroupedStreamImpl } from '@/streams/grouped.js'

export class KStreamImpl<K, V> implements KStream<K, V> {
	constructor(
		readonly app: FlowAppInterface,
		readonly node: OutputProcessor<K, V>,
		readonly format: StreamFormat<K, V>,
		/** Source topics that feed into this stream. Used for changelog partition inference. */
		readonly sourceTopics: Set<string> = new Set()
	) {}

	map<K2, V2>(fn: (key: K | null, value: V | null) => KeyValue<K2, V2>): KStream<K2, V2> {
		const node = new MapNode<K, V, K2, V2>(fn)
		this.node.connect(node)
		return new KStreamImpl<K2, V2>(this.app, node, {}, this.sourceTopics)
	}

	mapValues<V2>(fn: (value: V | null) => V2 | null): KStream<K, V2> {
		const node = new MapValuesNode<K, V, V2>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V2>(this.app, node, { keyCodec: this.format.keyCodec }, this.sourceTopics)
	}

	flatMapValues<V2>(fn: (value: V | null) => Iterable<V2>): KStream<K, V2> {
		const node = new FlatMapValuesNode<K, V, V2>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V2>(this.app, node, { keyCodec: this.format.keyCodec }, this.sourceTopics)
	}

	filter(fn: (key: K | null, value: V | null) => boolean): KStream<K, V> {
		const node = new FilterNode<K, V>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V>(this.app, node, this.format, this.sourceTopics)
	}

	peek(fn: (key: K | null, value: V | null) => void): KStream<K, V> {
		const node = new PeekNode<K, V>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V>(this.app, node, this.format, this.sourceTopics)
	}

	selectKey<K2>(fn: (value: V | null, key: K | null) => K2): KStream<K2, V> {
		const node = new SelectKeyNode<K, V, K2>(fn)
		this.node.connect(node)
		return new KStreamImpl<K2, V>(this.app, node, { valueCodec: this.format.valueCodec }, this.sourceTopics)
	}

	merge(other: KStream<K, V>): KStream<K, V> {
		const otherStream = ensureStream(other, this.app)
		const node = new PassThroughNode<K, V>()
		this.node.connect(node)
		otherStream.node.connect(node)

		const keyCodec = this.format.keyCodec === otherStream.format.keyCodec ? this.format.keyCodec : undefined
		const valueCodec = this.format.valueCodec === otherStream.format.valueCodec ? this.format.valueCodec : undefined

		// Merge source topics from both streams
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])

		return new KStreamImpl<K, V>(this.app, node, { keyCodec, valueCodec }, mergedTopics)
	}

	branch(...predicates: Array<(key: K | null, value: V | null) => boolean>): KStream<K, V>[] {
		const node = new BranchNode<K, V>()
		this.node.connect(node)

		return predicates.map(predicate => {
			const branchNode = new PassThroughNode<K, V>()
			node.addBranch(predicate, branchNode)
			return new KStreamImpl<K, V>(this.app, branchNode, this.format, this.sourceTopics)
		})
	}

	to(topic: string, options?: Produced<K, V>): void {
		// FlowAppInterface satisfies ProduceNode's FlowAppInterface (both have sendToTopic)
		const node = new ProduceNode<K, V>(this.app, topic, options, this.format, false)
		this.node.connect(node)
	}

	through(topic: string, options?: Produced<K, V>): KStream<K, V> {
		// FlowAppInterface satisfies ProduceNode's FlowAppInterface (both have sendToTopic)
		const node = new ProduceNode<K, V>(this.app, topic, options, this.format, true)
		this.node.connect(node)
		// After going through a repartition topic, the new source is the through topic
		return new KStreamImpl<K, V>(this.app, node, this.format, new Set([topic]))
	}

	toTable(options?: Materialized<K, V>): KTable<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('toTable() requires both key and value codecs for stateful operations')
		}

		const storeName = options?.storeName ?? `toTable-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, V>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		// Create table state node that maintains state
		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		this.node.connect(tableStateNode)

		return new KTableImpl<K, V>(this.app, tableStateNode, this.format, storeRef)
	}

	groupBy<K2>(fn: (key: K | null, value: V | null) => K2, options?: Grouped<K2, V>): KGroupedStream<K2, V> {
		// Add a selectKey node to re-key the stream
		const selectNode = new SelectKeyNode<K, V, K2>((value, key) => fn(key, value))
		this.node.connect(selectNode)

		const keyCodec = options?.key
		const valueCodec = options?.value ?? this.format.valueCodec

		// Re-keyed: don't restrict changelog restoration to source partitions
		return new KGroupedStreamImpl(this.app, selectNode, { keyCodec, valueCodec }, this.sourceTopics, false)
	}

	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		// Same key: restrict changelog restoration to source partitions
		return new KGroupedStreamImpl(this.app, this.node, { keyCodec, valueCodec }, this.sourceTopics, true)
	}

	join<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		// Check if joining with a KTable (Stream-Table join)
		if (other instanceof KTableImpl) {
			const tableImpl = other as KTableImpl<K, V2>
			if (!tableImpl.storeRef.store) {
				throw new Error('Cannot join with a KTable that has no state store')
			}

			const joinNode = new StreamTableJoinNode<K, V, V2, VR>(tableImpl.storeRef, joiner)
			this.node.connect(joinNode)

			// Stream-table join: only stream's source topics affect partitioning
			return new KStreamImpl<K, VR>(this.app, joinNode, { keyCodec: this.format.keyCodec }, this.sourceTopics)
		}

		// Stream-Stream join requires windowing
		if (!(other instanceof KStreamImpl)) {
			throw new Error('Can only join with KStreamImpl or KTableImpl instances')
		}

		const otherStream = other as KStreamImpl<K, V2>
		const joinWindow = options?.within
		if (!joinWindow) {
			throw new Error('Stream-stream join requires a join window. Provide options.within.')
		}

		const joinWindowMs = resolveJoinWindowMs(joinWindow)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Merge source topics from both streams for stream-stream join
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])

		// Create window stores for both streams
		const leftStoreName = `stream-join-left-${this.app.nextStoreId()}`
		const rightStoreName = `stream-join-right-${this.app.nextStoreId()}`

		// Back both side stores with a changelog so a restarted/rebalanced task rebuilds its
		// in-flight join window instead of silently dropping matches. A stream-stream join
		// requires its inputs to be co-partitioned (same key, same partition count) — the caller's
		// responsibility, as in Kafka Streams, since this library does not auto-insert a
		// repartition topic. Given that, each side's store is co-partitioned with its own stream's
		// source topics, so restoration is restricted to the assigned source partitions.
		const windowOptions = { retentionMs: joinWindowMs * 2, windowSizeMs: joinWindowMs }
		const leftStore = this.app.getOrCreateWindowStore<K, V>(
			leftStoreName,
			keyCodec,
			valueCodec,
			windowOptions,
			options?.changelog,
			this.sourceTopics
		)
		const rightStore = this.app.getOrCreateWindowStore<K, V2>(
			rightStoreName,
			keyCodec,
			otherValueCodec,
			windowOptions,
			options?.changelog,
			otherStream.sourceTopics
		)

		const leftStoreRef = { store: leftStore }
		const rightStoreRef = { store: rightStore }

		// Create merge node for results
		const mergeNode = new PassThroughNode<K, VR>()

		// Left stream join processor
		const leftJoinNode = new StreamStreamJoinNode<K, V, V2, VR>(leftStoreRef, rightStoreRef, joiner, joinWindowMs)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// Right stream join processor - swap args so joiner receives (left, right) order
		const rightJoiner = (value: V2, otherValue: V) => joiner(otherValue, value)
		const rightJoinNode = new StreamStreamJoinNode<K, V2, V, VR>(
			rightStoreRef,
			leftStoreRef,
			rightJoiner,
			joinWindowMs
		)
		otherStream.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		return new KStreamImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec }, mergedTopics)
	}

	leftJoin<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		// Check if joining with a KTable (Stream-Table left join)
		if (other instanceof KTableImpl) {
			const tableImpl = other as KTableImpl<K, V2>
			if (!tableImpl.storeRef.store) {
				throw new Error('Cannot join with a KTable that has no state store')
			}

			const joinNode = new StreamTableLeftJoinNode<K, V, V2, VR>(tableImpl.storeRef, joiner)
			this.node.connect(joinNode)

			// Stream-table join: only stream's source topics affect partitioning
			return new KStreamImpl<K, VR>(this.app, joinNode, { keyCodec: this.format.keyCodec }, this.sourceTopics)
		}

		// Stream-Stream left join
		if (!(other instanceof KStreamImpl)) {
			throw new Error('Can only join with KStreamImpl or KTableImpl instances')
		}

		const otherStream = other as KStreamImpl<K, V2>
		const joinWindow = options?.within
		if (!joinWindow) {
			throw new Error('Stream-stream left join requires a join window. Provide options.within.')
		}

		const joinWindowMs = resolveJoinWindowMs(joinWindow)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Merge source topics from both streams
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])

		// Create window stores for both streams
		const leftStoreName = `stream-left-join-left-${this.app.nextStoreId()}`
		const rightStoreName = `stream-left-join-right-${this.app.nextStoreId()}`

		// Back both side stores with a changelog so a restarted/rebalanced task rebuilds its
		// in-flight join window instead of silently dropping matches. A stream-stream join
		// requires its inputs to be co-partitioned (same key, same partition count) — the caller's
		// responsibility, as in Kafka Streams, since this library does not auto-insert a
		// repartition topic. Given that, each side's store is co-partitioned with its own stream's
		// source topics, so restoration is restricted to the assigned source partitions.
		const windowOptions = { retentionMs: joinWindowMs * 2, windowSizeMs: joinWindowMs }
		const leftStore = this.app.getOrCreateWindowStore<K, V>(
			leftStoreName,
			keyCodec,
			valueCodec,
			windowOptions,
			options?.changelog,
			this.sourceTopics
		)
		const rightStore = this.app.getOrCreateWindowStore<K, V2>(
			rightStoreName,
			keyCodec,
			otherValueCodec,
			windowOptions,
			options?.changelog,
			otherStream.sourceTopics
		)

		const leftStoreRef = { store: leftStore }
		const rightStoreRef = { store: rightStore }

		// Create merge node for results
		const mergeNode = new PassThroughNode<K, VR>()

		// Left stream left-join processor (emits with null if no match)
		const leftJoinNode = new StreamStreamLeftJoinNode<K, V, V2, VR>(
			leftStoreRef,
			rightStoreRef,
			joiner,
			joinWindowMs
		)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// Right stream inner-join processor (only emits if left exists)
		const rightJoiner = (value: V2, otherValue: V) => joiner(otherValue, value)
		const rightJoinNode = new StreamStreamJoinNode<K, V2, V, VR>(
			rightStoreRef,
			leftStoreRef,
			rightJoiner,
			joinWindowMs
		)
		otherStream.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		return new KStreamImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec }, mergedTopics)
	}

	outerJoin<V2, VR>(
		other: KStream<K, V2>,
		joiner: (value: V | null, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR> {
		if (!(other instanceof KStreamImpl)) {
			throw new Error('Can only outer join with KStreamImpl instances')
		}

		const otherStream = other as KStreamImpl<K, V2>
		const joinWindow = options?.within
		if (!joinWindow) {
			throw new Error('Stream-stream outer join requires a join window. Provide options.within.')
		}

		const joinWindowMs = resolveJoinWindowMs(joinWindow)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Create window stores for both streams
		const leftStoreName = `stream-outer-join-left-${this.app.nextStoreId()}`
		const rightStoreName = `stream-outer-join-right-${this.app.nextStoreId()}`

		// Back both side stores with a changelog so a restarted/rebalanced task rebuilds its
		// in-flight join window instead of silently dropping matches. A stream-stream join
		// requires its inputs to be co-partitioned (same key, same partition count) — the caller's
		// responsibility, as in Kafka Streams, since this library does not auto-insert a
		// repartition topic. Given that, each side's store is co-partitioned with its own stream's
		// source topics, so restoration is restricted to the assigned source partitions.
		const windowOptions = { retentionMs: joinWindowMs * 2, windowSizeMs: joinWindowMs }
		const leftStore = this.app.getOrCreateWindowStore<K, V>(
			leftStoreName,
			keyCodec,
			valueCodec,
			windowOptions,
			options?.changelog,
			this.sourceTopics
		)
		const rightStore = this.app.getOrCreateWindowStore<K, V2>(
			rightStoreName,
			keyCodec,
			otherValueCodec,
			windowOptions,
			options?.changelog,
			otherStream.sourceTopics
		)

		const leftStoreRef = { store: leftStore }
		const rightStoreRef = { store: rightStore }

		// Create merge node for results
		const mergeNode = new PassThroughNode<K, VR>()

		// Left stream outer-join processor
		const leftJoinNode = new StreamStreamOuterJoinNode<K, V, V2, VR>(
			leftStoreRef,
			rightStoreRef,
			joiner,
			joinWindowMs
		)
		this.node.connect(leftJoinNode)
		leftJoinNode.connect(mergeNode)

		// Right stream outer-join processor - swap args so joiner receives (left, right) order
		const rightJoiner = (value: V2 | null, otherValue: V | null) => joiner(otherValue, value)
		const rightJoinNode = new StreamStreamOuterJoinNode<K, V2, V, VR>(
			rightStoreRef,
			leftStoreRef,
			rightJoiner,
			joinWindowMs
		)
		otherStream.node.connect(rightJoinNode)
		rightJoinNode.connect(mergeNode)

		// Merge source topics from both streams for partition inference
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])
		return new KStreamImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec }, mergedTopics)
	}
}

export function ensureStream<K, V>(stream: KStream<K, V>, app: FlowAppInterface): KStreamImpl<K, V> {
	if (stream instanceof KStreamImpl) {
		if (stream.app !== app) {
			throw new Error('Cannot merge streams from different Flow apps')
		}
		return stream as KStreamImpl<K, V>
	}
	throw new Error('Unsupported stream implementation')
}
