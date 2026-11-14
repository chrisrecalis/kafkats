import type { KTable, KGroupedStream, KGroupedTable, WindowedKGroupedStream, Materialized, Windowed } from '@/types.js'
import type { Codec } from '@/codec.js'
import type { KeyValueStore, SessionStore, WindowStore } from '@/state.js'
import type { Processor, OutputProcessor, StoreRef, StreamFormat } from '@/processors/base.js'
import {
	AggregateNode,
	ReduceNode,
	WindowedAggregateNode,
	WindowedReduceNode,
	SessionAggregateNode,
	SessionReduceNode,
	WINDOW_STORE_RETENTION_MULTIPLIER,
} from '@/processors/aggregation.js'
import { TableDeltaCountNode, TableDeltaReduceNode, TableDeltaAggregateNode } from '@/processors/table.js'
import { TimeWindows, SessionWindows, SlidingWindows } from '@/windows.js'
import { parseWindowDuration, SLIDING_WINDOWS_NOT_IMPLEMENTED } from '@/helpers.js'
import { KTableImpl } from '@/streams/ktable.js'
import type { TopologyBuilder } from '@/streams/topology-builder.js'

const numberCodec: Codec<number> = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

/** Encodes a windowed key as the key's bytes followed by the window start and end (int64 BE each). */
function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<Windowed<K>> {
	return {
		encode: (wk: Windowed<K>) => {
			const keyBytes = keyCodec.encode(wk.key)
			const buf = Buffer.alloc(keyBytes.length + 16)
			keyBytes.copy(buf, 0)
			buf.writeBigInt64BE(BigInt(wk.window.start), keyBytes.length)
			buf.writeBigInt64BE(BigInt(wk.window.end), keyBytes.length + 8)
			return buf
		},
		decode: (b: Buffer) => ({
			key: keyCodec.decode(b.subarray(0, b.length - 16)),
			window: {
				start: Number(b.readBigInt64BE(b.length - 16)),
				end: Number(b.readBigInt64BE(b.length - 8)),
			},
		}),
	}
}

function required<T>(value: T | undefined, message: string): T {
	if (!value) {
		throw new Error(message)
	}
	return value
}

/**
 * Create a key-value store for an aggregation, connect the node that maintains it after `upstream`
 * and return the resulting table.
 */
function materializeKeyValue<K, V, A>(
	builder: TopologyBuilder,
	upstream: OutputProcessor<K, V>,
	sourceTopics: Set<string>,
	defaultStorePrefix: string,
	keyCodec: Codec<K>,
	valueCodec: Codec<A>,
	options: Materialized<K, A> | undefined,
	createNode: (storeName: string, storeRef: StoreRef<KeyValueStore<K, A>>) => Processor<K, V, K, A>
): KTable<K, A> {
	const storeName = options?.storeName ?? `${defaultStorePrefix}-${builder.topology.nextStoreId()}`
	const store = builder.stores.getOrCreateKeyValueStore<K, A>(storeName, {
		keyCodec,
		valueCodec,
		changelog: options?.changelog,
		sourceTopics,
	})
	const storeRef = { store }
	const node = createNode(storeName, storeRef)
	upstream.connect(node)
	return new KTableImpl<K, A>(builder, node, { keyCodec, valueCodec }, storeRef, sourceTopics)
}

export class KGroupedStreamImpl<K, V> implements KGroupedStream<K, V> {
	constructor(
		private readonly builder: TopologyBuilder,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		/** Source topics that feed into this grouped stream. Used for changelog partition inference. */
		private readonly sourceTopics: Set<string> = new Set()
	) {}

	private materialize<A>(
		defaultStorePrefix: string,
		keyCodec: Codec<K>,
		valueCodec: Codec<A>,
		options: Materialized<K, A> | undefined,
		createNode: (storeName: string, storeRef: StoreRef<KeyValueStore<K, A>>) => Processor<K, V, K, A>
	): KTable<K, A> {
		return materializeKeyValue(
			this.builder,
			this.node,
			this.sourceTopics,
			defaultStorePrefix,
			keyCodec,
			valueCodec,
			options,
			createNode
		)
	}

	count(options?: Materialized<K, number>): KTable<K, number> {
		const keyCodec = required(
			options?.key ?? this.format.keyCodec,
			'count() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.'
		)
		return this.materialize(
			'count-store',
			keyCodec,
			options?.value ?? numberCodec,
			options,
			(storeName, storeRef) =>
				new AggregateNode<K, V, number>(
					storeName,
					storeRef,
					() => 0,
					(_k, _v, n) => n + 1
				)
		)
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<K, V>): KTable<K, V> {
		const keyCodec = required(
			options?.key ?? this.format.keyCodec,
			'reduce() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.'
		)
		const valueCodec = required(
			options?.value ?? this.format.valueCodec,
			'reduce() requires a value codec. Provide one via groupBy/groupByKey options or Materialized.'
		)
		return this.materialize(
			'reduce-store',
			keyCodec,
			valueCodec,
			options,
			(storeName, storeRef) => new ReduceNode<K, V>(storeName, storeRef, reducer)
		)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A> {
		const keyCodec = required(
			options?.key ?? this.format.keyCodec,
			'aggregate() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.'
		)
		const valueCodec = required(options?.value, 'aggregate() requires a value codec in Materialized options.')
		return this.materialize(
			'aggregate-store',
			keyCodec,
			valueCodec,
			options,
			(storeName, storeRef) => new AggregateNode<K, V, A>(storeName, storeRef, initializer, aggregator)
		)
	}

	windowedBy(windows: TimeWindows | SessionWindows | SlidingWindows): WindowedKGroupedStream<K, V> {
		return new WindowedKGroupedStreamImpl<K, V>(this.builder, this.node, this.format, windows, this.sourceTopics)
	}
}

type WindowedNodes<K, V, A> = {
	window: (storeName: string, storeRef: StoreRef<WindowStore<K, A>>) => Processor<K, V, Windowed<K>, A>
	session: (storeName: string, storeRef: StoreRef<SessionStore<K, A>>) => Processor<K, V, Windowed<K>, A>
}

export class WindowedKGroupedStreamImpl<K, V> implements WindowedKGroupedStream<K, V> {
	private readonly windowSizeMs: number
	private readonly advanceMs: number

	constructor(
		private readonly builder: TopologyBuilder,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		private readonly windows: TimeWindows | SessionWindows | SlidingWindows,
		/** Source topics that feed into this windowed grouped stream. Used for changelog partition inference. */
		private readonly sourceTopics: Set<string> = new Set()
	) {
		if (windows instanceof TimeWindows) {
			this.windowSizeMs = parseWindowDuration(windows.size)
			// Honour TimeWindows.advanceBy() — when advance < size, hopping
			// windows assign records to multiple overlapping windows.
			this.advanceMs = windows.advance ? parseWindowDuration(windows.advance) : this.windowSizeMs
		} else if (windows instanceof SlidingWindows) {
			// No dedicated processor yet; treating as tumbling silently produces wrong aggregates.
			throw new Error(SLIDING_WINDOWS_NOT_IMPLEMENTED)
		} else {
			// For session windows the "window size" is the inactivity gap.
			this.windowSizeMs = parseWindowDuration(windows.gap)
			this.advanceMs = this.windowSizeMs
		}
	}

	/**
	 * Create the window or session store for an aggregation, connect the node that maintains it and
	 * return the resulting table. Stores keep windows/sessions for a multiple of the window size.
	 */
	private materialize<A>(
		defaultStorePrefix: string,
		keyCodec: Codec<K>,
		valueCodec: Codec<A>,
		options: Materialized<Windowed<K>, A> | undefined,
		nodes: WindowedNodes<K, V, A>
	): KTable<Windowed<K>, A> {
		const { stores, topology } = this.builder
		const storeName = options?.storeName ?? `${defaultStorePrefix}-${topology.nextStoreId()}`
		const storeOptions = {
			keyCodec,
			valueCodec,
			changelog: options?.changelog,
			sourceTopics: this.sourceTopics,
			retentionMs: this.windowSizeMs * WINDOW_STORE_RETENTION_MULTIPLIER,
		}

		let node: Processor<K, V, Windowed<K>, A>
		if (this.windows instanceof SessionWindows) {
			const store = stores.getOrCreateSessionStore<K, A>(storeName, storeOptions)
			node = nodes.session(storeName, { store })
		} else {
			const store = stores.getOrCreateWindowStore<K, A>(storeName, {
				...storeOptions,
				windowSizeMs: this.windowSizeMs,
			})
			node = nodes.window(storeName, { store })
		}
		this.node.connect(node)

		return new KTableImpl<Windowed<K>, A>(this.builder, node, { keyCodec: windowedKeyCodec(keyCodec), valueCodec })
	}

	count(options?: Materialized<Windowed<K>, number>): KTable<Windowed<K>, number> {
		const keyCodec = required(this.format.keyCodec, 'windowed count() requires a key codec.')
		const initializer = () => 0
		const increment = (_key: K, _value: V, count: number) => count + 1
		return this.materialize('window-count-store', keyCodec, options?.value ?? numberCodec, options, {
			window: (storeName, storeRef) =>
				new WindowedAggregateNode<K, V, number>(
					storeName,
					storeRef,
					initializer,
					increment,
					this.windowSizeMs,
					this.advanceMs
				),
			session: (storeName, storeRef) =>
				new SessionAggregateNode<K, V, number>(
					storeName,
					storeRef,
					initializer,
					increment,
					(a, b) => a + b,
					this.windowSizeMs
				),
		})
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<Windowed<K>, V>): KTable<Windowed<K>, V> {
		const keyCodec = required(this.format.keyCodec, 'windowed reduce() requires a key codec.')
		const valueCodec = required(
			options?.value ?? this.format.valueCodec,
			'windowed reduce() requires a value codec.'
		)
		return this.materialize('window-reduce-store', keyCodec, valueCodec, options, {
			window: (storeName, storeRef) =>
				new WindowedReduceNode<K, V>(storeName, storeRef, reducer, this.windowSizeMs, this.advanceMs),
			session: (storeName, storeRef) =>
				new SessionReduceNode<K, V>(storeName, storeRef, reducer, this.windowSizeMs),
		})
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<Windowed<K>, A> & { merger?: (aggregate1: A, aggregate2: A) => A }
	): KTable<Windowed<K>, A> {
		const keyCodec = required(this.format.keyCodec, 'windowed aggregate() requires a key codec.')
		const valueCodec = required(
			options?.value,
			'windowed aggregate() requires a value codec in Materialized options.'
		)
		// Sessions merge when a record bridges them, which needs to combine two aggregates.
		const merger = options?.merger
		if (this.windows instanceof SessionWindows && !merger) {
			throw new Error(
				'aggregate() on session windows requires a merger to combine the aggregates of sessions ' +
					'that merge. Provide one via options: aggregate(init, agg, { value, merger: (a, b) => ... }).'
			)
		}
		return this.materialize('window-aggregate-store', keyCodec, valueCodec, options, {
			window: (storeName, storeRef) =>
				new WindowedAggregateNode<K, V, A>(
					storeName,
					storeRef,
					initializer,
					aggregator,
					this.windowSizeMs,
					this.advanceMs
				),
			session: (storeName, storeRef) =>
				new SessionAggregateNode<K, V, A>(
					storeName,
					storeRef,
					initializer,
					aggregator,
					merger!,
					this.windowSizeMs
				),
		})
	}
}

export class KGroupedTableImpl<K, V> implements KGroupedTable<K, V> {
	constructor(
		private readonly builder: TopologyBuilder,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		/** The repartition topic carrying the grouped rows; the aggregate state is co-partitioned with it. */
		private readonly sourceTopics: Set<string>
	) {}

	private materialize<A>(
		defaultStorePrefix: string,
		keyCodec: Codec<K>,
		valueCodec: Codec<A>,
		options: Materialized<K, A> | undefined,
		createNode: (storeName: string, storeRef: StoreRef<KeyValueStore<K, A>>) => Processor<K, V, K, A>
	): KTable<K, A> {
		return materializeKeyValue(
			this.builder,
			this.node,
			this.sourceTopics,
			defaultStorePrefix,
			keyCodec,
			valueCodec,
			options,
			createNode
		)
	}

	count(options?: Materialized<K, number>): KTable<K, number> {
		const keyCodec = required(options?.key ?? this.format.keyCodec, 'KGroupedTable.count() requires a key codec.')
		return this.materialize(
			'table-count-store',
			keyCodec,
			options?.value ?? numberCodec,
			options,
			(storeName, storeRef) => new TableDeltaCountNode<K, V>(storeName, storeRef)
		)
	}

	reduce(
		adder: (aggregate: V, value: V) => V,
		subtractor: (aggregate: V, value: V) => V,
		options?: Materialized<K, V>
	): KTable<K, V> {
		const keyCodec = required(options?.key ?? this.format.keyCodec, 'KGroupedTable.reduce() requires a key codec.')
		const valueCodec = required(
			options?.value ?? this.format.valueCodec,
			'KGroupedTable.reduce() requires a value codec.'
		)
		return this.materialize(
			'table-reduce-store',
			keyCodec,
			valueCodec,
			options,
			(storeName, storeRef) => new TableDeltaReduceNode<K, V>(storeName, storeRef, adder, subtractor)
		)
	}

	aggregate<A>(
		initializer: () => A,
		adder: (key: K, value: V, aggregate: A) => A,
		subtractor: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A> {
		const keyCodec = required(
			options?.key ?? this.format.keyCodec,
			'KGroupedTable.aggregate() requires a key codec.'
		)
		const valueCodec = required(
			options?.value,
			'KGroupedTable.aggregate() requires a value codec in Materialized options.'
		)
		return this.materialize('table-aggregate-store', keyCodec, valueCodec, options, (storeName, storeRef) => {
			// Row count per group, so a group whose last row leaves is removed instead of lingering
			// as whatever the subtractor returns for "nothing".
			const members = this.builder.stores.getOrCreateKeyValueStore<K, number>(`${storeName}-members`, {
				keyCodec,
				valueCodec: numberCodec,
				changelog: options?.changelog === false ? false : undefined,
				sourceTopics: this.sourceTopics,
			})
			return new TableDeltaAggregateNode<K, V, A>(storeName, storeRef, initializer, adder, subtractor, {
				store: members,
			})
		})
	}
}
