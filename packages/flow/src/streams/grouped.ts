import type { KTable, KGroupedStream, KGroupedTable, WindowedKGroupedStream, Materialized, Windowed } from '@/types.js'
import type { KeyValueStore } from '@/state.js'
import type { Codec } from '@/codec.js'
import type { Processor, OutputProcessor, StreamFormat } from '@/processors/base.js'
import {
	AggregateNode,
	ReduceNode,
	WindowedAggregateNode,
	WindowedReduceNode,
	SessionAggregateNode,
	SessionReduceNode,
} from '@/processors/aggregation.js'
import { TimeWindows, SessionWindows, SlidingWindows } from '@/windows.js'
import { parseWindowDuration } from '@/helpers.js'
import { KTableImpl, type FlowAppInterface } from '@/streams/ktable.js'

export class KGroupedStreamImpl<K, V> implements KGroupedStream<K, V> {
	constructor(
		private readonly app: FlowAppInterface,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		/** Source topics that feed into this grouped stream. Used for changelog partition inference. */
		private readonly sourceTopics: Set<string> = new Set()
	) {}

	count(options?: Materialized<K, number>): KTable<K, number> {
		const keyCodec = options?.key ?? this.format.keyCodec
		if (!keyCodec) {
			throw new Error('count() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.')
		}

		// Create a codec for number values
		const valueCodec: Codec<number> = options?.value ?? {
			encode: (v: number) => {
				const buf = Buffer.alloc(8)
				buf.writeDoubleLE(v, 0)
				return buf
			},
			decode: (b: Buffer) => b.readDoubleLE(0),
		}

		const storeName = options?.storeName ?? `count-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, number>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, number>(
			storeName,
			storeRef,
			() => 0,
			(_key, _value, aggregate) => aggregate + 1
		)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, number>(this.app, aggregateNode, {
			keyCodec,
			valueCodec,
		})
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<K, V>): KTable<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		if (!keyCodec) {
			throw new Error(
				'reduce() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.'
			)
		}
		if (!valueCodec) {
			throw new Error(
				'reduce() requires a value codec. Provide one via groupBy/groupByKey options or Materialized.'
			)
		}

		const storeName = options?.storeName ?? `reduce-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, V>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		const aggregateNode = new ReduceNode<K, V>(storeName, storeRef, reducer)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, V>(this.app, aggregateNode, { keyCodec, valueCodec })
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value

		if (!keyCodec) {
			throw new Error(
				'aggregate() requires a key codec. Provide one via groupBy/groupByKey options or Materialized.'
			)
		}
		if (!valueCodec) {
			throw new Error('aggregate() requires a value codec in Materialized options.')
		}

		const storeName = options?.storeName ?? `aggregate-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, A>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, A>(storeName, storeRef, initializer, aggregator)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, A>(this.app, aggregateNode, { keyCodec, valueCodec })
	}

	windowedBy(windows: TimeWindows | SessionWindows | SlidingWindows): WindowedKGroupedStream<K, V> {
		return new WindowedKGroupedStreamImpl<K, V>(this.app, this.node, this.format, windows, this.sourceTopics)
	}
}

export class WindowedKGroupedStreamImpl<K, V> implements WindowedKGroupedStream<K, V> {
	private readonly windowSizeMs: number

	constructor(
		private readonly app: FlowAppInterface,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		private readonly windows: TimeWindows | SessionWindows | SlidingWindows,
		/** Source topics that feed into this windowed grouped stream. Used for changelog partition inference. */
		private readonly sourceTopics: Set<string> = new Set()
	) {
		if (windows instanceof TimeWindows) {
			this.windowSizeMs = parseWindowDuration(windows.size)
		} else if (windows instanceof SlidingWindows) {
			this.windowSizeMs = parseWindowDuration(windows.size)
		} else {
			// SessionWindows - use gap as approximate window size
			this.windowSizeMs = parseWindowDuration(windows.gap)
		}
	}

	count(options?: Materialized<Windowed<K>, number>): KTable<Windowed<K>, number> {
		const keyCodec = this.format.keyCodec
		if (!keyCodec) {
			throw new Error('windowed count() requires a key codec.')
		}

		const valueCodec: Codec<number> = options?.value ?? {
			encode: (v: number) => {
				const buf = Buffer.alloc(8)
				buf.writeDoubleLE(v, 0)
				return buf
			},
			decode: (b: Buffer) => b.readDoubleLE(0),
		}

		const storeName = options?.storeName ?? `window-count-store-${this.app.nextStoreId()}`

		let aggregateNode: Processor<K, V, Windowed<K>, number>

		if (this.windows instanceof SessionWindows) {
			// Use session store for session windows
			const sessionStore = this.app.stateStoreProvider.createSessionStore<K, number>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
			})
			this.app.stateStores.set(storeName, sessionStore as KeyValueStore<unknown, unknown>)
			const storeRef = { store: sessionStore }

			aggregateNode = new SessionAggregateNode<K, V, number>(
				storeName,
				storeRef,
				() => 0,
				(_key, _value, aggregate) => aggregate + 1,
				(a, b) => a + b, // merger for counts
				this.windowSizeMs
			)
		} else {
			// Use window store for time windows
			const store = this.app.stateStoreProvider.createWindowStore<K, number>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24, // Keep windows for 24x the window size
				windowSizeMs: this.windowSizeMs,
			})
			this.app.stateStores.set(storeName, store as KeyValueStore<unknown, unknown>)
			const storeRef = { store }

			aggregateNode = new WindowedAggregateNode<K, V, number>(
				storeName,
				storeRef,
				() => 0,
				(_key, _value, aggregate) => aggregate + 1,
				this.windowSizeMs
			)
		}

		this.node.connect(aggregateNode)

		// Create a windowed key codec
		const windowedKeyCodec: Codec<Windowed<K>> = {
			encode: (wk: Windowed<K>) => {
				const keyBytes = keyCodec.encode(wk.key)
				const buf = Buffer.alloc(keyBytes.length + 16)
				keyBytes.copy(buf, 0)
				buf.writeBigInt64BE(BigInt(wk.window.start), keyBytes.length)
				buf.writeBigInt64BE(BigInt(wk.window.end), keyBytes.length + 8)
				return buf
			},
			decode: (b: Buffer) => {
				const keyBytes = b.subarray(0, b.length - 16)
				return {
					key: keyCodec.decode(keyBytes),
					window: {
						start: Number(b.readBigInt64BE(b.length - 16)),
						end: Number(b.readBigInt64BE(b.length - 8)),
					},
				}
			},
		}

		return new KTableImpl<Windowed<K>, number>(this.app, aggregateNode, { keyCodec: windowedKeyCodec, valueCodec })
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<Windowed<K>, V>): KTable<Windowed<K>, V> {
		const keyCodec = this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		if (!keyCodec) {
			throw new Error('windowed reduce() requires a key codec.')
		}
		if (!valueCodec) {
			throw new Error('windowed reduce() requires a value codec.')
		}

		const storeName = options?.storeName ?? `window-reduce-store-${this.app.nextStoreId()}`

		let aggregateNode: Processor<K, V, Windowed<K>, V>

		if (this.windows instanceof SessionWindows) {
			// Use session store for session windows
			const sessionStore = this.app.stateStoreProvider.createSessionStore<K, V>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
			})
			this.app.stateStores.set(storeName, sessionStore as KeyValueStore<unknown, unknown>)
			const storeRef = { store: sessionStore }

			aggregateNode = new SessionReduceNode<K, V>(storeName, storeRef, reducer, this.windowSizeMs)
		} else {
			// Use window store for time windows
			const store = this.app.stateStoreProvider.createWindowStore<K, V>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
				windowSizeMs: this.windowSizeMs,
			})
			this.app.stateStores.set(storeName, store as KeyValueStore<unknown, unknown>)
			const storeRef = { store }

			aggregateNode = new WindowedReduceNode<K, V>(storeName, storeRef, reducer, this.windowSizeMs)
		}

		this.node.connect(aggregateNode)

		const windowedKeyCodec: Codec<Windowed<K>> = {
			encode: (wk: Windowed<K>) => {
				const keyBytes = keyCodec.encode(wk.key)
				const buf = Buffer.alloc(keyBytes.length + 16)
				keyBytes.copy(buf, 0)
				buf.writeBigInt64BE(BigInt(wk.window.start), keyBytes.length)
				buf.writeBigInt64BE(BigInt(wk.window.end), keyBytes.length + 8)
				return buf
			},
			decode: (b: Buffer) => {
				const keyBytes = b.subarray(0, b.length - 16)
				return {
					key: keyCodec.decode(keyBytes),
					window: {
						start: Number(b.readBigInt64BE(b.length - 16)),
						end: Number(b.readBigInt64BE(b.length - 8)),
					},
				}
			},
		}

		return new KTableImpl<Windowed<K>, V>(this.app, aggregateNode, {
			keyCodec: windowedKeyCodec,
			valueCodec,
		})
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<Windowed<K>, A>
	): KTable<Windowed<K>, A> {
		const keyCodec = this.format.keyCodec
		const valueCodec = options?.value

		if (!keyCodec) {
			throw new Error('windowed aggregate() requires a key codec.')
		}
		if (!valueCodec) {
			throw new Error('windowed aggregate() requires a value codec in Materialized options.')
		}

		const storeName = options?.storeName ?? `window-aggregate-store-${this.app.nextStoreId()}`

		const store = this.app.stateStoreProvider.createWindowStore<K, A>(storeName, {
			keyCodec,
			valueCodec,
			retentionMs: this.windowSizeMs * 24,
			windowSizeMs: this.windowSizeMs,
		})
		this.app.stateStores.set(storeName, store as KeyValueStore<unknown, unknown>)
		const storeRef = { store }

		const aggregateNode = new WindowedAggregateNode<K, V, A>(
			storeName,
			storeRef,
			initializer,
			aggregator,
			this.windowSizeMs
		)

		this.node.connect(aggregateNode)

		const windowedKeyCodec: Codec<Windowed<K>> = {
			encode: (wk: Windowed<K>) => {
				const keyBytes = keyCodec.encode(wk.key)
				const buf = Buffer.alloc(keyBytes.length + 16)
				keyBytes.copy(buf, 0)
				buf.writeBigInt64BE(BigInt(wk.window.start), keyBytes.length)
				buf.writeBigInt64BE(BigInt(wk.window.end), keyBytes.length + 8)
				return buf
			},
			decode: (b: Buffer) => {
				const keyBytes = b.subarray(0, b.length - 16)
				return {
					key: keyCodec.decode(keyBytes),
					window: {
						start: Number(b.readBigInt64BE(b.length - 16)),
						end: Number(b.readBigInt64BE(b.length - 8)),
					},
				}
			},
		}

		return new KTableImpl<Windowed<K>, A>(this.app, aggregateNode, {
			keyCodec: windowedKeyCodec,
			valueCodec,
		})
	}
}

export class KGroupedTableImpl<K, V> implements KGroupedTable<K, V> {
	constructor(
		private readonly app: FlowAppInterface,
		private readonly node: OutputProcessor<K, V>,
		private readonly format: StreamFormat<K, V>,
		private readonly sourceStoreRef: { store: KeyValueStore<unknown, unknown> | null }
	) {}

	count(options?: Materialized<K, number>): KTable<K, number> {
		const keyCodec = options?.key ?? this.format.keyCodec
		if (!keyCodec) {
			throw new Error('KGroupedTable.count() requires a key codec.')
		}

		const valueCodec: Codec<number> = options?.value ?? {
			encode: (v: number) => {
				const buf = Buffer.alloc(8)
				buf.writeDoubleLE(v, 0)
				return buf
			},
			decode: (b: Buffer) => b.readDoubleLE(0),
		}

		const storeName = options?.storeName ?? `table-count-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, number>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, number>(
			storeName,
			storeRef,
			() => 0,
			(_key, _value, aggregate) => aggregate + 1
		)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, number>(this.app, aggregateNode, { keyCodec, valueCodec }, storeRef)
	}

	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<K, V>): KTable<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		if (!keyCodec) {
			throw new Error('KGroupedTable.reduce() requires a key codec.')
		}
		if (!valueCodec) {
			throw new Error('KGroupedTable.reduce() requires a value codec.')
		}

		const storeName = options?.storeName ?? `table-reduce-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, V>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const aggregateNode = new ReduceNode<K, V>(storeName, storeRef, reducer)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, V>(this.app, aggregateNode, { keyCodec, valueCodec }, storeRef)
	}

	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value

		if (!keyCodec) {
			throw new Error('KGroupedTable.aggregate() requires a key codec.')
		}
		if (!valueCodec) {
			throw new Error('KGroupedTable.aggregate() requires a value codec in Materialized options.')
		}

		const storeName = options?.storeName ?? `table-aggregate-store-${this.app.nextStoreId()}`
		const store = this.app.getOrCreateStore<K, A>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, A>(storeName, storeRef, initializer, aggregator)

		this.node.connect(aggregateNode)

		return new KTableImpl<K, A>(this.app, aggregateNode, { keyCodec, valueCodec }, storeRef)
	}
}
