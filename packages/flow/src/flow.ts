import { AsyncLocalStorage } from 'node:async_hooks'
import {
	KafkaClient,
	type KafkaClientConfig,
	type Consumer,
	type ConsumerConfig,
	type Message,
	type ConsumeContext,
	type Producer,
	type ProducerConfig,
	type RunEachOptions,
} from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { buffer as bufferCodec } from '@/codec.js'
import type { StateStoreProvider, KeyValueStore, WindowStore, SessionStore, WindowedKey } from '@/state.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import {
	type ChangelogConfig,
	type ChangelogRestorationOptions,
	type ChangelogTopicSpec,
	ChangelogWriter,
	ChangelogRestorer,
	ChangelogPartitionMismatchError,
	SourceTopicNotFoundError,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
} from '@/changelog.js'
import { ChangelogBackedKeyValueStore } from '@/state/changelog.js'

export type StreamState = 'CREATED' | 'RUNNING' | 'REBALANCING' | 'ERROR' | 'STOPPED'

export type WindowDuration = number | `${number}ms` | `${number}s` | `${number}m` | `${number}h` | `${number}d`

export interface FlowConfig {
	applicationId: string
	client: KafkaClient | KafkaClientConfig
	numStreamThreads?: number
	processingGuarantee?: 'at_least_once' | 'exactly_once'
	/**
	 * Transaction commit interval in milliseconds for exactly_once mode.
	 * Multiple messages are batched into a single transaction and committed together.
	 * Lower values provide lower latency but more transaction overhead.
	 * Higher values provide better throughput but higher latency.
	 * Default: 100ms
	 */
	commitIntervalMs?: number
	stateDir?: string
	stateStoreProvider?: StateStoreProvider
	consumer?: Omit<ConsumerConfig, 'groupId'>
	producer?: ProducerConfig
	runEach?: RunEachOptions
	/**
	 * Default changelog topic configuration.
	 */
	changelog?: {
		/** Default replication factor for changelog topics */
		replicationFactor?: number
		/** Additional topic configs applied to all changelog topics */
		topicConfigs?: Record<string, string>
		/** Options to control changelog restoration behavior on startup. */
		restoration?: ChangelogRestorationOptions
		/**
		 * Whether to auto-create changelog topics during startup.
		 * Default: true
		 * Set to false to only validate existing topics (useful in production).
		 */
		autoCreate?: boolean
	}
}

export interface Consumed<K, V> {
	key?: Codec<K>
	value?: Codec<V>
	offsetReset?: 'earliest' | 'latest' | 'none'
}

export interface Produced<K, V> {
	key?: Codec<K>
	value?: Codec<V>
	partitioner?: (key: K | null, value: V, partitionCount: number) => number
}

export interface Grouped<K, V> {
	key?: Codec<K>
	value?: Codec<V>
}

export interface Materialized<K, V> {
	storeName?: string
	key?: Codec<K>
	value?: Codec<V>
	/** Changelog backing for fault-tolerant state. Default: true (enabled) */
	changelog?: boolean | ChangelogConfig
}

export interface Joined<K, V1, V2> {
	key?: Codec<K>
	value?: Codec<V1>
	otherValue?: Codec<V2>
	within?: TimeWindows | SessionWindows | SlidingWindows
}

export interface Topic<K = Buffer, V = Buffer> {
	name: string
	key?: Codec<K>
	value?: Codec<V>
}

export function topic<K = Buffer, V = Buffer>(
	name: string,
	options?: { key?: Codec<K>; value?: Codec<V> }
): Topic<K, V> {
	return {
		name,
		key: options?.key,
		value: options?.value,
	}
}

export interface Windowed<K> {
	key: K
	window: { start: number; end: number }
}

export class TimeWindows {
	readonly size: WindowDuration
	readonly advance?: WindowDuration

	private constructor(size: WindowDuration, advance?: WindowDuration) {
		this.size = size
		this.advance = advance
	}

	static of(size: WindowDuration): TimeWindows {
		return new TimeWindows(size)
	}

	advanceBy(advance: WindowDuration): TimeWindows {
		return new TimeWindows(this.size, advance)
	}
}

export class SessionWindows {
	readonly gap: WindowDuration

	private constructor(gap: WindowDuration) {
		this.gap = gap
	}

	static withInactivityGap(gap: WindowDuration): SessionWindows {
		return new SessionWindows(gap)
	}
}

export class SlidingWindows {
	readonly size: WindowDuration
	readonly grace?: WindowDuration

	private constructor(size: WindowDuration, grace?: WindowDuration) {
		this.size = size
		this.grace = grace
	}

	static of(size: WindowDuration): SlidingWindows {
		return new SlidingWindows(size)
	}

	gracePeriod(grace: WindowDuration): SlidingWindows {
		return new SlidingWindows(this.size, grace)
	}
}

export interface FlowApp {
	stream<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: Consumed<K, V>): KStream<K, V>
	table<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V>
	globalTable<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V>
	start(): Promise<void>
	close(): Promise<void>
	state(): StreamState
}

export type KeyValue<K, V> = readonly [K, V]

export interface KStream<K, V> {
	map<K2, V2>(fn: (key: K, value: V) => KeyValue<K2, V2>): KStream<K2, V2>
	mapValues<V2>(fn: (value: V) => V2): KStream<K, V2>
	flatMapValues<V2>(fn: (value: V) => Iterable<V2>): KStream<K, V2>
	filter(fn: (key: K, value: V) => boolean): KStream<K, V>
	peek(fn: (key: K, value: V) => void): KStream<K, V>
	selectKey<K2>(fn: (value: V, key: K) => K2): KStream<K2, V>
	merge(other: KStream<K, V>): KStream<K, V>
	branch(...predicates: Array<(key: K, value: V) => boolean>): KStream<K, V>[]
	to(topic: string, options?: Produced<K, V>): void
	through(topic: string, options?: Produced<K, V>): KStream<K, V>
	toTable(options?: Materialized<K, V>): KTable<K, V>
	groupBy<K2>(fn: (key: K, value: V) => K2, options?: Grouped<K2, V>): KGroupedStream<K2, V>
	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V>
	join<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR>
	leftJoin<V2, VR>(
		other: KStream<K, V2> | KTable<K, V2>,
		joiner: (value: V, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR>
	outerJoin<V2, VR>(
		other: KStream<K, V2>,
		joiner: (value: V | null, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KStream<K, VR>
}

export interface KTable<K, V> {
	toStream(): KStream<K, V>
	mapValues<V2>(fn: (value: V) => V2): KTable<K, V2>
	filter(fn: (key: K, value: V) => boolean): KTable<K, V>
	groupBy<K2>(fn: (key: K, value: V) => KeyValue<K2, V>, options?: Grouped<K2, V>): KGroupedTable<K2, V>
	join<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V, otherValue: V2) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR>
	leftJoin<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR>
	outerJoin<V2, VR>(
		other: KTable<K, V2>,
		joiner: (value: V | null, otherValue: V2 | null) => VR,
		options?: Joined<K, V, V2>
	): KTable<K, VR>
}

export interface KGroupedStream<K, V> {
	count(options?: Materialized<K, number>): KTable<K, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<K, V>): KTable<K, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A>
	windowedBy(windows: TimeWindows | SessionWindows | SlidingWindows): WindowedKGroupedStream<K, V>
}

export interface KGroupedTable<K, V> {
	count(options?: Materialized<K, number>): KTable<K, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<K, V>): KTable<K, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A>
}

export interface WindowedKGroupedStream<K, V> {
	count(options?: Materialized<Windowed<K>, number>): KTable<Windowed<K>, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<Windowed<K>, V>): KTable<Windowed<K>, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<Windowed<K>, A>
	): KTable<Windowed<K>, A>
}

type StreamRecord<K, V> = {
	key: K | null
	value: V
	topic: string
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
}

type StreamFormat<K, V> = {
	keyCodec?: Codec<K>
	valueCodec?: Codec<V>
}

type ActiveTransaction = {
	send(
		topic: string,
		messages: { key?: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer>; partition?: number }
	): Promise<unknown>
	sendOffsets(params: {
		groupId?: string
		consumerGroupMetadata?: {
			groupId: string
			generationId: number
			memberId: string
			groupInstanceId?: string | null
		}
		offsets: Array<{ topic: string; partition: number; offset: bigint }>
	}): Promise<void>
}

type TopicPartitionKey = `${string}:${number}`

type WorkerContext = {
	id: number
	producer: Producer
	consumer: Consumer
	activeTransaction: ActiveTransaction | null
	groupInstanceId?: string | null
	sourcesByTopic: Map<string, SourceNode<unknown, unknown>[]>
	assignedPartitions: Map<TopicPartitionKey, { topic: string; partition: number }>
	// Transaction batching state for EOS
	pendingOffsets: Map<TopicPartitionKey, { topic: string; partition: number; offset: bigint }>
	transactionActive: boolean
	commitTimer: ReturnType<typeof setTimeout> | null
	lastCommitTime: number
}

/**
 * AsyncLocalStorage for tracking the current worker context during message processing.
 * Used by changelog writers to access the correct producer/transaction.
 */
const workerContextStorage = new AsyncLocalStorage<WorkerContext>()

const DEFAULT_BUFFER_CODEC: Codec<Buffer> = bufferCodec()

abstract class Processor<K, V> {
	protected downstream: Processor<unknown, unknown>[] = []

	connect(node: Processor<unknown, unknown>): void {
		this.downstream.push(node)
	}

	cloneGraph(
		worker: WorkerContext,
		map: Map<Processor<unknown, unknown>, Processor<unknown, unknown>>
	): Processor<K, V> {
		const existing = map.get(this)
		if (existing) {
			return existing as Processor<K, V>
		}
		const cloned = this.clone(worker)
		map.set(this, cloned as Processor<unknown, unknown>)
		this.cloneEdges(cloned as Processor<unknown, unknown>, worker, map)
		return cloned
	}

	protected async forward(record: StreamRecord<unknown, unknown>): Promise<void> {
		for (const node of this.downstream) {
			await node.process(record)
		}
	}

	protected cloneEdges(
		cloned: Processor<unknown, unknown>,
		worker: WorkerContext,
		map: Map<Processor<unknown, unknown>, Processor<unknown, unknown>>
	): void {
		for (const node of this.downstream) {
			const childClone = node.cloneGraph(worker, map)
			cloned.connect(childClone)
		}
	}

	abstract clone(worker: WorkerContext): Processor<K, V>
	abstract process(record: StreamRecord<K, V>): Promise<void>
}

class PassThroughNode<K, V> extends Processor<K, V> {
	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new PassThroughNode<K, V>()
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record as StreamRecord<unknown, unknown>)
	}
}

class MapNode<K, V, K2, V2> extends Processor<K, V> {
	constructor(private readonly fn: (key: K, value: V) => KeyValue<K2, V2>) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new MapNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const [nextKey, nextValue] = this.fn(record.key as K, record.value)
		const next: StreamRecord<K2, V2> = {
			...record,
			key: nextKey,
			value: nextValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

class MapValuesNode<K, V, V2> extends Processor<K, V> {
	constructor(private readonly fn: (value: V) => V2) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new MapValuesNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const next: StreamRecord<K, V2> = {
			...record,
			value: this.fn(record.value),
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

class FlatMapValuesNode<K, V, V2> extends Processor<K, V> {
	constructor(private readonly fn: (value: V) => Iterable<V2>) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new FlatMapValuesNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		for (const value of this.fn(record.value)) {
			const next: StreamRecord<K, V2> = {
				...record,
				value,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}
	}
}

class FilterNode<K, V> extends Processor<K, V> {
	constructor(private readonly fn: (key: K, value: V) => boolean) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new FilterNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		if (this.fn(record.key as K, record.value)) {
			await this.forward(record as StreamRecord<unknown, unknown>)
		}
	}
}

class PeekNode<K, V> extends Processor<K, V> {
	constructor(private readonly fn: (key: K, value: V) => void) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new PeekNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		this.fn(record.key as K, record.value)
		await this.forward(record as StreamRecord<unknown, unknown>)
	}
}

class SelectKeyNode<K, V, K2> extends Processor<K, V> {
	constructor(private readonly fn: (value: V, key: K) => K2) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new SelectKeyNode(this.fn)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const next: StreamRecord<K2, V> = {
			...record,
			key: this.fn(record.value, record.key as K),
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

class BranchNode<K, V> extends Processor<K, V> {
	private readonly branches: Array<{
		predicate: (key: K, value: V) => boolean
		node: Processor<unknown, unknown>
	}> = []

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new BranchNode<K, V>()
	}

	addBranch(predicate: (key: K, value: V) => boolean, node: Processor<unknown, unknown>): void {
		this.branches.push({ predicate, node })
	}

	protected override cloneEdges(
		cloned: Processor<unknown, unknown>,
		worker: WorkerContext,
		map: Map<Processor<unknown, unknown>, Processor<unknown, unknown>>
	): void {
		const branchClone = cloned as BranchNode<K, V>
		for (const branch of this.branches) {
			const childClone = branch.node.cloneGraph(worker, map)
			branchClone.addBranch(branch.predicate, childClone)
		}
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		for (const branch of this.branches) {
			if (branch.predicate(record.key as K, record.value)) {
				await branch.node.process(record as StreamRecord<unknown, unknown>)
				return
			}
		}
	}
}

class ProduceNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly app: FlowAppImpl,
		private readonly topic: string,
		private readonly produced: Produced<K, V> | undefined,
		private readonly format: StreamFormat<K, V>,
		private readonly forwardAfter: boolean,
		private readonly worker?: WorkerContext
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		return new ProduceNode(this.app, this.topic, this.produced, this.format, this.forwardAfter, worker)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		if (!this.worker) {
			throw new Error('ProduceNode is not bound to a worker')
		}
		await this.app.sendToTopic(this.worker, this.topic, record, this.produced, this.format)
		if (this.forwardAfter) {
			await this.forward(record as StreamRecord<unknown, unknown>)
		}
	}
}

class SourceNode<K, V> extends Processor<K, V> {
	constructor(
		readonly topic: string,
		private readonly format: StreamFormat<K, V>
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new SourceNode(this.topic, this.format)
	}

	async handleMessage(message: Message<Buffer>, ctx: ConsumeContext): Promise<void> {
		void ctx
		const key = this.decodeKey(message.key)
		const value = this.decodeValue(message.value)

		const record: StreamRecord<K, V> = {
			key,
			value,
			topic: message.topic,
			partition: message.partition,
			offset: message.offset,
			timestamp: message.timestamp,
			headers: message.headers,
		}

		await this.forward(record as StreamRecord<unknown, unknown>)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record as StreamRecord<unknown, unknown>)
	}

	private decodeKey(raw: Buffer | null): K | null {
		if (raw === null) return null
		const codec = this.format.keyCodec
		return codec ? codec.decode(raw) : (raw as unknown as K)
	}

	private decodeValue(raw: Buffer | null): V {
		if (raw === null) {
			return null as unknown as V
		}
		const codec = this.format.valueCodec
		return codec ? codec.decode(raw) : (raw as unknown as V)
	}
}

/**
 * Processor node for aggregations (count, reduce, aggregate).
 * Maintains state in a KeyValueStore and emits updated aggregates downstream.
 */
class AggregateNode<K, V, A> extends Processor<K, V> {
	private store: KeyValueStore<K, A> | null = null

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		// Share the same store reference across clones
		return new AggregateNode<K, V, A>(this.storeName, this.storeRef, this.initializer, this.aggregator)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`State store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys for aggregations
			return
		}

		// Get current aggregate or initialize
		const storedAggregate = await store.get(key)
		const aggregate: A = storedAggregate !== undefined ? storedAggregate : this.initializer()

		// Apply aggregation
		const newAggregate = this.aggregator(key, record.value, aggregate)

		// Store updated aggregate
		await store.put(key, newAggregate)

		// Forward the updated aggregate downstream
		const next: StreamRecord<K, A> = {
			...record,
			key,
			value: newAggregate,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/** Default transaction commit interval for exactly_once mode (100ms) */
const DEFAULT_COMMIT_INTERVAL_MS = 100

class FlowAppImpl implements FlowApp {
	private readonly client: KafkaClient
	private readonly ownsClient: boolean
	private readonly sourcesByTopic = new Map<string, SourceNode<unknown, unknown>[]>()
	private readonly offsetResetByTopic = new Map<string, 'earliest' | 'latest' | 'none'>()
	private readonly partitionCounts = new Map<string, number>()
	private readonly eosEnabled: boolean
	private readonly commitIntervalMs: number
	private readonly stateStoreProvider: StateStoreProvider
	private readonly stateStores = new Map<string, KeyValueStore<unknown, unknown>>()
	private readonly changelogTopics = new Map<string, ChangelogTopicSpec>()
	private readonly changelogWriters = new Map<string, ChangelogWriter<unknown, unknown>>()
	private currentState: StreamState = 'CREATED'
	private workers: WorkerContext[] = []
	private runPromises: Promise<void>[] = []
	private stoppedWorkers = 0
	private totalWorkers = 0
	private lastError: Error | null = null
	private storeCounter = 0
	private startupProducer: Producer | null = null
	private changelogRestorationEnabled = false
	private restorationComplete = false
	private restorationQueue: Promise<void> = Promise.resolve()

	constructor(readonly config: FlowConfig) {
		this.eosEnabled = (config.processingGuarantee ?? 'at_least_once') === 'exactly_once'
		this.commitIntervalMs = config.commitIntervalMs ?? DEFAULT_COMMIT_INTERVAL_MS
		if (config.client instanceof KafkaClient) {
			this.client = config.client
			this.ownsClient = false
		} else {
			this.client = new KafkaClient(config.client)
			this.ownsClient = true
		}
		this.stateStoreProvider = config.stateStoreProvider ?? new InMemoryStateStoreProvider()
	}

	getOrCreateStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>
	): KeyValueStore<K, V> {
		const storeName = name ?? `store-${this.storeCounter++}`
		let store = this.stateStores.get(storeName)
		if (!store) {
			const innerStore = this.stateStoreProvider.createKeyValueStore(storeName, {
				keyCodec: keyCodec as Codec<unknown>,
				valueCodec: valueCodec as Codec<unknown>,
			})

			// Resolve changelog config (defaults to enabled)
			const changelogConfig = resolveChangelogConfig(changelog)

			if (changelogConfig.enabled) {
				// Create changelog spec
				const topicName =
					changelogConfig.topicName ?? buildChangelogTopicName(this.config.applicationId, storeName)

				// Partition count is always inferred from source topics to ensure state locality.
				// Task N processing partition N must write to changelog partition N.
				const spec: ChangelogTopicSpec = {
					storeName,
					topicName,
					replicationFactor: changelogConfig.replicationFactor,
					configs: getDefaultTopicConfigs(changelogConfig.topicConfigs),
					keyCodec: keyCodec as Codec<unknown>,
					valueCodec: valueCodec as Codec<unknown>,
					skipRestoration: changelogConfig.skipRestoration ?? false,
					sourceTopics: sourceTopics ?? new Set(),
					validateOnly: this.config.changelog?.autoCreate === false,
				}
				this.changelogTopics.set(storeName, spec)

				// Create changelog writer with callbacks to get producer/transaction from worker context
				const writer = new ChangelogWriter<K, V>(
					topicName,
					keyCodec,
					valueCodec,
					() => {
						const worker = workerContextStorage.getStore()
						if (!worker) {
							throw new Error('Changelog write called outside of message processing context')
						}
						return worker.producer
					},
					() => {
						const worker = workerContextStorage.getStore()
						if (!worker || !worker.activeTransaction) {
							return null
						}
						return worker.activeTransaction
					}
				)
				this.changelogWriters.set(storeName, writer as ChangelogWriter<unknown, unknown>)

				// Wrap with changelog-backed store
				store = new ChangelogBackedKeyValueStore(innerStore, writer) as KeyValueStore<unknown, unknown>
			} else {
				store = innerStore
			}

			this.stateStores.set(storeName, store)
		}
		return store as KeyValueStore<K, V>
	}

	stream<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: Consumed<K, V>): KStream<K, V> {
		const resolved = this.resolveSource(source, options)
		const node = new SourceNode<K, V>(resolved.topic, resolved.format)
		this.registerSource(resolved.topic, node, resolved.offsetReset)
		// Track source topic for changelog partition inference
		return new KStreamImpl<K, V>(this, node, resolved.format, new Set([resolved.topic]))
	}

	table<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V> {
		const resolved = this.resolveSource(source, options)
		const sourceNode = new SourceNode<K, V>(resolved.topic, resolved.format)
		this.registerSource(resolved.topic, sourceNode, resolved.offsetReset)

		// Create state store for table - prefer materialized codecs over consumed codecs
		const keyCodec = options?.materialized?.key ?? resolved.format.keyCodec
		const valueCodec = options?.materialized?.value ?? resolved.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('table() requires both key and value codecs for stateful operations')
		}

		const storeName = options?.materialized?.storeName ?? `table-${resolved.topic}-${this.storeCounter++}`
		// Pass source topic for changelog partition inference
		const sourceTopics = new Set([resolved.topic])
		const store = this.getOrCreateStore<K, V>(
			storeName,
			keyCodec,
			valueCodec,
			options?.materialized?.changelog,
			sourceTopics
		)
		const storeRef = { store }

		// Create table state node that maintains state
		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		sourceNode.connect(tableStateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, V>(this, tableStateNode, resolved.format, storeRef)
	}

	globalTable<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V> {
		const resolved = this.resolveSource(source, options)
		const sourceNode = new SourceNode<K, V>(resolved.topic, resolved.format)
		this.registerSource(resolved.topic, sourceNode, resolved.offsetReset)

		// Create state store for global table - prefer materialized codecs
		const keyCodec = options?.materialized?.key ?? resolved.format.keyCodec
		const valueCodec = options?.materialized?.value ?? resolved.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('globalTable() requires both key and value codecs for stateful operations')
		}

		const storeName = options?.materialized?.storeName ?? `global-table-${resolved.topic}-${this.storeCounter++}`
		// Pass source topic for changelog partition inference
		const sourceTopics = new Set([resolved.topic])
		const store = this.getOrCreateStore<K, V>(
			storeName,
			keyCodec,
			valueCodec,
			options?.materialized?.changelog,
			sourceTopics
		)
		const storeRef = { store }

		// Create table state node that maintains state
		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		sourceNode.connect(tableStateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, V>(this, tableStateNode, resolved.format, storeRef)
	}

	async start(): Promise<void> {
		if (this.currentState === 'RUNNING') {
			return
		}

		this.currentState = 'RUNNING'
		await this.client.connect()

		// Validate and create changelog topics (returns false if skipped due to no brokers)
		const changelogTopicsCreated = await this.validateAndCreateChangelogTopics()
		this.changelogRestorationEnabled = changelogTopicsCreated

		// Initialize all state stores
		for (const store of this.stateStores.values()) {
			await store.init()
		}

		const topics = [...this.sourcesByTopic.keys()]
		if (topics.length === 0) {
			return
		}

		const threadCount = Math.max(1, this.config.numStreamThreads ?? 1)
		this.workers = []
		this.runPromises = []
		this.totalWorkers = threadCount
		this.stoppedWorkers = 0

		const workerReady: Promise<void>[] = []

		for (let id = 0; id < threadCount; id += 1) {
			const producer = this.client.producer(this.buildProducerConfig(id, threadCount))
			const groupInstanceId = this.buildGroupInstanceId(id, threadCount)
			const consumer = this.client.consumer({
				groupId: this.config.applicationId,
				...this.config.consumer,
				autoOffsetReset: this.resolveOffsetReset(),
				groupInstanceId,
				isolationLevel: this.eosEnabled ? 'read_committed' : this.config.consumer?.isolationLevel,
			})

			const worker: WorkerContext = {
				id,
				producer,
				consumer,
				activeTransaction: null,
				groupInstanceId: groupInstanceId ?? null,
				sourcesByTopic: new Map(),
				assignedPartitions: new Map(),
				// Transaction batching state
				pendingOffsets: new Map(),
				transactionActive: false,
				commitTimer: null,
				lastCommitTime: 0,
			}

			worker.sourcesByTopic = this.buildWorkerSources(worker)
			this.workers.push(worker)
			this.attachConsumerEvents(consumer, worker)

			workerReady.push(
				new Promise<void>((resolve, reject) => {
					const timeoutMs = 10_000
					const timer = setTimeout(() => {
						cleanup()
						reject(new Error(`Consumer did not start within ${timeoutMs}ms`))
					}, timeoutMs)

					const onRunning = () => {
						cleanup()
						resolve()
					}
					const onError = (error: unknown) => {
						cleanup()
						reject(error instanceof Error ? error : new Error(String(error)))
					}
					const onStopped = () => {
						cleanup()
						reject(new Error('Consumer stopped before starting'))
					}

					const cleanup = () => {
						clearTimeout(timer)
						consumer.off('running', onRunning)
						consumer.off('error', onError)
						consumer.off('stopped', onStopped)
					}

					consumer.once('running', onRunning)
					consumer.once('error', onError)
					consumer.once('stopped', onStopped)
				})
			)

			const runPromise = consumer
				.runEach(
					topics,
					async (message, ctx) => {
						// Run within worker context for changelog writes
						await workerContextStorage.run(worker, async () => {
							const sources = worker.sourcesByTopic.get(message.topic)
							if (!sources) return
							if (!this.eosEnabled) {
								for (const source of sources) {
									await source.handleMessage(message, ctx)
								}
								return
							}
							await this.processInTransaction(message, ctx, sources, worker)
						})
					},
					this.buildRunEachOptions()
				)
				.catch(err => {
					this.lastError = err as Error
					this.currentState = 'ERROR'
				})

			this.runPromises.push(runPromise)
		}

		await Promise.all(workerReady)

		// Restore only the changelog partitions that are assigned to this application instance.
		// Workers pause their partitions in the partitionsAssigned handler to avoid processing
		// messages before state is fully restored.
		try {
			if (changelogTopicsCreated) {
				const assigned: Array<{ topic: string; partition: number }> = []
				const seen = new Set<string>()
				for (const worker of this.workers) {
					for (const tp of worker.assignedPartitions.values()) {
						const key = `${tp.topic}:${tp.partition}`
						if (seen.has(key)) continue
						seen.add(key)
						assigned.push(tp)
					}
				}

				await this.enqueueChangelogRestoration(assigned)
			}
		} catch (err) {
			this.lastError = err as Error
			this.currentState = 'ERROR'
			await this.close().catch(() => {})
			throw err
		} finally {
			for (const worker of this.workers) {
				const partitions = [...worker.assignedPartitions.values()]
				if (partitions.length > 0) {
					try {
						worker.consumer.resume(partitions)
					} catch {
						// Ignore - consumer may already be stopping after an error
					}
				}
			}
			this.restorationComplete = true
		}
	}

	private enqueueChangelogRestoration(partitions: Array<{ topic: string; partition: number }>): Promise<void> {
		if (!this.changelogRestorationEnabled || this.changelogTopics.size === 0) {
			return Promise.resolve()
		}
		if (partitions.length === 0) {
			return Promise.resolve()
		}

		const task = this.restorationQueue.then(() => this.restoreFromChangelogs(partitions))
		this.restorationQueue = task
		return task
	}

	async close(): Promise<void> {
		if (this.currentState === 'STOPPED') {
			return
		}

		// Commit any pending transactions before stopping
		for (const worker of this.workers) {
			await this.commitTransactionBatch(worker).catch(() => {
				// Ignore errors during shutdown - best effort commit
			})
		}

		for (const worker of this.workers) {
			worker.consumer.stop()
		}

		if (this.runPromises.length > 0) {
			await Promise.allSettled(this.runPromises)
		}

		for (const worker of this.workers) {
			await worker.producer.disconnect()
		}

		// Close all state stores
		await this.stateStoreProvider.close()

		if (this.ownsClient) {
			await this.client.disconnect()
		}

		this.currentState = 'STOPPED'
	}

	state(): StreamState {
		return this.currentState
	}

	getError(): Error | null {
		return this.lastError
	}

	async sendToTopic<K, V>(
		worker: WorkerContext,
		topic: string,
		record: StreamRecord<K, V>,
		options: Produced<K, V> | undefined,
		format: StreamFormat<K, V>
	): Promise<void> {
		const producer = worker.producer

		const keyCodec = options?.key ?? format.keyCodec
		const valueCodec = options?.value ?? format.valueCodec

		const key = this.encodeKey(record.key, keyCodec)
		const value = record.value === null ? null : this.encodeValue(record.value, valueCodec)

		const partition = options?.partitioner
			? await this.resolvePartition(topic, record.key, record.value, options.partitioner)
			: undefined

		if (this.eosEnabled) {
			if (!worker.activeTransaction) {
				throw new Error('exactly_once requires a transactional context')
			}
			await worker.activeTransaction.send(topic, {
				key: key ?? undefined,
				value,
				headers: record.headers,
				partition,
			})
			return
		}

		await producer.send(topic, {
			key: key ?? undefined,
			value,
			headers: record.headers,
			partition,
		})
	}

	private registerSource(
		topic: string,
		node: SourceNode<unknown, unknown>,
		offsetReset?: 'earliest' | 'latest' | 'none'
	) {
		const existing = this.sourcesByTopic.get(topic)
		if (existing) {
			existing.push(node)
		} else {
			this.sourcesByTopic.set(topic, [node])
		}

		if (offsetReset) {
			this.offsetResetByTopic.set(topic, offsetReset)
		}
	}

	private resolveSource<K, V>(source: string | Topic<K, V>, options?: Consumed<K, V>) {
		const topicName = typeof source === 'string' ? source : source.name
		const keyCodec = options?.key ?? (typeof source === 'string' ? undefined : source.key)
		const valueCodec = options?.value ?? (typeof source === 'string' ? undefined : source.value)

		const format: StreamFormat<K, V> = {
			keyCodec: keyCodec ?? (DEFAULT_BUFFER_CODEC as Codec<K>),
			valueCodec: valueCodec ?? (DEFAULT_BUFFER_CODEC as Codec<V>),
		}

		return {
			topic: topicName,
			format,
			offsetReset: options?.offsetReset,
		}
	}

	private resolveOffsetReset(): 'earliest' | 'latest' | 'none' | undefined {
		const values = new Set(this.offsetResetByTopic.values())
		if (values.size > 1) {
			throw new Error('Multiple offsetReset values detected. Use a separate flow for each offset policy.')
		}
		return values.size === 1 ? [...values][0] : this.config.consumer?.autoOffsetReset
	}

	/**
	 * Validate existing changelog topics and create missing ones.
	 *
	 * Partition count is always inferred from source topics to ensure state locality.
	 * Task N processing partition N must write to changelog partition N.
	 *
	 * For each changelog topic:
	 * 1. Infer partition count from source topics (max partition count)
	 * 2. If the topic exists, validate it has the expected partition count
	 * 3. If the topic doesn't exist, create it (unless validateOnly mode)
	 *
	 * Returns true if all validations passed and topics were created/validated.
	 * Returns false if creation was skipped due to connection issues.
	 *
	 * @throws SourceTopicNotFoundError if a source topic doesn't exist
	 * @throws ChangelogPartitionMismatchError if existing changelog has wrong partition count
	 */
	private async validateAndCreateChangelogTopics(): Promise<boolean> {
		if (this.changelogTopics.size === 0) {
			return true
		}

		// 1. Collect all topics we need metadata for
		const allTopics = new Set<string>()
		for (const spec of this.changelogTopics.values()) {
			for (const topic of spec.sourceTopics) {
				allTopics.add(topic)
			}
			allTopics.add(spec.topicName)
		}

		// 2. Fetch metadata
		let metadata
		try {
			metadata = await this.client.getMetadata([...allTopics])
		} catch (err) {
			const error = err as Error
			if (error.message?.includes('No brokers available')) {
				// Skip validation/creation - changelog writes will fail at runtime if topics don't exist
				return false
			}
			throw err
		}

		// 3. Validate and determine partition counts
		const topicsToCreate: Array<{
			name: string
			numPartitions: number
			replicationFactor?: number
			configs: Record<string, string>
		}> = []

		for (const [storeName, spec] of this.changelogTopics) {
			// Infer partition count from source topics (always - no override allowed)
			let requiredPartitions = 0
			for (const sourceTopic of spec.sourceTopics) {
				const topicMeta = metadata.topics.get(sourceTopic)
				if (!topicMeta) {
					throw new SourceTopicNotFoundError(sourceTopic, storeName)
				}
				requiredPartitions = Math.max(requiredPartitions, topicMeta.partitions.size)
			}
			if (requiredPartitions === 0) {
				// No source topics or all have 0 partitions, use 1 as default
				requiredPartitions = 1
			}

			// Check existing changelog
			const existingMeta = metadata.topics.get(spec.topicName)
			if (existingMeta) {
				const actualPartitions = existingMeta.partitions.size
				if (actualPartitions !== requiredPartitions) {
					throw new ChangelogPartitionMismatchError(spec.topicName, requiredPartitions, actualPartitions, [
						...spec.sourceTopics,
					])
				}
				// Exists with correct partitions, skip creation
				continue
			}

			// Need to create (unless validateOnly)
			if (!spec.validateOnly) {
				topicsToCreate.push({
					name: spec.topicName,
					numPartitions: requiredPartitions,
					replicationFactor: spec.replicationFactor ?? this.config.changelog?.replicationFactor,
					configs: spec.configs,
				})
			}
		}

		// 4. Create missing topics
		if (topicsToCreate.length > 0) {
			await this.client.createTopics(topicsToCreate)
		}

		return true
	}

	/**
	 * Restore state from changelog topics for all stores.
	 */
	private async restoreFromChangelogs(
		assignedPartitions: Array<{ topic: string; partition: number }>
	): Promise<void> {
		for (const [storeName, spec] of this.changelogTopics) {
			if (spec.skipRestoration) {
				continue
			}

			const store = this.stateStores.get(storeName)
			if (!store) {
				continue
			}

			const partitions = spec.sourceTopics.size
				? [...new Set(assignedPartitions.filter(tp => spec.sourceTopics.has(tp.topic)).map(tp => tp.partition))]
				: undefined
			if (partitions?.length === 0) {
				continue
			}

			// Get the inner store for restoration (unwrap changelog-backed store)
			const innerStore = store instanceof ChangelogBackedKeyValueStore ? store.innerStore : store

			const restorer = new ChangelogRestorer(spec.topicName, spec.keyCodec, spec.valueCodec, innerStore)

			try {
				const restoredCount = await restorer.restore(
					this.client,
					this.config.changelog?.restoration,
					partitions
				)
				if (restoredCount > 0) {
					// Log restoration (optional - could add a logger)
				}
			} catch (err) {
				// If topic doesn't exist or is empty, that's ok - first startup
				// Also handle connection errors (e.g., in test environments)
				const error = err as Error & { code?: string }
				if (error.code === 'UNKNOWN_TOPIC_OR_PARTITION') {
					continue
				}
				if (error.message?.includes('No brokers available')) {
					continue
				}
				throw err
			}
		}
	}

	private buildWorkerSources(worker: WorkerContext): Map<string, SourceNode<unknown, unknown>[]> {
		const clonedByTopic = new Map<string, SourceNode<unknown, unknown>[]>()
		const cloneMap = new Map<Processor<unknown, unknown>, Processor<unknown, unknown>>()

		for (const [topic, sources] of this.sourcesByTopic) {
			for (const source of sources) {
				const cloned = source.cloneGraph(worker, cloneMap)
				const list = clonedByTopic.get(topic) ?? []
				list.push(cloned as SourceNode<unknown, unknown>)
				clonedByTopic.set(topic, list)
			}
		}

		return clonedByTopic
	}

	private buildRunEachOptions(): RunEachOptions | undefined {
		if (!this.eosEnabled) {
			return this.config.runEach
		}

		const base = this.config.runEach ?? {}
		return {
			...base,
			autoCommit: false,
			commitOffsets: false,
			partitionConcurrency: 1,
		}
	}

	private buildProducerConfig(workerId: number, threadCount: number): ProducerConfig | undefined {
		if (!this.eosEnabled) {
			return this.config.producer
		}

		const base = this.config.producer ?? {}
		const baseTransactionalId = base.transactionalId ?? this.buildTransactionalId()
		const transactionalId = threadCount > 1 ? `${baseTransactionalId}-w${workerId}` : baseTransactionalId
		return {
			...base,
			transactionalId,
			idempotent: true,
		}
	}

	private buildTransactionalId(): string {
		const clientId = this.getClientId()
		if (!clientId) {
			throw new Error('exactly_once requires a clientId to derive transactionalId')
		}
		return `${this.config.applicationId}-${clientId}`
	}

	private buildGroupInstanceId(workerId: number, threadCount: number): string | undefined {
		const base = this.config.consumer?.groupInstanceId
		if (!base) {
			return undefined
		}
		return threadCount > 1 ? `${base}-w${workerId}` : base
	}

	private getClientId(): string {
		if (this.config.client instanceof KafkaClient) {
			return this.config.client.getConfig().clientId
		}
		return this.config.client.clientId
	}

	private getConsumerGroupMetadata(
		consumer: Consumer,
		groupInstanceId?: string | null
	): {
		groupId: string
		generationId: number
		memberId: string
		groupInstanceId?: string | null
	} | null {
		const owner = consumer as unknown as {
			consumerGroup?: { currentMemberId: string; currentGenerationId: number }
		}
		const group = owner?.consumerGroup
		if (!group) {
			return null
		}
		return {
			groupId: this.config.applicationId,
			generationId: group.currentGenerationId,
			memberId: group.currentMemberId,
			groupInstanceId: groupInstanceId ?? null,
		}
	}

	/**
	 * Process a message within a batched transaction.
	 * Multiple messages are batched into a single transaction and committed together
	 * when the commit interval elapses.
	 */
	private async processInTransaction(
		message: Message<Buffer>,
		ctx: ConsumeContext,
		sources: SourceNode<unknown, unknown>[],
		worker: WorkerContext
	): Promise<void> {
		const producer = worker.producer
		if (!producer) {
			throw new Error('Flow is not started')
		}

		// Start a new transaction if one isn't active
		if (!worker.transactionActive) {
			await this.beginTransaction(worker)
		}

		// Process the message within the current transaction
		for (const source of sources) {
			await source.handleMessage(message, ctx)
		}

		// Track this offset for the batch commit
		const key: TopicPartitionKey = `${message.topic}:${message.partition}`
		const existingOffset = worker.pendingOffsets.get(key)
		// Only update if this offset is higher (messages may be processed out of order within a partition)
		if (!existingOffset || message.offset >= existingOffset.offset) {
			worker.pendingOffsets.set(key, {
				topic: message.topic,
				partition: message.partition,
				offset: message.offset + 1n, // Commit the next offset
			})
		}

		// Schedule a commit if the interval has elapsed
		const now = Date.now()
		if (now - worker.lastCommitTime >= this.commitIntervalMs) {
			await this.commitTransactionBatch(worker)
		} else if (!worker.commitTimer) {
			// Schedule a commit for when the interval elapses
			this.scheduleCommit(worker)
		}
	}

	/**
	 * Begin a new transaction for the worker
	 */
	private async beginTransaction(worker: WorkerContext): Promise<void> {
		const producer = worker.producer

		// Begin the transaction using the producer's internal transaction start
		// We access the producer's transaction method but don't await its completion yet
		worker.activeTransaction = await this.startProducerTransaction(producer)
		worker.transactionActive = true
		worker.lastCommitTime = Date.now()
	}

	/**
	 * Start a producer transaction and return the transaction handle
	 */
	private startProducerTransaction(producer: Producer): Promise<ActiveTransaction> {
		return new Promise((resolve, reject) => {
			// We need to call producer.transaction() but capture the tx object
			// and not wait for the entire transaction to complete
			const txPromise = producer.transaction(async tx => {
				resolve(tx as ActiveTransaction)
				// This promise will be resolved externally when we commit
				return new Promise<void>((resolveInner, rejectInner) => {
					// Store the resolve/reject for later use
					;(tx as ActiveTransaction & { _resolve?: () => void; _reject?: (err: Error) => void })._resolve =
						resolveInner
					;(tx as ActiveTransaction & { _resolve?: () => void; _reject?: (err: Error) => void })._reject =
						rejectInner
				})
			})
			txPromise.catch(reject)
		})
	}

	/**
	 * Commit the current transaction batch
	 */
	private async commitTransactionBatch(worker: WorkerContext): Promise<void> {
		// Cancel any pending commit timer
		this.cancelCommitTimer(worker)

		if (!worker.transactionActive || !worker.activeTransaction) {
			return
		}

		const tx = worker.activeTransaction
		const offsets = Array.from(worker.pendingOffsets.values())

		if (offsets.length > 0) {
			const groupMetadata = this.getConsumerGroupMetadata(worker.consumer, worker.groupInstanceId)
			if (!groupMetadata || groupMetadata.generationId < 0) {
				throw new Error('exactly_once requires active consumer group metadata')
			}

			// Send all accumulated offsets
			await tx.sendOffsets({
				consumerGroupMetadata: groupMetadata,
				offsets,
			})
		}

		// Complete the transaction by resolving the inner promise
		const txWithResolve = tx as ActiveTransaction & { _resolve?: () => void }
		if (txWithResolve._resolve) {
			txWithResolve._resolve()
		}

		// Reset state for next batch
		worker.pendingOffsets.clear()
		worker.activeTransaction = null
		worker.transactionActive = false
		worker.lastCommitTime = Date.now()
	}

	/**
	 * Schedule a commit timer for the worker
	 */
	private scheduleCommit(worker: WorkerContext): void {
		if (worker.commitTimer) {
			return
		}

		const timeUntilCommit = Math.max(0, this.commitIntervalMs - (Date.now() - worker.lastCommitTime))
		worker.commitTimer = setTimeout(() => {
			worker.commitTimer = null
			this.commitTransactionBatch(worker).catch(err => {
				this.lastError = err as Error
				this.currentState = 'ERROR'
			})
		}, timeUntilCommit)
	}

	/**
	 * Cancel any pending commit timer for the worker
	 */
	private cancelCommitTimer(worker: WorkerContext): void {
		if (worker.commitTimer) {
			clearTimeout(worker.commitTimer)
			worker.commitTimer = null
		}
	}

	private encodeKey<K>(key: K | null, codec?: Codec<K>): Buffer | null {
		if (key === null || key === undefined) {
			return null
		}
		if (codec) {
			return codec.encode(key)
		}
		if (Buffer.isBuffer(key)) {
			return key
		}
		throw new Error('No key codec provided for non-Buffer key')
	}

	private encodeValue<V>(value: V, codec?: Codec<V>): Buffer {
		if (codec) {
			return codec.encode(value)
		}
		if (Buffer.isBuffer(value)) {
			return value
		}
		throw new Error('No value codec provided for non-Buffer value')
	}

	private async resolvePartition<K, V>(
		topic: string,
		key: K | null,
		value: V,
		partitioner: (key: K | null, value: V, partitionCount: number) => number
	): Promise<number> {
		const count = await this.getPartitionCount(topic)
		const partition = partitioner(key, value, count)
		if (!Number.isInteger(partition) || partition < 0 || partition >= count) {
			throw new Error(`partitioner returned invalid partition ${partition} for topic ${topic}`)
		}
		return partition
	}

	private async getPartitionCount(topic: string): Promise<number> {
		const cached = this.partitionCounts.get(topic)
		if (cached !== undefined) {
			return cached
		}

		const metadata = await this.client.getMetadata([topic])
		const meta = metadata.topics.get(topic)
		if (!meta) {
			throw new Error(`Topic ${topic} not found in metadata`)
		}

		const count = meta.partitions.size
		this.partitionCounts.set(topic, count)
		return count
	}

	private attachConsumerEvents(consumer: Consumer, worker: WorkerContext): void {
		consumer.on('rebalance', () => {
			this.currentState = 'REBALANCING'
			// Commit any pending transactions before rebalance completes
			// This is done synchronously in the event handler to ensure
			// offsets are committed before partitions are reassigned
			if (this.eosEnabled && worker.transactionActive) {
				this.commitTransactionBatch(worker).catch(err => {
					this.lastError = err as Error
					this.currentState = 'ERROR'
				})
			}
		})
		consumer.on('partitionsAssigned', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.set(`${tp.topic}:${tp.partition}`, tp)
			}

			if (this.changelogRestorationEnabled && this.changelogTopics.size > 0) {
				// Prevent processing messages until the app restores state for these partitions.
				try {
					consumer.pause(partitions)
				} catch {
					// Ignore - pause is best effort and may fail if the consumer is stopping
				}

				// After startup, restore newly assigned partitions before resuming them.
				if (this.restorationComplete) {
					void this.enqueueChangelogRestoration(partitions)
						.then(() => {
							try {
								consumer.resume(partitions)
							} catch {
								// Ignore - consumer may have been stopped while restoring
							}
						})
						.catch(err => {
							this.lastError = err as Error
							this.currentState = 'ERROR'
							consumer.stop()
						})
				}
			}

			if (this.currentState !== 'ERROR') {
				this.currentState = 'RUNNING'
			}
		})
		consumer.on('partitionsRevoked', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
		})
		consumer.on('partitionsLost', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
		})
		consumer.on('error', error => {
			this.lastError = error
			this.currentState = 'ERROR'
		})
		consumer.on('stopped', () => {
			// Cancel any pending commit timer
			this.cancelCommitTimer(worker)
			this.stoppedWorkers += 1
			if (this.currentState !== 'ERROR' && this.stoppedWorkers >= this.totalWorkers) {
				this.currentState = 'STOPPED'
			}
		})
	}
}

class KStreamImpl<K, V> implements KStream<K, V> {
	constructor(
		readonly app: FlowAppImpl,
		private readonly node: Processor<K, V>,
		private readonly format: StreamFormat<K, V>,
		/** Source topics that feed into this stream. Used for changelog partition inference. */
		readonly sourceTopics: Set<string> = new Set()
	) {}

	map<K2, V2>(fn: (key: K, value: V) => KeyValue<K2, V2>): KStream<K2, V2> {
		const node = new MapNode<K, V, K2, V2>(fn)
		this.node.connect(node as unknown as Processor<unknown, unknown>)
		return new KStreamImpl<K2, V2>(this.app, node as unknown as Processor<K2, V2>, {}, this.sourceTopics)
	}

	mapValues<V2>(fn: (value: V) => V2): KStream<K, V2> {
		const node = new MapValuesNode<K, V, V2>(fn)
		this.node.connect(node as unknown as Processor<unknown, unknown>)
		return new KStreamImpl<K, V2>(
			this.app,
			node as unknown as Processor<K, V2>,
			{ keyCodec: this.format.keyCodec },
			this.sourceTopics
		)
	}

	flatMapValues<V2>(fn: (value: V) => Iterable<V2>): KStream<K, V2> {
		const node = new FlatMapValuesNode<K, V, V2>(fn)
		this.node.connect(node as unknown as Processor<unknown, unknown>)
		return new KStreamImpl<K, V2>(
			this.app,
			node as unknown as Processor<K, V2>,
			{ keyCodec: this.format.keyCodec },
			this.sourceTopics
		)
	}

	filter(fn: (key: K, value: V) => boolean): KStream<K, V> {
		const node = new FilterNode<K, V>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V>(this.app, node, this.format, this.sourceTopics)
	}

	peek(fn: (key: K, value: V) => void): KStream<K, V> {
		const node = new PeekNode<K, V>(fn)
		this.node.connect(node)
		return new KStreamImpl<K, V>(this.app, node, this.format, this.sourceTopics)
	}

	selectKey<K2>(fn: (value: V, key: K) => K2): KStream<K2, V> {
		const node = new SelectKeyNode<K, V, K2>(fn)
		this.node.connect(node as unknown as Processor<unknown, unknown>)
		return new KStreamImpl<K2, V>(
			this.app,
			node as unknown as Processor<K2, V>,
			{ valueCodec: this.format.valueCodec },
			this.sourceTopics
		)
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

	branch(...predicates: Array<(key: K, value: V) => boolean>): KStream<K, V>[] {
		const node = new BranchNode<K, V>()
		this.node.connect(node)

		return predicates.map(predicate => {
			const branchNode = new PassThroughNode<K, V>()
			node.addBranch(predicate, branchNode)
			return new KStreamImpl<K, V>(this.app, branchNode, this.format, this.sourceTopics)
		})
	}

	to(topic: string, options?: Produced<K, V>): void {
		const node = new ProduceNode<K, V>(this.app, topic, options, this.format, false)
		this.node.connect(node)
	}

	through(topic: string, options?: Produced<K, V>): KStream<K, V> {
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

		const storeName = options?.storeName ?? `toTable-store-${this.app['storeCounter']++}`
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
		this.node.connect(tableStateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, V>(this.app, tableStateNode, this.format, storeRef)
	}

	groupBy<K2>(fn: (key: K, value: V) => K2, options?: Grouped<K2, V>): KGroupedStream<K2, V> {
		// Add a selectKey node to re-key the stream
		const selectNode = new SelectKeyNode<K, V, K2>((value, key) => fn(key, value))
		this.node.connect(selectNode as unknown as Processor<unknown, unknown>)

		const keyCodec = options?.key ?? (undefined as Codec<K2> | undefined)
		const valueCodec = options?.value ?? this.format.valueCodec

		return new KGroupedStreamImpl<K2, V>(
			this.app,
			selectNode as unknown as Processor<K2, V>,
			{ keyCodec, valueCodec },
			this.sourceTopics
		)
	}

	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V> {
		const keyCodec = options?.key ?? this.format.keyCodec
		const valueCodec = options?.value ?? this.format.valueCodec

		return new KGroupedStreamImpl<K, V>(this.app, this.node, { keyCodec, valueCodec }, this.sourceTopics)
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
			this.node.connect(joinNode as unknown as Processor<unknown, unknown>)

			// Stream-table join: only stream's source topics affect partitioning
			return new KStreamImpl<K, VR>(
				this.app,
				joinNode as unknown as Processor<K, VR>,
				{ keyCodec: this.format.keyCodec },
				this.sourceTopics
			)
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

		const joinWindowMs =
			joinWindow instanceof TimeWindows
				? parseWindowDuration(joinWindow.size)
				: joinWindow instanceof SlidingWindows
					? parseWindowDuration(joinWindow.size)
					: parseWindowDuration(joinWindow.gap)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Merge source topics from both streams for stream-stream join
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])

		// Create window stores for both streams
		const leftStoreName = `stream-join-left-${this.app['storeCounter']++}`
		const rightStoreName = `stream-join-right-${this.app['storeCounter']++}`

		const leftStore = this.app['stateStoreProvider'].createWindowStore<K, V>(leftStoreName, {
			keyCodec,
			valueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})
		const rightStore = this.app['stateStoreProvider'].createWindowStore<K, V2>(rightStoreName, {
			keyCodec,
			valueCodec: otherValueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})

		this.app['stateStores'].set(leftStoreName, leftStore as KeyValueStore<unknown, unknown>)
		this.app['stateStores'].set(rightStoreName, rightStore as KeyValueStore<unknown, unknown>)

		const leftStoreRef = { store: leftStore }
		const rightStoreRef = { store: rightStore }

		// Create merge node for results
		const mergeNode = new PassThroughNode<K, VR>()

		// Left stream join processor
		const leftJoinNode = new StreamStreamJoinNode<K, V, V2, VR>(
			leftStoreRef,
			rightStoreRef,
			joiner,
			joinWindowMs,
			false // don't swap args
		)
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// Right stream join processor - swap args so joiner receives (left, right) order
		const rightJoinNode = new StreamStreamJoinNode<K, V2, V, VR>(
			rightStoreRef as { store: WindowStore<K, V2> | null },
			leftStoreRef as { store: WindowStore<K, V> | null },
			joiner as unknown as (value: V2, otherValue: V) => VR,
			joinWindowMs,
			true // swap args: call joiner(otherValue, record.value) = joiner(left, right)
		)
		otherStream.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

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
			this.node.connect(joinNode as unknown as Processor<unknown, unknown>)

			// Stream-table join: only stream's source topics affect partitioning
			return new KStreamImpl<K, VR>(
				this.app,
				joinNode as unknown as Processor<K, VR>,
				{ keyCodec: this.format.keyCodec },
				this.sourceTopics
			)
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

		const joinWindowMs =
			joinWindow instanceof TimeWindows
				? parseWindowDuration(joinWindow.size)
				: joinWindow instanceof SlidingWindows
					? parseWindowDuration(joinWindow.size)
					: parseWindowDuration(joinWindow.gap)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Merge source topics from both streams
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])

		// Create window stores for both streams
		const leftStoreName = `stream-left-join-left-${this.app['storeCounter']++}`
		const rightStoreName = `stream-left-join-right-${this.app['storeCounter']++}`

		const leftStore = this.app['stateStoreProvider'].createWindowStore<K, V>(leftStoreName, {
			keyCodec,
			valueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})
		const rightStore = this.app['stateStoreProvider'].createWindowStore<K, V2>(rightStoreName, {
			keyCodec,
			valueCodec: otherValueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})

		this.app['stateStores'].set(leftStoreName, leftStore as KeyValueStore<unknown, unknown>)
		this.app['stateStores'].set(rightStoreName, rightStore as KeyValueStore<unknown, unknown>)

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
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// Right stream inner-join processor (only emits if left exists)
		const rightJoinNode = new StreamStreamJoinNode<K, V2, V, VR>(
			rightStoreRef as { store: WindowStore<K, V2> | null },
			leftStoreRef as { store: WindowStore<K, V> | null },
			joiner as unknown as (value: V2, otherValue: V) => VR,
			joinWindowMs,
			true // swap args: call joiner(otherValue, record.value) = joiner(left, right)
		)
		otherStream.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

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

		const joinWindowMs =
			joinWindow instanceof TimeWindows
				? parseWindowDuration(joinWindow.size)
				: joinWindow instanceof SlidingWindows
					? parseWindowDuration(joinWindow.size)
					: parseWindowDuration(joinWindow.gap)

		const keyCodec = this.format.keyCodec
		const valueCodec = this.format.valueCodec
		const otherValueCodec = otherStream.format.valueCodec

		if (!keyCodec || !valueCodec || !otherValueCodec) {
			throw new Error('Stream-stream join requires key and value codecs')
		}

		// Create window stores for both streams
		const leftStoreName = `stream-outer-join-left-${this.app['storeCounter']++}`
		const rightStoreName = `stream-outer-join-right-${this.app['storeCounter']++}`

		const leftStore = this.app['stateStoreProvider'].createWindowStore<K, V>(leftStoreName, {
			keyCodec,
			valueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})
		const rightStore = this.app['stateStoreProvider'].createWindowStore<K, V2>(rightStoreName, {
			keyCodec,
			valueCodec: otherValueCodec,
			retentionMs: joinWindowMs * 2,
			windowSizeMs: joinWindowMs,
		})

		this.app['stateStores'].set(leftStoreName, leftStore as KeyValueStore<unknown, unknown>)
		this.app['stateStores'].set(rightStoreName, rightStore as KeyValueStore<unknown, unknown>)

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
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// Right stream outer-join processor - swap args so joiner receives (left, right) order
		const rightJoinNode = new StreamStreamOuterJoinNode<K, V2, V, VR>(
			rightStoreRef as { store: WindowStore<K, V2> | null },
			leftStoreRef as { store: WindowStore<K, V> | null },
			joiner as unknown as (value: V2 | null, otherValue: V | null) => VR,
			joinWindowMs,
			true // swap args: call joiner(otherValue, record.value) = joiner(left, right)
		)
		otherStream.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// Merge source topics from both streams for partition inference
		const mergedTopics = new Set([...this.sourceTopics, ...otherStream.sourceTopics])
		return new KStreamImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec }, mergedTopics)
	}
}

/**
 * Processor node that maintains table state in a KeyValueStore.
 * Intercepts records and stores them, then forwards downstream.
 */
class TableStateNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: KeyValueStore<K, V> | null }
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new TableStateNode<K, V>(this.storeName, this.storeRef)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Table state store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key !== null) {
			// Handle tombstones: null value means delete from table
			if (record.value === null) {
				await store.delete(key)
			} else {
				await store.put(key, record.value)
			}
		}

		await this.forward(record as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for KTable.groupBy() that handles retractions.
 * When a source table row changes, this node:
 * 1. Emits a tombstone for the old grouped key (if key changed)
 * 2. Emits the new key-value pair
 * 3. Tracks source-key -> grouped-key mappings in a state store
 */
class TableGroupByNode<K, V, K2> extends Processor<K, V> {
	constructor(
		private readonly fn: (key: K, value: V) => KeyValue<K2, V>,
		private readonly keyMappingStoreRef: { store: KeyValueStore<K, K2> | null },
		private readonly sourceKeyCodec: Codec<K>,
		private readonly groupedKeyCodec: Codec<K2>
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new TableGroupByNode<K, V, K2>(
			this.fn,
			this.keyMappingStoreRef,
			this.sourceKeyCodec,
			this.groupedKeyCodec
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.keyMappingStoreRef.store
		if (!store) {
			throw new Error('Key mapping store not initialized for table groupBy')
		}

		const sourceKey = record.key
		if (sourceKey === null) {
			return
		}

		// Get the previous grouped key for this source key
		const previousGroupedKey = await store.get(sourceKey)

		// Handle tombstone: source row deleted
		if (record.value === null) {
			if (previousGroupedKey !== undefined) {
				// Emit tombstone for the old grouped key
				const tombstone: StreamRecord<K2, V> = {
					...record,
					key: previousGroupedKey,
					value: null as unknown as V,
				}
				await this.forward(tombstone as StreamRecord<unknown, unknown>)
				// Remove the key mapping
				await store.delete(sourceKey)
			}
			return
		}

		// Compute new grouped key
		const [newGroupedKey, newValue] = this.fn(sourceKey, record.value)

		// Check if grouped key changed
		if (previousGroupedKey !== undefined) {
			const prevKeyBytes = this.groupedKeyCodec.encode(previousGroupedKey)
			const newKeyBytes = this.groupedKeyCodec.encode(newGroupedKey)
			if (!prevKeyBytes.equals(newKeyBytes)) {
				// Key changed: emit tombstone for old key
				const tombstone: StreamRecord<K2, V> = {
					...record,
					key: previousGroupedKey,
					value: null as unknown as V,
				}
				await this.forward(tombstone as StreamRecord<unknown, unknown>)
			}
		}

		// Store the new key mapping
		await store.put(sourceKey, newGroupedKey)

		// Emit the new key-value pair
		const next: StreamRecord<K2, V> = {
			...record,
			key: newGroupedKey,
			value: newValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for Stream-Table inner join.
 * For each stream record, looks up the table value and applies the joiner.
 */
class StreamTableJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly tableStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new StreamTableJoinNode<K, V1, V2, VR>(this.tableStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.tableStoreRef.store
		if (!store) {
			throw new Error('Table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys
			return
		}

		const tableValue = await store.get(key)
		if (tableValue === undefined) {
			// Inner join: skip if no matching table record
			return
		}

		const joinedValue = this.joiner(record.value, tableValue)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for Stream-Table left join.
 * For each stream record, looks up the table value (may be null) and applies the joiner.
 */
class StreamTableLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly tableStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new StreamTableLeftJoinNode<K, V1, V2, VR>(this.tableStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.tableStoreRef.store
		if (!store) {
			throw new Error('Table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			// Skip records with null keys
			return
		}

		const tableValue = await store.get(key)
		const joinedValue = this.joiner(record.value, tableValue ?? null)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for Stream-Stream join (inner join).
 * Stores records in a window store and looks up matching records from the other stream.
 */
class StreamStreamJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly joinWindowMs: number,
		private readonly swapJoinerArgs: boolean = false
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new StreamStreamJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.swapJoinerArgs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		// Store this record in my window store
		await myStore.put({ key, windowStart, windowEnd }, record.value)

		// Look up matching records from the other stream within the join window
		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			// For the right side, we need to swap args so joiner receives (leftVal, rightVal)
			const joinedValue = this.swapJoinerArgs
				? (this.joiner as unknown as (v2: V2, v1: V1) => VR)(otherValue, record.value)
				: this.joiner(record.value, otherValue)

			const next: StreamRecord<K, VR> = {
				...record,
				value: joinedValue,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}
	}
}

/**
 * Processor node for Stream-Stream left join.
 */
class StreamStreamLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new StreamStreamLeftJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		// Store this record in my window store
		await myStore.put({ key, windowStart, windowEnd }, record.value)

		// Look up matching records from the other stream within the join window
		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			const joinedValue = this.joiner(record.value, otherValue)
			const next: StreamRecord<K, VR> = {
				...record,
				value: joinedValue,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}

		// Left join: emit with null if no match found
		if (!hasMatch) {
			const joinedValue = this.joiner(record.value, null)
			const next: StreamRecord<K, VR> = {
				...record,
				value: joinedValue,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}
	}
}

/**
 * Processor node for Stream-Stream outer join.
 */
class StreamStreamOuterJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly myStoreRef: { store: WindowStore<K, V1> | null },
		private readonly otherStoreRef: { store: WindowStore<K, V2> | null },
		private readonly joiner: (value: V1 | null, otherValue: V2 | null) => VR,
		private readonly joinWindowMs: number,
		private readonly swapJoinerArgs: boolean = false
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new StreamStreamOuterJoinNode<K, V1, V2, VR>(
			this.myStoreRef,
			this.otherStoreRef,
			this.joiner,
			this.joinWindowMs,
			this.swapJoinerArgs
		)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const myStore = this.myStoreRef.store
		const otherStore = this.otherStoreRef.store
		if (!myStore || !otherStore) {
			throw new Error('Window stores not initialized for stream-stream join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const timestamp = Number(record.timestamp)
		const windowStart = timestamp
		const windowEnd = timestamp + this.joinWindowMs

		// Store this record in my window store
		await myStore.put({ key, windowStart, windowEnd }, record.value)

		// Look up matching records from the other stream within the join window
		const searchFrom = timestamp - this.joinWindowMs
		const searchTo = timestamp + this.joinWindowMs

		let hasMatch = false
		for await (const [windowedKey, otherValue] of otherStore.fetch(key, searchFrom, searchTo)) {
			void windowedKey
			hasMatch = true
			// For the right side, we need to swap args so joiner receives (leftVal, rightVal)
			const joinedValue = this.swapJoinerArgs
				? (this.joiner as unknown as (v2: V2 | null, v1: V1 | null) => VR)(otherValue, record.value)
				: this.joiner(record.value, otherValue)

			const next: StreamRecord<K, VR> = {
				...record,
				value: joinedValue,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}

		// Outer join: emit with null if no match found
		if (!hasMatch) {
			// For the right side with no match, swap args: (null, record.value) instead of (record.value, null)
			const joinedValue = this.swapJoinerArgs
				? (this.joiner as unknown as (v2: V2 | null, v1: V1 | null) => VR)(null, record.value)
				: this.joiner(record.value, null)

			const next: StreamRecord<K, VR> = {
				...record,
				value: joinedValue,
			}
			await this.forward(next as StreamRecord<unknown, unknown>)
		}
	}
}

/**
 * Processor node for Table-Table inner join.
 * When a record arrives from one table, looks up the other table and emits a join result.
 */
class TableTableJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly otherStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new TableTableJoinNode<K, V1, V2, VR>(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.otherStoreRef.store
		if (!store) {
			throw new Error('Other table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const otherValue = await store.get(key)
		if (otherValue === undefined) {
			// Inner join: skip if no matching record in other table
			return
		}

		const joinedValue = this.joiner(record.value, otherValue)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for Table-Table left join.
 */
class TableTableLeftJoinNode<K, V1, V2, VR> extends Processor<K, V1> {
	constructor(
		private readonly otherStoreRef: { store: KeyValueStore<K, V2> | null },
		private readonly joiner: (value: V1, otherValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V1> {
		void worker
		return new TableTableLeftJoinNode<K, V1, V2, VR>(this.otherStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = this.otherStoreRef.store
		if (!store) {
			throw new Error('Other table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const otherValue = await store.get(key)
		const joinedValue = this.joiner(record.value, otherValue ?? null)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for Table-Table outer join from the "right" side.
 * Used when the right table updates and needs to look up the left table.
 */
class TableTableOuterJoinRightNode<K, V1, V2, VR> extends Processor<K, V2> {
	constructor(
		private readonly leftStoreRef: { store: KeyValueStore<K, V1> | null },
		private readonly joiner: (leftValue: V1 | null, rightValue: V2 | null) => VR
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V2> {
		void worker
		return new TableTableOuterJoinRightNode<K, V1, V2, VR>(this.leftStoreRef, this.joiner)
	}

	async process(record: StreamRecord<K, V2>): Promise<void> {
		const store = this.leftStoreRef.store
		if (!store) {
			throw new Error('Left table state store not initialized for join')
		}

		const key = record.key
		if (key === null) {
			return
		}

		const leftValue = await store.get(key)
		const joinedValue = this.joiner(leftValue ?? null, record.value)
		const next: StreamRecord<K, VR> = {
			...record,
			value: joinedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

class KTableImpl<K, V> implements KTable<K, V> {
	readonly storeRef: { store: KeyValueStore<K, V> | null }

	constructor(
		readonly app: FlowAppImpl,
		readonly node: Processor<K, V>,
		readonly format: StreamFormat<K, V>,
		storeRef?: { store: KeyValueStore<K, V> | null }
	) {
		this.storeRef = storeRef ?? { store: null }
	}

	toStream(): KStream<K, V> {
		return new KStreamImpl<K, V>(this.app, this.node, this.format)
	}

	mapValues<V2>(fn: (value: V) => V2): KTable<K, V2> {
		const node = new MapValuesNode<K, V, V2>(fn)
		this.node.connect(node as unknown as Processor<unknown, unknown>)
		return new KTableImpl<K, V2>(this.app, node as unknown as Processor<K, V2>, { keyCodec: this.format.keyCodec })
	}

	filter(fn: (key: K, value: V) => boolean): KTable<K, V> {
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

		// Create a key mapping store to track source key -> grouped key
		const mappingStoreName = `groupby-mapping-${this.app['storeCounter']++}`
		const mappingStore = this.app.getOrCreateStore<K, K2>(mappingStoreName, sourceKeyCodec, groupedKeyCodec)
		const mappingStoreRef = { store: mappingStore }

		// Use TableGroupByNode to properly handle retractions
		const groupByNode = new TableGroupByNode<K, V, K2>(fn, mappingStoreRef, sourceKeyCodec, groupedKeyCodec)
		this.node.connect(groupByNode as unknown as Processor<unknown, unknown>)

		return new KGroupedTableImpl<K2, V>(
			this.app,
			groupByNode as unknown as Processor<K2, V>,
			{ keyCodec: groupedKeyCodec, valueCodec },
			this.storeRef as { store: KeyValueStore<K, V> | null }
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
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// When other table updates, look up this table (reverse joiner args)
		const rightJoinNode = new TableTableJoinNode<K, V2, V, VR>(this.storeRef, (rightVal, leftVal) =>
			joiner(leftVal, rightVal)
		)
		otherTable.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

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
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// When other (right) table updates, look up this (left) table - inner join behavior
		// (only emit if left side exists)
		const rightJoinNode = new TableTableJoinNode<K, V2, V, VR>(this.storeRef, (rightVal, leftVal) =>
			joiner(leftVal, rightVal)
		)
		otherTable.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

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

		// When this (left) table updates, look up other (right) table - left join
		const leftJoinNode = new TableTableLeftJoinNode<K, V, V2, VR>(otherTable.storeRef, (leftVal, rightVal) =>
			joiner(leftVal, rightVal)
		)
		this.node.connect(leftJoinNode as unknown as Processor<unknown, unknown>)
		leftJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		// When other (right) table updates, look up this (left) table
		const rightJoinNode = new TableTableOuterJoinRightNode<K, V, V2, VR>(this.storeRef, joiner)
		otherTable.node.connect(rightJoinNode as unknown as Processor<unknown, unknown>)
		rightJoinNode.connect(mergeNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, VR>(this.app, mergeNode, { keyCodec: this.format.keyCodec })
	}
}

class KGroupedStreamImpl<K, V> implements KGroupedStream<K, V> {
	constructor(
		private readonly app: FlowAppImpl,
		private readonly node: Processor<K, V>,
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

		const storeName = options?.storeName ?? `count-store-${this.app['storeCounter']++}`
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

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, number>(this.app, aggregateNode as unknown as Processor<K, number>, {
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

		const storeName = options?.storeName ?? `reduce-store-${this.app['storeCounter']++}`
		const store = this.app.getOrCreateStore<K, V>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		// For reduce, we use the first value as the initial value.
		// The initializer returns undefined, which we detect in the aggregator
		// to know when this is the first value for a key. This survives restarts
		// because the store will return the persisted value (not undefined).
		const aggregateNode = new AggregateNode<K, V, V>(
			storeName,
			storeRef,
			() => undefined as unknown as V,
			(_key, value, aggregate) => {
				// If aggregate is undefined, this is the first value for this key
				if ((aggregate as unknown) === undefined) {
					return value
				}
				return reducer(aggregate, value)
			}
		)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, V>(this.app, aggregateNode as unknown as Processor<K, V>, { keyCodec, valueCodec })
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

		const storeName = options?.storeName ?? `aggregate-store-${this.app['storeCounter']++}`
		const store = this.app.getOrCreateStore<K, A>(
			storeName,
			keyCodec,
			valueCodec,
			options?.changelog,
			this.sourceTopics
		)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, A>(storeName, storeRef, initializer, aggregator)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, A>(this.app, aggregateNode as unknown as Processor<K, A>, { keyCodec, valueCodec })
	}

	windowedBy(windows: TimeWindows | SessionWindows | SlidingWindows): WindowedKGroupedStream<K, V> {
		return new WindowedKGroupedStreamImpl<K, V>(this.app, this.node, this.format, windows, this.sourceTopics)
	}
}

/**
 * Parse a WindowDuration to milliseconds.
 */
function parseWindowDuration(duration: WindowDuration): number {
	if (typeof duration === 'number') {
		return duration
	}
	const match = duration.match(/^(\d+)(ms|s|m|h|d)$/)
	if (!match) {
		throw new Error(`Invalid window duration: ${duration}`)
	}
	const value = parseInt(match[1]!, 10)
	const unit = match[2]
	switch (unit) {
		case 'ms':
			return value
		case 's':
			return value * 1000
		case 'm':
			return value * 60 * 1000
		case 'h':
			return value * 60 * 60 * 1000
		case 'd':
			return value * 24 * 60 * 60 * 1000
		default:
			throw new Error(`Unknown time unit: ${unit}`)
	}
}

/**
 * Processor node for windowed aggregations.
 */
class WindowedAggregateNode<K, V, A> extends Processor<K, V> {
	// Cleanup interval: run expiration every 60 seconds
	private static readonly CLEANUP_INTERVAL_MS = 60_000
	// Shared state for last cleanup time across clones
	private readonly cleanupState: { lastCleanupTime: number }

	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: WindowStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly windowSizeMs: number,
		cleanupState?: { lastCleanupTime: number }
	) {
		super()
		this.cleanupState = cleanupState ?? { lastCleanupTime: 0 }
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new WindowedAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.windowSizeMs,
			this.cleanupState // Share cleanup state across clones
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Window store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			return
		}

		// Calculate window for this record based on timestamp
		const timestamp = Number(record.timestamp)
		const windowStart = Math.floor(timestamp / this.windowSizeMs) * this.windowSizeMs
		const windowEnd = windowStart + this.windowSizeMs

		const windowedKey: import('./state.js').WindowedKey<K> = {
			key,
			windowStart,
			windowEnd,
		}

		// Get current aggregate or initialize
		const storedAggregate = await store.get(windowedKey)
		const aggregate: A = storedAggregate !== undefined ? storedAggregate : this.initializer()

		// Apply aggregation
		const newAggregate = this.aggregator(key, record.value, aggregate)

		// Store updated aggregate
		await store.put(windowedKey, newAggregate)

		// Periodically enforce retention by expiring old windows
		const now = Date.now()
		if (now - this.cleanupState.lastCleanupTime > WindowedAggregateNode.CLEANUP_INTERVAL_MS) {
			this.cleanupState.lastCleanupTime = now
			await store.expireOldWindows(now)
		}

		// Forward the windowed result
		const windowedResult: Windowed<K> = {
			key,
			window: { start: windowStart, end: windowEnd },
		}

		const next: StreamRecord<Windowed<K>, A> = {
			...record,
			key: windowedResult,
			value: newAggregate,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

/**
 * Processor node for session window aggregations.
 * Sessions are merged when events arrive within the inactivity gap.
 */
class SessionAggregateNode<K, V, A> extends Processor<K, V> {
	constructor(
		private readonly storeName: string,
		private readonly storeRef: { store: SessionStore<K, A> | null },
		private readonly initializer: () => A,
		private readonly aggregator: (key: K, value: V, aggregate: A) => A,
		private readonly merger: (aggregate1: A, aggregate2: A) => A,
		private readonly gapMs: number
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V> {
		void worker
		return new SessionAggregateNode<K, V, A>(
			this.storeName,
			this.storeRef,
			this.initializer,
			this.aggregator,
			this.merger,
			this.gapMs
		)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		const store = this.storeRef.store
		if (!store) {
			throw new Error(`Session store '${this.storeName}' not initialized`)
		}

		const key = record.key
		if (key === null) {
			return
		}

		const timestamp = Number(record.timestamp)

		// Find all sessions that could be merged with this event
		// A session overlaps if the new event's timestamp is within gap of the session's boundaries
		const searchFrom = timestamp - this.gapMs
		const searchTo = timestamp + this.gapMs

		const overlappingSessions: Array<{ windowedKey: WindowedKey<K>; value: A }> = []
		for await (const [windowedKey, value] of store.findSessions(key, searchFrom, searchTo)) {
			overlappingSessions.push({ windowedKey, value })
		}

		let mergedStart = timestamp
		let mergedEnd = timestamp
		let mergedValue = this.initializer()

		if (overlappingSessions.length > 0) {
			// Delete all overlapping sessions - they will be merged
			for (const { windowedKey } of overlappingSessions) {
				await store.delete(windowedKey)
				// Track merged boundaries
				mergedStart = Math.min(mergedStart, windowedKey.windowStart)
				mergedEnd = Math.max(mergedEnd, windowedKey.windowEnd)
			}

			// Merge all session values together
			for (const { value } of overlappingSessions) {
				mergedValue = this.merger(mergedValue, value)
			}
		}

		// Apply the new event's value
		mergedValue = this.aggregator(key, record.value, mergedValue)

		// Store the merged session
		const newSessionKey: WindowedKey<K> = {
			key,
			windowStart: mergedStart,
			windowEnd: mergedEnd,
		}
		await store.put(newSessionKey, mergedValue)

		// Forward the session result
		const windowedResult: Windowed<K> = {
			key,
			window: { start: mergedStart, end: mergedEnd },
		}

		const next: StreamRecord<Windowed<K>, A> = {
			...record,
			key: windowedResult,
			value: mergedValue,
		}
		await this.forward(next as StreamRecord<unknown, unknown>)
	}
}

class WindowedKGroupedStreamImpl<K, V> implements WindowedKGroupedStream<K, V> {
	private readonly windowSizeMs: number

	constructor(
		private readonly app: FlowAppImpl,
		private readonly node: Processor<K, V>,
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

		const storeName = options?.storeName ?? `window-count-store-${this.app['storeCounter']++}`

		let aggregateNode: Processor<K, V>

		if (this.windows instanceof SessionWindows) {
			// Use session store for session windows
			const sessionStore = this.app['stateStoreProvider'].createSessionStore<K, number>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
			})
			this.app['stateStores'].set(storeName, sessionStore as KeyValueStore<unknown, unknown>)
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
			const store = this.app['stateStoreProvider'].createWindowStore<K, number>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24, // Keep windows for 24x the window size
				windowSizeMs: this.windowSizeMs,
			})
			this.app['stateStores'].set(storeName, store as KeyValueStore<unknown, unknown>)
			const storeRef = { store }

			aggregateNode = new WindowedAggregateNode<K, V, number>(
				storeName,
				storeRef,
				() => 0,
				(_key, _value, aggregate) => aggregate + 1,
				this.windowSizeMs
			)
		}

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

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

		return new KTableImpl<Windowed<K>, number>(
			this.app,
			aggregateNode as unknown as Processor<Windowed<K>, number>,
			{ keyCodec: windowedKeyCodec, valueCodec }
		)
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

		const storeName = options?.storeName ?? `window-reduce-store-${this.app['storeCounter']++}`

		let aggregateNode: Processor<K, V>

		if (this.windows instanceof SessionWindows) {
			// Use session store for session windows
			const sessionStore = this.app['stateStoreProvider'].createSessionStore<K, V>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
			})
			this.app['stateStores'].set(storeName, sessionStore as KeyValueStore<unknown, unknown>)
			const storeRef = { store: sessionStore }

			aggregateNode = new SessionAggregateNode<K, V, V>(
				storeName,
				storeRef,
				() => undefined as unknown as V,
				(_key, value, aggregate) => {
					if ((aggregate as unknown) === undefined) {
						return value
					}
					return reducer(aggregate, value)
				},
				reducer, // merger uses the same reducer
				this.windowSizeMs
			)
		} else {
			// Use window store for time windows
			const store = this.app['stateStoreProvider'].createWindowStore<K, V>(storeName, {
				keyCodec,
				valueCodec,
				retentionMs: this.windowSizeMs * 24,
				windowSizeMs: this.windowSizeMs,
			})
			this.app['stateStores'].set(storeName, store as KeyValueStore<unknown, unknown>)
			const storeRef = { store }

			// For windowed reduce, WindowedAggregateNode handles window calculation
			// using record.timestamp. We detect first value by checking if aggregate
			// is undefined (from initializer). This survives restarts correctly.
			aggregateNode = new WindowedAggregateNode<K, V, V>(
				storeName,
				storeRef,
				() => undefined as unknown as V,
				(_key, value, aggregate) => {
					if ((aggregate as unknown) === undefined) {
						return value
					}
					return reducer(aggregate, value)
				},
				this.windowSizeMs
			)
		}

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

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

		return new KTableImpl<Windowed<K>, V>(this.app, aggregateNode as unknown as Processor<Windowed<K>, V>, {
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

		const storeName = options?.storeName ?? `window-aggregate-store-${this.app['storeCounter']++}`
		const store = this.app['stateStoreProvider'].createWindowStore<K, A>(storeName, {
			keyCodec,
			valueCodec,
			retentionMs: this.windowSizeMs * 24,
			windowSizeMs: this.windowSizeMs,
		})
		this.app['stateStores'].set(storeName, store as KeyValueStore<unknown, unknown>)
		const storeRef = { store }

		const aggregateNode = new WindowedAggregateNode<K, V, A>(
			storeName,
			storeRef,
			initializer,
			aggregator,
			this.windowSizeMs
		)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

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

		return new KTableImpl<Windowed<K>, A>(this.app, aggregateNode as unknown as Processor<Windowed<K>, A>, {
			keyCodec: windowedKeyCodec,
			valueCodec,
		})
	}
}

class KGroupedTableImpl<K, V> implements KGroupedTable<K, V> {
	constructor(
		private readonly app: FlowAppImpl,
		private readonly node: Processor<K, V>,
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

		const storeName = options?.storeName ?? `table-count-store-${this.app['storeCounter']++}`
		const store = this.app.getOrCreateStore<K, number>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, number>(
			storeName,
			storeRef,
			() => 0,
			(_key, _value, aggregate) => aggregate + 1
		)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, number>(
			this.app,
			aggregateNode as unknown as Processor<K, number>,
			{ keyCodec, valueCodec },
			storeRef
		)
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

		const storeName = options?.storeName ?? `table-reduce-store-${this.app['storeCounter']++}`
		const store = this.app.getOrCreateStore<K, V>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		// For reduce, the first value becomes the initial aggregate.
		// We detect first value by checking if aggregate is undefined.
		const aggregateNode = new AggregateNode<K, V, V>(
			storeName,
			storeRef,
			() => undefined as unknown as V,
			(_key, value, aggregate) => {
				if ((aggregate as unknown) === undefined) {
					return value
				}
				return reducer(aggregate, value)
			}
		)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, V>(
			this.app,
			aggregateNode as unknown as Processor<K, V>,
			{ keyCodec, valueCodec },
			storeRef
		)
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

		const storeName = options?.storeName ?? `table-aggregate-store-${this.app['storeCounter']++}`
		const store = this.app.getOrCreateStore<K, A>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const aggregateNode = new AggregateNode<K, V, A>(storeName, storeRef, initializer, aggregator)

		this.node.connect(aggregateNode as unknown as Processor<unknown, unknown>)

		return new KTableImpl<K, A>(
			this.app,
			aggregateNode as unknown as Processor<K, A>,
			{ keyCodec, valueCodec },
			storeRef
		)
	}
}

function ensureStream<K, V>(stream: KStream<K, V>, app: FlowAppImpl): KStreamImpl<K, V> {
	if (stream instanceof KStreamImpl) {
		if (stream.app !== app) {
			throw new Error('Cannot merge streams from different Flow apps')
		}
		return stream as KStreamImpl<K, V>
	}
	throw new Error('Unsupported stream implementation')
}

export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}
