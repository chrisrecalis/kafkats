import type {
	AutoOffsetReset,
	KafkaClient,
	KafkaClientConfig,
	ConsumerConfig,
	ProducerConfig,
	RunBatchOptions,
} from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { StateStoreProvider } from '@/state.js'
import type { ChangelogConfig, ChangelogRestorationOptions } from '@/legacy/changelog.js'
import type { Topic } from '@/topic.js'

// Re-import window classes for type references in Joined
import type { TimeWindows, SessionWindows, SlidingWindows } from '@/legacy/windows.js'

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
	 *
	 * Under at_least_once it is how often the progress within a consumed batch becomes
	 * committable (the offsets themselves are committed by the consumer's auto-commit).
	 */
	commitIntervalMs?: number
	stateDir?: string
	stateStoreProvider?: StateStoreProvider
	/** `autoOffsetReset` is one policy for every source topic (a per-partition policy function is not supported). */
	consumer?: Omit<ConsumerConfig, 'groupId' | 'autoOffsetReset'> & { autoOffsetReset?: AutoOffsetReset }
	producer?: ProducerConfig
	/**
	 * How each worker's consumer runs: partition concurrency and offset auto-commit. Under
	 * exactly_once, offsets are committed by the transaction and partitions are processed one at a time.
	 */
	consumerRun?: RunBatchOptions
	/**
	 * Whether a stream worker that fails with a recoverable error (broker outage, failed commit,
	 * lost session, ...) restarts itself and resumes from the last committed offsets.
	 * Errors thrown by user code and fatal errors (fencing, authorization) always stop the worker
	 * and put the app in the ERROR state.
	 * Default: true
	 */
	restartOnError?: boolean
	/**
	 * How long start() waits for each stream worker to join the consumer group.
	 * Joining can take as long as the session timeout of a crashed member the group is still
	 * waiting for.
	 * Default: 120000
	 */
	startTimeoutMs?: number
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
	partitioner?: (key: K | null, value: V | null, partitionCount: number) => number
}

export interface Grouped<K, V> {
	/** Name used for the internal repartition topic (`<applicationId>-<name>-repartition`). */
	name?: string
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
	/** Changelog backing for the join window stores. Enabled by default. */
	changelog?: boolean | ChangelogConfig
}

export interface Windowed<K> {
	key: K
	window: { start: number; end: number }
}

export type KeyValue<K, V> = readonly [K, V]

export type { Topic }

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
	/** The most recent error observed by the app (null if none). */
	getError(): Error | null
}

export interface KStream<K, V> {
	map<K2, V2>(fn: (key: K | null, value: V | null) => KeyValue<K2, V2>): KStream<K2, V2>
	mapValues<V2>(fn: (value: V | null) => V2 | null): KStream<K, V2>
	flatMapValues<V2>(fn: (value: V | null) => Iterable<V2>): KStream<K, V2>
	filter(fn: (key: K | null, value: V | null) => boolean): KStream<K, V>
	peek(fn: (key: K | null, value: V | null) => void): KStream<K, V>
	selectKey<K2>(fn: (value: V | null, key: K | null) => K2): KStream<K2, V>
	merge(other: KStream<K, V>): KStream<K, V>
	branch(...predicates: Array<(key: K | null, value: V | null) => boolean>): KStream<K, V>[]
	to(topic: string, options?: Produced<K, V>): void
	through(topic: string, options?: Produced<K, V>): KStream<K, V>
	toTable(options?: Materialized<K, V>): KTable<K, V>
	groupBy<K2>(fn: (key: K | null, value: V | null) => K2, options?: Grouped<K2, V>): KGroupedStream<K2, V>
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
	mapValues<V2>(fn: (value: V | null) => V2 | null): KTable<K, V2>
	filter(fn: (key: K | null, value: V | null) => boolean): KTable<K, V>
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
	reduce(
		adder: (aggregate: V, value: V) => V,
		subtractor: (aggregate: V, value: V) => V,
		options?: Materialized<K, V>
	): KTable<K, V>
	aggregate<A>(
		initializer: () => A,
		adder: (key: K, value: V, aggregate: A) => A,
		subtractor: (key: K, value: V, aggregate: A) => A,
		options?: Materialized<K, A>
	): KTable<K, A>
}

export interface WindowedKGroupedStream<K, V> {
	count(options?: Materialized<Windowed<K>, number>): KTable<Windowed<K>, number>
	reduce(reducer: (aggregate: V, value: V) => V, options?: Materialized<Windowed<K>, V>): KTable<Windowed<K>, V>
	aggregate<A>(
		initializer: () => A,
		aggregator: (key: K, value: V, aggregate: A) => A,
		/** `merger` is required for session windows: it combines the aggregates of two sessions that merge. */
		options?: Materialized<Windowed<K>, A> & { merger?: (aggregate1: A, aggregate2: A) => A }
	): KTable<Windowed<K>, A>
}
