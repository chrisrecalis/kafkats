import type { ConsumerConfig, KafkaClient, KafkaClientConfig, ProducerConfig } from '@kafkats/client'
import type {
	DeserializationExceptionHandler,
	HandlerResponse,
	ProcessingExceptionHandler,
	ProductionExceptionHandler,
	UncaughtExceptionHandler,
	UncaughtExceptionResponse,
} from '@/api/handlers.js'
import type { KStream } from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type { Consumed, Materialized } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import type { ChangelogRestorationOptions } from '@/changelog/config.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { StateStore, StateStoreProvider } from '@/state.js'
import type { Topic } from '@/topic.js'

/** RUNNING only when every assigned task is restored and running; REBALANCING during rebalances, restores and recovery. */
export type StreamState = 'CREATED' | 'RUNNING' | 'REBALANCING' | 'ERROR' | 'STOPPED'

export interface FlowConfig {
	applicationId: string
	client: KafkaClient | KafkaClientConfig
	/** Default 'at_least_once'. */
	processingGuarantee?: 'at_least_once' | 'exactly_once'
	/** How often offsets (and under exactly_once the transaction) are committed. Default 100 under exactly_once, 5000 under at_least_once. */
	commitIntervalMs?: number
	/** at_least_once: how long commits may keep failing with timeouts before the app rejoins the group. Default 300000. */
	taskTimeoutMs?: number
	/** Default inMemory(). */
	stateStoreProvider?: StateStoreProvider
	/**
	 * The group id is the application id. `partitionAssignmentStrategy` must be 'range' if given; `maxWaitMs`
	 * defaults to 500; `autoOffsetReset` is the default reset policy of the sources (default 'earliest').
	 */
	consumer?: Omit<
		ConsumerConfig,
		'groupId' | 'onBeforeRebalance' | 'onPartitionsAssigned' | 'onPartitionsRevoked' | 'onPartitionsLost'
	>
	/**
	 * exactly_once: `transactionalId` defaults to `<applicationId>-<processId>`, `retries` to 10 and
	 * `transactionTimeoutMs` to 10000 (it must not be below commitIntervalMs).
	 */
	producer?: ProducerConfig
	/** How long start() waits for the first assignment. Default 120000. */
	startTimeoutMs?: number
	/** How long a task waits for data on all its input partitions before processing out of order. Default 0; -1 never waits. */
	maxTaskIdleMs?: number
	/** Records buffered per partition before it is paused. Default 1000. */
	bufferedRecordsPerPartition?: number
	/** How often consumed records of repartition topics are deleted. Default 30000; 0 disables purging. */
	repartitionPurgeIntervalMs?: number
	/** Default: the record timestamp; an invalid (negative) timestamp stops the app. */
	defaultTimestampExtractor?: TimestampExtractor
	/** Default 'fail'. */
	deserializationExceptionHandler?: HandlerResponse | DeserializationExceptionHandler
	/** Default 'fail'. */
	processingExceptionHandler?: HandlerResponse | ProcessingExceptionHandler
	/** Default 'fail'. */
	productionExceptionHandler?: HandlerResponse | ProductionExceptionHandler
	/** What a fatal error does. Default 'shutdown' (state ERROR). */
	uncaughtExceptionHandler?: UncaughtExceptionResponse | UncaughtExceptionHandler
	changelog?: {
		/** Default replication factor of changelog and repartition topics. */
		replicationFactor?: number
		/** Applied to every changelog and repartition topic, below per-store configs. */
		topicConfigs?: Record<string, string>
		/** How changelogs are restored. */
		restoration?: ChangelogRestorationOptions
		/** Whether internal topics are created; false only validates them. Default true. */
		autoCreate?: boolean
	}
}

export interface CloseOptions {
	/** close() returns within this time even when the cluster is unreachable. Default 20000. */
	timeoutMs?: number
}

/** A node of a described topology. */
export interface TopologyNodeDescription {
	readonly name: string
	readonly kind: 'source' | 'processor' | 'sink'
	/** Sources: the topics read; sinks: the topic written (empty for a TopicNameExtractor). */
	readonly topics: readonly string[]
	/** Processors: the stores connected to them. */
	readonly stores: readonly string[]
	readonly predecessors: readonly string[]
	readonly successors: readonly string[]
}

export interface SubtopologyDescription {
	readonly id: number
	readonly nodes: readonly TopologyNodeDescription[]
}

export interface GlobalStoreDescription {
	readonly id: number
	readonly source: TopologyNodeDescription
	readonly processor: TopologyNodeDescription
}

/** The built topology; `toString()` prints it in Kafka Streams' format. */
export interface TopologyDescription {
	readonly subtopologies: readonly SubtopologyDescription[]
	readonly globalStores: readonly GlobalStoreDescription[]
	toString(): string
}

export interface FlowApp {
	stream<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: Consumed<K, V>): KStream<K, V>
	table<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V>
	/**
	 * A table every instance reads in full: loaded before processing starts, then followed, in a store without a
	 * changelog (the topic is one). Only usable as the lookup side of `KStream.join/leftJoin`.
	 */
	globalTable<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): GlobalKTable<K, V>
	/** Registers a Processor API store; processors connect to it through `process(..., { stores })`. */
	addStateStore(definition: StoreDefinition<StateStore>): this
	describe(): TopologyDescription
	start(): Promise<void>
	close(options?: CloseOptions): Promise<void>
	state(): StreamState
	/**
	 * The fatal error while ERROR (the first one; kept until close()); otherwise the most recent recoverable
	 * error, which never changes state(). Cleared when a new start begins. Use state() for health.
	 */
	getError(): Error | null
}
