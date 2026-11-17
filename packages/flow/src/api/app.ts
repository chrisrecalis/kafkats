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

/**
 * A node of a described topology (Kafka Streams' `TopologyDescription.Node`). Neighbours are listed by name in Kafka
 * Streams' order: nodes with more descendants first, then by name.
 */
export interface TopologyNodeDescription {
	readonly name: string
	readonly kind: 'source' | 'processor' | 'sink'
	/**
	 * Sources: the topics read; sinks: the topic written, none when a TopicNameExtractor picks it per record. Internal
	 * topics carry their full names (`<applicationId>-<name>-repartition`).
	 */
	readonly topics: readonly string[]
	/** Processors: the state stores connected to them (global stores are readable without being connected). */
	readonly stores: readonly string[]
	/** The parent nodes; none for sources. */
	readonly predecessors: readonly string[]
	/** The child nodes; none for sinks. */
	readonly successors: readonly string[]
}

/** A connected part of the topology; it runs as one task per input partition, with the task ids `<id>_<partition>`. */
export interface SubtopologyDescription {
	readonly id: number
	/** Kafka Streams' order: nodes with more descendants first (so a node precedes its children), then by name. */
	readonly nodes: readonly TopologyNodeDescription[]
}

/** A global store: the source of its topic and the processor that writes the store. It generates no tasks. */
export interface GlobalStoreDescription {
	/** Numbered after the subtopologies, so that the ids of subtopologies are those of their tasks. */
	readonly id: number
	readonly source: TopologyNodeDescription
	readonly processor: TopologyNodeDescription
}

/**
 * The topology an app runs (Kafka Streams' `TopologyDescription`), in plain data: `JSON.stringify` keeps all of it.
 * `toString()` prints it in Kafka Streams' format.
 */
export interface TopologyDescription {
	/** By id. */
	readonly subtopologies: readonly SubtopologyDescription[]
	/** By id. */
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
	/**
	 * Registers a store made by `Stores` (Kafka Streams' `addStateStore()`), so that processors connect to it by name in
	 * `process(supplier, { stores: [name] })`. Naming the definition itself in `stores` registers it too; registering
	 * it again has no effect. Another store of the same name is a TopologyError.
	 */
	addStateStore(definition: StoreDefinition<StateStore>): this
	/**
	 * Builds the topology and describes it. Building freezes it, as start() does: later DSL calls throw TopologyError,
	 * and an invalid topology throws its TopologyError here.
	 */
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
