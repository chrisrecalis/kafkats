import { randomUUID } from 'node:crypto'
import {
	KafkaClient,
	KafkaError,
	ErrorCode,
	type Consumer,
	type Message,
	type ConsumeContext,
	type Producer,
	type ProducerConfig,
	type RunEachOptions,
	type Logger,
} from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { buffer as bufferCodec } from '@/codec.js'
import type {
	StateStoreProvider,
	KeyValueStore,
	WindowStore,
	SessionStore,
	WindowedKey,
	ChangelogCheckpointStore,
} from '@/state.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import {
	type ChangelogConfig,
	type ChangelogTopicSpec,
	ChangelogWriter,
	ChangelogRestorer,
	ChangelogPartitionMismatchError,
	CoPartitioningError,
	SourceTopicNotFoundError,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
	windowedKeyCodec,
} from '@/changelog.js'
import {
	ChangelogBackedKeyValueStore,
	ChangelogBackedWindowStore,
	ChangelogBackedSessionStore,
} from '@/state/changelog.js'
import { BufferedKeyValueStore, BufferedSessionStore, BufferedWindowStore } from '@/state/buffered.js'
import {
	PartitionedKeyValueStore,
	PartitionedSessionStore,
	PartitionedWindowStore,
	StorePartitions,
	clearStore,
	partitionStoreName,
} from '@/state/partitioned.js'
import { recordContextStorage, type RecordContext } from '@/runtime/context.js'

// Public types
import type {
	StreamState,
	FlowConfig,
	FlowApp,
	KStream,
	KTable,
	Topic,
	Consumed,
	Produced,
	Materialized,
} from '@/types.js'

// Re-export public types for backward compatibility
export * from '@/types.js'
export * from '@/windows.js'
export { topic } from '@/topic.js'

// Internal processor types
import {
	SourceNode,
	type StreamFormat,
	type StreamRecord,
	type WorkerContext,
	type ActiveTransaction,
	type TopicPartitionKey,
	type AnyProcessor,
} from '@/processors/index.js'
import { TableStateNode } from '@/processors/table.js'

// Stream implementations
import { KStreamImpl } from '@/streams/kstream.js'
import { KTableImpl } from '@/streams/ktable.js'

const DEFAULT_BUFFER_CODEC: Codec<Buffer> = bufferCodec()

/**
 * Tracks the record currently being processed (worker + source partition).
 * Used by state stores and changelog writers to pick the right state partition and transaction.
 */
export const workerContextStorage = recordContextStorage

/** Default transaction commit interval for exactly_once mode (100ms) */
const DEFAULT_COMMIT_INTERVAL_MS = 100

const WORKER_START_TIMEOUT_MS = 10_000
const RESTART_INITIAL_BACKOFF_MS = 500
const RESTART_MAX_BACKOFF_MS = 30_000
/** How long after a revocation unowned in-memory state partitions are kept before being dropped. */
const UNOWNED_STATE_PRUNE_DELAY_MS = 30_000

type TopicPartition = { topic: string; partition: number }

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyStore = KeyValueStore<any, any>

/** A logical state store and how its state is laid out. */
type StoreDef = {
	name: string
	/**
	 * 'partition': one slice of state per source partition, restored from the changelog partition
	 * of the same index (the default; safe across instances).
	 * 'app': a single app-wide store (legacy layout, only for stores whose keys are not
	 * co-partitioned with their input).
	 */
	scope: 'partition' | 'app'
	partitions: StorePartitions<AnyStore, AnyStore>
	changelog: ChangelogTopicSpec | null
	sourceTopics: Set<string>
}

type RepartitionTopicSpec = {
	topicName: string
	/** Topics whose records are written into this repartition topic. */
	upstreamTopics: Set<string>
}

type GlobalTableDef = {
	topic: string
	storeName: string
	store: KeyValueStore<unknown, unknown>
	keyCodec: Codec<unknown>
	valueCodec: Codec<unknown>
	source: SourceNode<unknown, unknown>
	consumer: Consumer | null
	run: Promise<void> | null
}

/** Error codes that restarting a worker cannot fix. */
const FATAL_ERROR_CODES = new Set<number>([
	ErrorCode.ProducerFenced,
	ErrorCode.InvalidProducerEpoch,
	ErrorCode.FencedInstanceId,
	ErrorCode.TopicAuthorizationFailed,
	ErrorCode.GroupAuthorizationFailed,
	ErrorCode.ClusterAuthorizationFailed,
	ErrorCode.TransactionalIdAuthorizationFailed,
	ErrorCode.SaslAuthenticationFailed,
	ErrorCode.UnsupportedVersion,
	ErrorCode.InvalidSessionTimeout,
	ErrorCode.InconsistentGroupProtocol,
	ErrorCode.InvalidConfig,
	ErrorCode.MessageTooLarge,
])

function isFatalKafkaError(error: unknown): boolean {
	return error instanceof KafkaError && FATAL_ERROR_CODES.has(error.errorCode)
}

/**
 * Whether a worker can recover from an error raised while running the topology by restarting
 * (rejoining the group and resuming from the last committed offsets). Only Kafka errors qualify:
 * an error thrown by user code would fail the same way on the same record again.
 */
function isRecoverable(error: unknown): boolean {
	return error instanceof KafkaError && !isFatalKafkaError(error)
}

/**
 * Whether a worker can recover from an infrastructure failure (commit, rebalance, fetch, restore).
 * No user code is involved, so anything except a known-fatal Kafka error is worth a restart.
 */
function isInfrastructureRecoverable(error: unknown): boolean {
	return !isFatalKafkaError(error)
}

function toError(err: unknown): Error {
	return err instanceof Error ? err : new Error(String(err))
}

class FlowAppImpl implements FlowApp {
	private readonly client: KafkaClient
	private readonly ownsClient: boolean
	/** Internal instance identity used to keep derived transactional IDs unique across replicas. */
	private readonly processId = randomUUID()
	private readonly sourcesByTopic = new Map<string, SourceNode<unknown, unknown>[]>()
	private readonly offsetResetByTopic = new Map<string, 'earliest' | 'latest' | 'none'>()
	private readonly partitionCounts = new Map<string, number>()
	private readonly eosEnabled: boolean
	private readonly commitIntervalMs: number
	readonly stateStoreProvider: StateStoreProvider
	/**
	 * Where changelog restore progress is tracked. Providers with durable state supply a durable
	 * store; otherwise progress is tracked in-process, which is enough to catch a retained partition
	 * up after a rebalance instead of rebuilding it from the start of the changelog.
	 */
	private readonly changelogCheckpointStore: ChangelogCheckpointStore
	private readonly durableCheckpoints: boolean
	private readonly processCheckpoints = new Map<string, bigint>()
	private pruneTimer: ReturnType<typeof setTimeout> | null = null
	readonly stateStores = new Map<string, KeyValueStore<unknown, unknown>>()
	private readonly storeDefs = new Map<string, StoreDef>()
	private readonly repartitionTopics = new Map<string, RepartitionTopicSpec>()
	private readonly copartitionGroups: Array<Set<string>> = []
	private readonly globalTables: GlobalTableDef[] = []
	private readonly logger: Logger
	private lastCheckpointErrorMessage: string | null = null
	private currentState: StreamState = 'CREATED'
	private closing = false
	private started = false
	private startPromise: Promise<void> | null = null
	private closePromise: Promise<void> | null = null
	private shutdownController = new AbortController()
	private workers: WorkerContext[] = []
	private runPromises: Promise<void>[] = []
	private lastError: Error | null = null
	private storeCounter = 0
	private repartitionCounter = 0
	/** True once the cluster was reachable at startup (false only with mocked/offline clients). */
	private clusterAvailable = false
	private startupError: Error | null = null
	private restorationQueue: Promise<void> = Promise.resolve()

	/** Get the next store ID and increment the counter */
	nextStoreId(): number {
		return this.storeCounter++
	}

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
		const durable = this.stateStoreProvider.getChangelogCheckpointStore?.()
		this.durableCheckpoints = durable !== undefined
		const inProcess = this.processCheckpoints
		this.changelogCheckpointStore = durable ?? {
			get: (topic, partition) => Promise.resolve(inProcess.get(`${topic}:${partition}`)),
			set: (topic, partition, offset) => {
				inProcess.set(`${topic}:${partition}`, offset)
				return Promise.resolve()
			},
		}
		this.logger = this.client.cluster
			.getLogger()
			.child({ component: 'flow', applicationId: config.applicationId, processId: this.processId })
	}

	// ------------------------------------------------------------------------------------------
	// Topology building
	// ------------------------------------------------------------------------------------------

	/** A store is partition-scoped when its keys are co-partitioned with known source topics. */
	private resolveScope(sourceTopics: Set<string> | undefined, copartitioned: boolean): 'partition' | 'app' {
		return copartitioned && sourceTopics !== undefined && sourceTopics.size > 0 ? 'partition' : 'app'
	}

	private registerStore(def: StoreDef, facade: KeyValueStore<unknown, unknown>): void {
		this.storeDefs.set(def.name, def)
		this.stateStores.set(def.name, facade)
		if (def.scope === 'partition') {
			this.copartitionGroups.push(def.sourceTopics)
		}
	}

	getOrCreateStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): KeyValueStore<K, V> {
		const storeName = name ?? `store-${this.nextStoreId()}`
		const existing = this.stateStores.get(storeName)
		if (existing) {
			return existing as KeyValueStore<K, V>
		}

		const scope = this.resolveScope(sourceTopics, restrictRestorationToSourcePartitions)
		const changelogConfig = resolveChangelogConfig(changelog)
		const writer = changelogConfig.enabled
			? this.setupChangelog<K, V>(
					storeName,
					keyCodec,
					valueCodec,
					changelogConfig,
					sourceTopics ?? new Set(),
					scope
				)
			: null

		const partitions = new StorePartitions<KeyValueStore<K, V>, KeyValueStore<K, V>>(
			storeName,
			partition => {
				const raw = this.stateStoreProvider.createKeyValueStore<K, V>(
					scope === 'partition' ? partitionStoreName(storeName, partition) : storeName,
					{ keyCodec, valueCodec }
				)
				const buffered = new BufferedKeyValueStore<K, V>(raw, keyCodec, valueCodec)
				const store = writer ? new ChangelogBackedKeyValueStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			scope === 'app'
		)
		const facade = new PartitionedKeyValueStore<K, V>(partitions)
		this.registerStore(
			{
				name: storeName,
				scope,
				partitions: partitions as unknown as StorePartitions<AnyStore, AnyStore>,
				changelog: writer ? this.changelogSpecs.get(storeName)! : null,
				sourceTopics: sourceTopics ?? new Set(),
			},
			facade as KeyValueStore<unknown, unknown>
		)
		return facade
	}

	private readonly changelogSpecs = new Map<string, ChangelogTopicSpec>()

	/**
	 * Register a changelog topic spec and writer for a store.
	 *
	 * `changelogKeyCodec` is the codec used to (de)serialize changelog record keys during
	 * restoration. For windowed/session stores it is `windowedKeyCodec(userKeyCodec)` so the
	 * WindowedKey round-trips through the changelog.
	 */
	private setupChangelog<KK, VV>(
		storeName: string,
		changelogKeyCodec: Codec<KK>,
		valueCodec: Codec<VV>,
		changelogConfig: ChangelogConfig,
		sourceTopics: Set<string>,
		scope: 'partition' | 'app',
		windowRetentionMs?: number
	): ChangelogWriter<KK, VV> {
		const topicName = changelogConfig.topicName ?? buildChangelogTopicName(this.config.applicationId, storeName)

		// Window/session changelogs use delete+compact with a finite retention tied to the store's
		// retention, so the broker prunes records for expired windows. Without this the (KV) default
		// of compact + infinite retention would keep every window forever and resurrect expired
		// windows on restore. User-supplied topicConfigs still take precedence.
		const windowConfigOverrides: Record<string, string> =
			windowRetentionMs !== undefined
				? { 'cleanup.policy': 'delete,compact', 'retention.ms': String(windowRetentionMs) }
				: {}

		const spec: ChangelogTopicSpec = {
			storeName,
			topicName,
			replicationFactor: changelogConfig.replicationFactor,
			configs: getDefaultTopicConfigs({ ...windowConfigOverrides, ...changelogConfig.topicConfigs }),
			keyCodec: changelogKeyCodec as Codec<unknown>,
			valueCodec: valueCodec as Codec<unknown>,
			skipRestoration: changelogConfig.skipRestoration ?? false,
			sourceTopics,
			restrictRestorationToSourcePartitions: scope === 'partition',
			validateOnly: this.config.changelog?.autoCreate === false,
		}
		this.changelogSpecs.set(storeName, spec)

		return new ChangelogWriter<KK, VV>(
			topicName,
			changelogKeyCodec,
			valueCodec,
			() => {
				const ctx = recordContextStorage.getStore()
				if (!ctx) {
					throw new Error('Changelog write called outside of message processing context')
				}
				return ctx.worker.producer
			},
			() => recordContextStorage.getStore()?.worker.activeTransaction ?? null,
			async result => {
				// Mocked producers (tests) don't report offsets; nothing to checkpoint then.
				if (!result || typeof result.offset !== 'bigint') return
				const worker = recordContextStorage.getStore()?.worker
				if (worker?.activeTransaction) {
					// Checkpoint only once the transaction has committed (see commitTransactionBatch).
					const key: TopicPartitionKey = `${result.topic}:${result.partition}`
					const nextOffset = result.offset + 1n
					const existing = worker.pendingChangelogOffsets.get(key)
					if (!existing || nextOffset > existing.offset) {
						worker.pendingChangelogOffsets.set(key, {
							topic: result.topic,
							partition: result.partition,
							offset: nextOffset,
						})
					}
					return
				}

				try {
					await this.changelogCheckpointStore.set(result.topic, result.partition, result.offset + 1n)
				} catch (err) {
					this.recordCheckpointError(err, { topic: result.topic, partition: result.partition })
				}
			},
			// State partition N is logged to changelog partition N (see ChangelogWriter).
			scope === 'partition' ? () => recordContextStorage.getStore()?.partition : undefined
		)
	}

	getOrCreateWindowStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		windowOptions: { retentionMs: number; windowSizeMs: number },
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): WindowStore<K, V> {
		const storeName = name ?? `window-store-${this.nextStoreId()}`
		const existing = this.stateStores.get(storeName)
		if (existing) {
			return existing as unknown as WindowStore<K, V>
		}

		const scope = this.resolveScope(sourceTopics, restrictRestorationToSourcePartitions)
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		const changelogConfig = resolveChangelogConfig(changelog)
		const writer = changelogConfig.enabled
			? this.setupChangelog<WindowedKey<K>, V>(
					storeName,
					storeKeyCodec,
					valueCodec,
					changelogConfig,
					sourceTopics ?? new Set(),
					scope,
					windowOptions.retentionMs
				)
			: null

		const partitions = new StorePartitions<WindowStore<K, V>, WindowStore<K, V>>(
			storeName,
			partition => {
				const raw = this.stateStoreProvider.createWindowStore<K, V>(
					scope === 'partition' ? partitionStoreName(storeName, partition) : storeName,
					{
						keyCodec,
						valueCodec,
						retentionMs: windowOptions.retentionMs,
						windowSizeMs: windowOptions.windowSizeMs,
					}
				)
				const buffered = new BufferedWindowStore<K, V>(
					raw,
					storeKeyCodec,
					valueCodec,
					keyCodec,
					windowOptions.retentionMs
				)
				const store = writer ? new ChangelogBackedWindowStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			scope === 'app'
		)
		const facade = new PartitionedWindowStore<K, V>(partitions)
		this.registerStore(
			{
				name: storeName,
				scope,
				partitions: partitions as unknown as StorePartitions<AnyStore, AnyStore>,
				changelog: writer ? this.changelogSpecs.get(storeName)! : null,
				sourceTopics: sourceTopics ?? new Set(),
			},
			facade as unknown as KeyValueStore<unknown, unknown>
		)
		return facade
	}

	getOrCreateSessionStore<K, V>(
		name: string | undefined,
		keyCodec: Codec<K>,
		valueCodec: Codec<V>,
		sessionOptions: { retentionMs: number },
		changelog?: boolean | ChangelogConfig,
		sourceTopics?: Set<string>,
		restrictRestorationToSourcePartitions = true
	): SessionStore<K, V> {
		const storeName = name ?? `session-store-${this.nextStoreId()}`
		const existing = this.stateStores.get(storeName)
		if (existing) {
			return existing as unknown as SessionStore<K, V>
		}

		const scope = this.resolveScope(sourceTopics, restrictRestorationToSourcePartitions)
		const storeKeyCodec = windowedKeyCodec(keyCodec)
		const changelogConfig = resolveChangelogConfig(changelog)
		const writer = changelogConfig.enabled
			? this.setupChangelog<WindowedKey<K>, V>(
					storeName,
					storeKeyCodec,
					valueCodec,
					changelogConfig,
					sourceTopics ?? new Set(),
					scope,
					sessionOptions.retentionMs
				)
			: null

		const partitions = new StorePartitions<SessionStore<K, V>, SessionStore<K, V>>(
			storeName,
			partition => {
				const raw = this.stateStoreProvider.createSessionStore<K, V>(
					scope === 'partition' ? partitionStoreName(storeName, partition) : storeName,
					{ keyCodec, valueCodec, retentionMs: sessionOptions.retentionMs }
				)
				const buffered = new BufferedSessionStore<K, V>(
					raw,
					storeKeyCodec,
					valueCodec,
					keyCodec,
					sessionOptions.retentionMs
				)
				const store = writer ? new ChangelogBackedSessionStore<K, V>(buffered, writer) : buffered
				return { store, raw, buffer: buffered }
			},
			scope === 'app'
		)
		const facade = new PartitionedSessionStore<K, V>(partitions)
		this.registerStore(
			{
				name: storeName,
				scope,
				partitions: partitions as unknown as StorePartitions<AnyStore, AnyStore>,
				changelog: writer ? this.changelogSpecs.get(storeName)! : null,
				sourceTopics: sourceTopics ?? new Set(),
			},
			facade as unknown as KeyValueStore<unknown, unknown>
		)
		return facade
	}

	/**
	 * Declare that the given topics must be co-partitioned (same partition count), because records
	 * with the same key from each of them have to meet in the same state partition (joins, merges).
	 */
	requireCopartitioned(topics: Set<string>): void {
		if (topics.size > 1) {
			this.copartitionGroups.push(topics)
		}
	}

	/**
	 * Register an internal repartition topic fed by the given upstream topics and return its name.
	 * The topic is created at startup with the upstream partition count.
	 */
	createRepartitionTopic(name: string | undefined, upstreamTopics: Set<string>): string {
		const topicName = `${this.config.applicationId}-${name ?? `repartition-${this.repartitionCounter++}`}-repartition`
		if (!this.repartitionTopics.has(topicName)) {
			this.repartitionTopics.set(topicName, { topicName, upstreamTopics: new Set(upstreamTopics) })
		}
		return topicName
	}

	private readonly throughTopics = new Set<string>()

	registerThroughTopic(topic: string): void {
		this.throughTopics.add(topic)
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

		const storeName = options?.materialized?.storeName ?? `table-${resolved.topic}-${this.nextStoreId()}`
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
		sourceNode.connect(tableStateNode)

		return new KTableImpl<K, V>(this, tableStateNode, resolved.format, storeRef, sourceTopics)
	}

	/**
	 * A table that is fully replicated to every application instance.
	 *
	 * Unlike table(), a global table is not read through the consumer group: every instance reads
	 * all partitions of the topic itself (from the beginning, before processing starts, and
	 * continuously afterwards). Streams can therefore join against it regardless of how either
	 * topic is partitioned. The topic itself is the source of truth, so no changelog is written.
	 */
	globalTable<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V> {
		const resolved = this.resolveSource(source, options)
		const sourceNode = new SourceNode<K, V>(resolved.topic, resolved.format)

		const keyCodec = options?.materialized?.key ?? resolved.format.keyCodec
		const valueCodec = options?.materialized?.value ?? resolved.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('globalTable() requires both key and value codecs for stateful operations')
		}

		const storeName = options?.materialized?.storeName ?? `global-table-${resolved.topic}-${this.nextStoreId()}`
		const store = this.stateStoreProvider.createKeyValueStore<K, V>(storeName, { keyCodec, valueCodec })
		this.stateStores.set(storeName, store as KeyValueStore<unknown, unknown>)
		const storeRef = { store }

		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		sourceNode.connect(tableStateNode)

		this.globalTables.push({
			topic: resolved.topic,
			storeName,
			store: store as KeyValueStore<unknown, unknown>,
			keyCodec: resolved.format.keyCodec as Codec<unknown>,
			valueCodec: resolved.format.valueCodec as Codec<unknown>,
			source: sourceNode as SourceNode<unknown, unknown>,
			consumer: null,
			run: null,
		})

		return new KTableImpl<K, V>(this, tableStateNode, resolved.format, storeRef, new Set(), true)
	}

	// ------------------------------------------------------------------------------------------
	// Lifecycle
	// ------------------------------------------------------------------------------------------

	start(): Promise<void> {
		if (this.startPromise) {
			return this.startPromise
		}
		const promise = this.doStart()
		this.startPromise = promise
		promise.catch(() => {
			// A failed start can be retried.
			if (this.startPromise === promise) this.startPromise = null
		})
		return promise
	}

	private async doStart(): Promise<void> {
		this.closing = false
		this.started = false
		this.currentState = 'CREATED'
		this.startupError = null
		this.lastError = null
		this.shutdownController = new AbortController()
		this.closePromise = null

		try {
			await this.client.connect()
			this.assertNotClosing()

			// Validate topology against the cluster and create internal topics
			// (returns false if skipped because no cluster is reachable, e.g. mocked clients).
			this.clusterAvailable = await this.validateAndCreateInternalTopics()
			this.assertNotClosing()

			if (this.clusterAvailable) {
				// App-scoped stores and global tables must be complete before any record is processed.
				await this.restoreAppScopedStores()
				this.assertNotClosing()
				await Promise.all(this.globalTables.map(table => this.startGlobalTable(table)))
				this.assertNotClosing()
			}

			const topics = this.subscribedTopics()
			if (topics.length > 0) {
				await this.startWorkers(topics)
			}

			this.assertNotClosing()
			this.started = true
			if ((this.currentState as StreamState) !== 'ERROR') {
				this.currentState = 'RUNNING'
			}
		} catch (err) {
			const error = toError(err)
			const wasClosing = this.closing
			await this.shutdown()
			// Not started: surface the failure and allow start() to be called again.
			this.lastError = error
			this.currentState = wasClosing ? 'STOPPED' : 'ERROR'
			throw error
		}
	}

	private assertNotClosing(): void {
		if (this.closing) {
			throw new Error('Flow was closed while starting')
		}
		if (this.startupError) {
			throw this.startupError
		}
	}

	/** Topics consumed through the consumer group. */
	private subscribedTopics(): string[] {
		const topics = new Set(this.sourcesByTopic.keys())
		if (!this.clusterAvailable) {
			// Without a cluster (mocked clients) global tables are fed like regular sources.
			for (const table of this.globalTables) topics.add(table.topic)
		}
		return [...topics]
	}

	private async startWorkers(topics: string[]): Promise<void> {
		const threadCount = Math.max(1, this.config.numStreamThreads ?? 1)
		this.workers = []
		this.runPromises = []

		const workerReady: Promise<void>[] = []

		for (let id = 0; id < threadCount; id += 1) {
			const producer = this.client.producer(this.buildProducerConfig(id, threadCount))
			const groupInstanceId = this.buildGroupInstanceId(id, threadCount)

			// Forward reference: the consumer's onBeforeRebalance hook runs at rebalance time,
			// long after the worker has been constructed below.
			const workerRef: { current: WorkerContext | null } = { current: null }

			const userHook = this.config.consumer?.onBeforeRebalance
			const consumer = this.client.consumer({
				groupId: this.config.applicationId,
				...this.config.consumer,
				autoOffsetReset: this.resolveOffsetReset(),
				groupInstanceId,
				isolationLevel: this.eosEnabled ? 'read_committed' : this.config.consumer?.isolationLevel,
				// State is partitioned by source partition, so partition N of every co-partitioned
				// topic must be owned by the same worker. The range assignor guarantees that;
				// the sticky assignors balance topic-partitions individually and do not.
				partitionAssignmentStrategy: 'range',
				// EOS: commit the in-flight transaction inside the awaited rebalance window
				// so offsets land before the group rejoins. Compose with any user-supplied
				// hook (user runs first; their hook can prep state before the EOS commit).
				// A commit failure rejects this promise → the consumer halts before processing
				// on stale state and the worker restarts from the last committed offsets.
				onBeforeRebalance:
					this.eosEnabled || userHook
						? async () => {
								if (userHook) await userHook()
								if (!this.eosEnabled) return
								const w = workerRef.current
								if (w?.transactionActive) {
									await this.enqueueEosTask(w, () => this.commitTransactionBatch(w))
								}
							}
						: undefined,
			})

			const worker: WorkerContext = {
				id,
				producer,
				consumer,
				activeTransaction: null,
				activeTransactionPromise: null,
				offsetCommitContext: null,
				sourcesByTopic: new Map(),
				assignedPartitions: new Map(),
				pendingOffsets: new Map(),
				pendingChangelogOffsets: new Map(),
				dirtyStores: new Set(),
				streamClocks: new Map(),
				failure: null,
				eosQueue: Promise.resolve(),
				transactionActive: false,
				commitTimer: null,
				lastCommitTime: 0,
			}
			workerRef.current = worker

			worker.sourcesByTopic = this.buildWorkerSources(worker)
			this.workers.push(worker)
			this.attachConsumerEvents(consumer, worker)

			workerReady.push(
				new Promise<void>((resolve, reject) => {
					const timer = setTimeout(() => {
						cleanup()
						reject(new Error(`Consumer did not start within ${WORKER_START_TIMEOUT_MS}ms`))
					}, WORKER_START_TIMEOUT_MS)

					const onRunning = () => {
						cleanup()
						resolve()
					}
					const onError = (error: unknown) => {
						cleanup()
						reject(toError(error))
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

			this.runPromises.push(this.runWorker(worker, topics, threadCount))
		}

		await Promise.all(workerReady)

		// Partitions assigned so far are paused until their state is restored (see
		// attachConsumerEvents). Wait for those restorations before reporting the app as started.
		await this.drainRestorations()
	}

	/**
	 * Run a worker's consume loop until the app closes or the worker fails permanently.
	 *
	 * A worker that fails (commit failure, lost transaction, broker outage, ...) never continues
	 * past the failure: its transaction is aborted, buffered state is dropped and the consumer is
	 * stopped, so nothing after the last committed offsets is acknowledged. For recoverable errors
	 * the worker then rejoins the group and resumes from the last committed offsets.
	 */
	private async runWorker(worker: WorkerContext, topics: string[], threadCount: number): Promise<void> {
		let attempt = 0
		for (;;) {
			let runError: unknown = null
			try {
				await worker.consumer.runEach(
					topics,
					(message, ctx) => this.handleMessage(worker, message, ctx),
					this.buildRunEachOptions()
				)
			} catch (err) {
				runError = err
			}

			const failure =
				worker.failure ??
				(runError ? { error: toError(runError), recoverable: isInfrastructureRecoverable(runError) } : null)
			await this.resetWorker(worker)

			if (this.closing) return
			if (!failure) {
				// The consumer stopped without an error and without close(): nothing left to run.
				return
			}

			this.lastError = failure.error
			if (!this.started) {
				// Failures during startup are reported by start() itself.
				this.startupError ??= failure.error
				return
			}
			if (!failure.recoverable || this.config.restartOnError === false) {
				this.logger.error('stream worker failed', { worker: worker.id, error: failure.error.message })
				this.currentState = 'ERROR'
				return
			}

			attempt = worker.processedSinceRestart ? 1 : attempt + 1
			worker.processedSinceRestart = false
			const backoffMs = Math.min(
				RESTART_MAX_BACKOFF_MS,
				RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(attempt - 1, 10)
			)
			this.logger.warn('stream worker failed; restarting', {
				worker: worker.id,
				attempt,
				backoffMs,
				error: failure.error.message,
			})
			this.currentState = 'REBALANCING'
			await this.sleepUnlessClosing(backoffMs)
			if (this.closing) return

			if (this.eosEnabled) {
				// A producer whose transaction failed may be in an undefined state; start clean.
				await worker.producer.disconnect().catch(() => {})
				worker.producer = this.client.producer(this.buildProducerConfig(worker.id, threadCount))
			}
		}
	}

	private sleepUnlessClosing(ms: number): Promise<void> {
		const signal = this.shutdownController.signal
		if (signal.aborted) return Promise.resolve()
		return new Promise(resolve => {
			const timer = setTimeout(() => {
				signal.removeEventListener('abort', onAbort)
				resolve()
			}, ms)
			const onAbort = () => {
				clearTimeout(timer)
				resolve()
			}
			signal.addEventListener('abort', onAbort, { once: true })
		})
	}

	private async handleMessage(worker: WorkerContext, message: Message<Buffer>, ctx: ConsumeContext): Promise<void> {
		// Fail-stop: once a worker has failed, nothing more may be processed (and thereby
		// acknowledged) until it has restarted from the last committed offsets.
		if (worker.failure) throw worker.failure.error

		// close() first seals message delivery, then commits everything already queued.
		// Records that were fetched but not started remain uncommitted for the next owner.
		if (this.closing && this.eosEnabled) return

		const sources = worker.sourcesByTopic.get(message.topic)
		if (!sources) return

		const recordContext: RecordContext = { worker, topic: message.topic, partition: message.partition }

		if (!this.eosEnabled) {
			try {
				await recordContextStorage.run(recordContext, async () => {
					for (const source of sources) {
						await source.handleMessage(message, ctx)
					}
				})
				worker.processedSinceRestart = true
			} catch (err) {
				this.failWorker(worker, err)
				throw err
			}
			return
		}

		await this.enqueueEosTask(worker, () =>
			recordContextStorage.run(recordContext, () => this.processInTransaction(message, ctx, sources, worker))
		)
	}

	/** Mark a worker as failed and stop its consumer. Idempotent; the first failure wins. */
	private failWorker(worker: WorkerContext, err: unknown, recoverable = isRecoverable(err)): void {
		if (!worker.failure) {
			worker.failure = { error: toError(err), recoverable }
		}
		this.lastError = worker.failure.error
		try {
			worker.consumer.stop()
		} catch {
			// already stopping
		}
	}

	/** Bring a stopped worker back to a clean slate (no transaction, no buffered state, no partitions). */
	private async resetWorker(worker: WorkerContext): Promise<void> {
		// Let any queued transactional work finish before tearing the transaction down.
		await worker.eosQueue
		await this.abortTransactionBatch(worker, worker.failure?.error ?? new Error('worker stopped'))
		const released = [...worker.assignedPartitions.values()]
		worker.assignedPartitions.clear()
		worker.streamClocks.clear()
		this.releasePartitions(released)
		worker.failure = null
	}

	close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise
		}
		if (this.currentState === 'STOPPED' || (this.currentState === 'CREATED' && !this.startPromise)) {
			this.currentState = 'STOPPED'
			return Promise.resolve()
		}

		this.closePromise = (async () => {
			// Prevent a buffered record from opening a new transaction after the final commit
			// has been queued but before the consumer is stopped.
			this.closing = true
			this.shutdownController.abort()

			// Commit any pending transactions before stopping. A failure here loses nothing:
			// the transaction aborts and the records are reprocessed after restart.
			for (const worker of this.workers) {
				await this.enqueueEosTask(worker, () => this.commitTransactionBatch(worker)).catch(err => {
					this.lastError = toError(err)
					this.logger.warn('final transaction commit failed during close', {
						worker: worker.id,
						error: this.lastError.message,
					})
				})
			}

			// If start() is still in flight it observes `closing`, fails, and shuts down itself.
			if (this.startPromise && !this.started) {
				await this.startPromise.catch(() => {})
			}
			await this.shutdown()
			this.currentState = 'STOPPED'
			this.startPromise = null
		})()
		return this.closePromise
	}

	/** Stop and release everything. Safe to call on a partially started app. */
	private async shutdown(): Promise<void> {
		this.closing = true
		this.shutdownController.abort()

		for (const worker of this.workers) {
			this.cancelCommitTimer(worker)
			try {
				worker.consumer.stop()
			} catch {
				// not running
			}
		}
		for (const table of this.globalTables) {
			try {
				table.consumer?.stop()
			} catch {
				// not running
			}
		}

		await Promise.allSettled(this.runPromises)
		await Promise.allSettled(this.globalTables.map(table => table.run ?? Promise.resolve()))
		await this.restorationQueue.catch(() => {})

		for (const worker of this.workers) {
			await worker.producer.disconnect().catch(() => {})
		}
		this.workers = []
		this.runPromises = []
		for (const table of this.globalTables) {
			table.consumer = null
			table.run = null
		}

		if (this.pruneTimer) {
			clearTimeout(this.pruneTimer)
			this.pruneTimer = null
		}
		this.processCheckpoints.clear()

		// Drop in-process store instances; the provider owns (and closes) the underlying stores.
		for (const def of this.storeDefs.values()) {
			for (const partition of def.partitions.materializedPartitions()) {
				def.partitions.forget(partition)
			}
		}
		await this.stateStoreProvider.close().catch(err => {
			this.logger.warn('state store provider close failed', { error: toError(err).message })
		})

		if (this.ownsClient) {
			await this.client.disconnect().catch(() => {})
		}
		this.started = false
	}

	state(): StreamState {
		return this.currentState
	}

	getError(): Error | null {
		return this.lastError
	}

	isExactlyOnce(): boolean {
		return this.eosEnabled
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

		// Preserve event time: downstream windowing/joins on the output topic must see the
		// record's timestamp, not the wall-clock time at which it happened to be produced.
		const timestamp = record.timestamp > 0n ? new Date(Number(record.timestamp)) : undefined

		if (this.eosEnabled) {
			if (!worker.activeTransaction) {
				throw new Error('exactly_once requires a transactional context')
			}
			await worker.activeTransaction.send(topic, {
				key: key ?? undefined,
				value,
				headers: record.headers,
				partition,
				timestamp,
			})
			return
		}

		await producer.send(topic, {
			key: key ?? undefined,
			value,
			headers: record.headers,
			partition,
			timestamp,
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

	// ------------------------------------------------------------------------------------------
	// Internal topics & topology validation
	// ------------------------------------------------------------------------------------------

	/**
	 * Validate the topology against the cluster and create missing internal topics
	 * (repartition and changelog topics).
	 *
	 * Partition counts of internal topics are always inferred from the topics that feed them, so
	 * that state partition N, changelog partition N and source partition N all line up.
	 *
	 * Returns false when validation was skipped because no cluster is reachable.
	 *
	 * @throws SourceTopicNotFoundError if a source topic doesn't exist
	 * @throws CoPartitioningError if topics that must be co-partitioned have different partition counts
	 * @throws ChangelogPartitionMismatchError if an existing internal topic has the wrong partition count
	 */
	private async validateAndCreateInternalTopics(): Promise<boolean> {
		const userTopics = new Set<string>()
		for (const topic of this.sourcesByTopic.keys()) {
			if (!this.repartitionTopics.has(topic)) userTopics.add(topic)
		}
		for (const table of this.globalTables) userTopics.add(table.topic)
		const internalTopics = new Set<string>(this.repartitionTopics.keys())
		for (const spec of this.changelogSpecs.values()) {
			internalTopics.add(spec.topicName)
			for (const topic of spec.sourceTopics) {
				if (!this.repartitionTopics.has(topic)) userTopics.add(topic)
			}
		}
		for (const spec of this.repartitionTopics.values()) {
			for (const topic of spec.upstreamTopics) {
				if (!this.repartitionTopics.has(topic)) userTopics.add(topic)
			}
		}

		if (internalTopics.size === 0 && this.globalTables.length === 0 && this.copartitionGroups.length === 0) {
			// Stateless topology: nothing to validate, but report whether a cluster is reachable.
			return this.client.isConnected
		}

		let metadata
		try {
			metadata = await this.client.getMetadata([...userTopics, ...internalTopics])
		} catch (err) {
			const error = err as Error
			if (error.message?.includes('No brokers available')) {
				// No cluster (e.g. mocked client in tests): skip validation/creation/restoration.
				return false
			}
			throw err
		}

		const existing = (topic: string): number | undefined => {
			const size = metadata.topics.get(topic)?.partitions.size
			return size !== undefined && size > 0 ? size : undefined
		}

		// Required partition count of every topic; repartition topics inherit from their upstream.
		const resolving = new Set<string>()
		const required = new Map<string, number>()
		const partitionsOf = (topic: string, neededBy: string): number => {
			const known = required.get(topic)
			if (known !== undefined) return known
			const repartition = this.repartitionTopics.get(topic)
			let count: number
			if (repartition) {
				if (resolving.has(topic)) {
					throw new Error(`Repartition topic "${topic}" depends on itself`)
				}
				resolving.add(topic)
				count = 0
				for (const upstream of repartition.upstreamTopics) {
					count = Math.max(count, partitionsOf(upstream, topic))
				}
				resolving.delete(topic)
				if (count === 0) count = 1
			} else {
				const size = existing(topic)
				if (size === undefined) {
					throw new SourceTopicNotFoundError(topic, neededBy)
				}
				count = size
			}
			required.set(topic, count)
			return count
		}

		for (const topic of userTopics) partitionsOf(topic, 'topology')

		// Topics that share state must agree on the partition count.
		for (const group of this.copartitionGroups) {
			const counts = new Map<string, number>()
			for (const topic of group) counts.set(topic, partitionsOf(topic, 'co-partitioned operation'))
			if (new Set(counts.values()).size > 1) {
				throw new CoPartitioningError(counts)
			}
		}

		const topicsToCreate: Array<{
			name: string
			numPartitions: number
			replicationFactor?: number
			configs?: Record<string, string>
		}> = []
		const validateOnly = this.config.changelog?.autoCreate === false

		for (const spec of this.repartitionTopics.values()) {
			const requiredPartitions = partitionsOf(spec.topicName, 'topology')
			const actual = existing(spec.topicName)
			if (actual !== undefined) {
				if (actual !== requiredPartitions) {
					throw new ChangelogPartitionMismatchError(spec.topicName, requiredPartitions, actual, [
						...spec.upstreamTopics,
					])
				}
				continue
			}
			if (validateOnly) {
				throw new SourceTopicNotFoundError(spec.topicName, 'repartition (changelog.autoCreate is false)')
			}
			topicsToCreate.push({
				name: spec.topicName,
				numPartitions: requiredPartitions,
				replicationFactor: this.config.changelog?.replicationFactor,
			})
		}

		for (const [storeName, spec] of this.changelogSpecs) {
			let requiredPartitions = 0
			for (const sourceTopic of spec.sourceTopics) {
				requiredPartitions = Math.max(requiredPartitions, partitionsOf(sourceTopic, storeName))
			}
			if (requiredPartitions === 0) {
				// No source topics: a single-partition changelog.
				requiredPartitions = 1
			}

			const actual = existing(spec.topicName)
			if (actual !== undefined) {
				if (actual !== requiredPartitions) {
					throw new ChangelogPartitionMismatchError(spec.topicName, requiredPartitions, actual, [
						...spec.sourceTopics,
					])
				}
				continue
			}

			if (!spec.validateOnly) {
				topicsToCreate.push({
					name: spec.topicName,
					numPartitions: requiredPartitions,
					replicationFactor: spec.replicationFactor ?? this.config.changelog?.replicationFactor,
					configs: spec.configs,
				})
			}
		}

		if (topicsToCreate.length > 0) {
			await this.client.createTopics(topicsToCreate)
			await this.waitForTopics(topicsToCreate.map(t => t.name))
		}

		return true
	}

	/** Wait until newly created topics are visible in metadata with leaders, so consumers can subscribe. */
	private async waitForTopics(topics: string[]): Promise<void> {
		const deadline = Date.now() + 30_000
		for (;;) {
			try {
				const metadata = await this.client.getMetadata(topics)
				const ready = topics.every(topic => (metadata.topics.get(topic)?.partitions.size ?? 0) > 0)
				if (ready) return
			} catch {
				// metadata not propagated yet
			}
			if (Date.now() > deadline) {
				throw new Error(`Timed out waiting for internal topics to become available: ${topics.join(', ')}`)
			}
			await new Promise(resolve => setTimeout(resolve, 100))
		}
	}

	// ------------------------------------------------------------------------------------------
	// State restoration
	// ------------------------------------------------------------------------------------------

	/** App-scoped stores hold state for all partitions; rebuild them fully before processing. */
	private async restoreAppScopedStores(): Promise<void> {
		await Promise.all(
			[...this.storeDefs.values()]
				.filter(def => def.scope === 'app' && def.changelog && !def.changelog.skipRestoration)
				.map(async def => {
					const spec = def.changelog!
					const stack = await def.partitions.forPartition(0)
					const restorer = new ChangelogRestorer(
						spec.topicName,
						spec.keyCodec,
						spec.valueCodec,
						stack.raw as unknown as KeyValueStore<unknown, unknown>
					)
					await this.runRestorer(restorer, undefined)
				})
		)
	}

	/**
	 * Restore the state partitions that back newly assigned source partitions.
	 * Restorations are serialized so a partition is never restored twice concurrently.
	 */
	private enqueueRestoration(partitions: TopicPartition[]): Promise<void> {
		const task = this.restorationQueue.then(() => this.restorePartitions(partitions))
		this.restorationQueue = task.catch(err => {
			// A restore failure during startup fails start() (see drainRestorations).
			if (!this.started) this.startupError ??= toError(err)
		})
		return task
	}

	private async drainRestorations(): Promise<void> {
		let seen: Promise<void> | null = null
		while (seen !== this.restorationQueue) {
			seen = this.restorationQueue
			await seen
			this.assertNotClosing()
		}
	}

	private async restorePartitions(assigned: TopicPartition[]): Promise<void> {
		if (!this.clusterAvailable || assigned.length === 0) return

		await Promise.all(
			[...this.storeDefs.values()]
				.filter(def => def.scope === 'partition' && def.changelog && !def.changelog.skipRestoration)
				.map(async def => {
					const spec = def.changelog!
					const partitions = [
						...new Set(assigned.filter(tp => def.sourceTopics.has(tp.topic)).map(tp => tp.partition)),
					]
					if (partitions.length === 0) return

					const restorer = new ChangelogRestorer(
						spec.topicName,
						spec.keyCodec,
						spec.valueCodec,
						async partition => {
							const stack = await def.partitions.forPartition(partition)
							// Anything still buffered belongs to a transaction that no longer exists.
							stack.buffer.discardBuffer()
							return stack.raw as unknown as KeyValueStore<unknown, unknown>
						}
					)
					await this.runRestorer(restorer, partitions)
				})
		)
	}

	private async runRestorer(
		restorer: ChangelogRestorer<unknown, unknown>,
		partitions: number[] | undefined
	): Promise<void> {
		try {
			await restorer.restore(
				this.client,
				this.config.changelog?.restoration,
				partitions,
				this.changelogCheckpointStore
			)
		} catch (err) {
			// A changelog topic that doesn't exist yet has nothing to restore (first startup with
			// autoCreate disabled); anything else must fail loudly rather than run on partial state.
			const error = err as Error & { code?: string }
			if (error.code === 'UNKNOWN_TOPIC_OR_PARTITION') return
			if (error instanceof KafkaError && error.errorCode === ErrorCode.UnknownTopicOrPartition) return
			throw err
		}
	}

	/**
	 * Start reading newly assigned repartition-topic partitions from the beginning when the group
	 * has no committed offset for them. With the consumer default ('latest'), records written to
	 * an internal topic before this worker resolved its position would be skipped, i.e. lost.
	 */
	private async resetRepartitionPositions(worker: WorkerContext, partitions: TopicPartition[]): Promise<void> {
		const internal = partitions.filter(
			tp => this.repartitionTopics.has(tp.topic) || this.throughTopics.has(tp.topic)
		)
		if (internal.length === 0 || !this.clusterAvailable) return

		const byTopic = new Map<string, number[]>()
		for (const tp of internal) {
			byTopic.set(tp.topic, [...(byTopic.get(tp.topic) ?? []), tp.partition])
		}

		const groupId = this.config.applicationId
		const topics = [...byTopic].map(([name, parts]) => ({
			name,
			partitions: parts.map(partitionIndex => ({ partitionIndex })),
		}))
		// Offsets of an in-flight transaction (another instance committing) are reported as
		// unstable; wait for them rather than guessing.
		const deadline = Date.now() + 30_000
		let response
		for (;;) {
			try {
				const coordinator = await this.client.cluster.getCoordinator('GROUP', groupId)
				response = await coordinator.offsetFetch({ groupId, topics, requireStable: true })
				const code =
					response.errorCode !== ErrorCode.None
						? response.errorCode
						: response.topics.flatMap(t => t.partitions).find(p => p.errorCode !== ErrorCode.None)
								?.errorCode
				if (code === undefined) break
				throw new KafkaError(`OffsetFetch failed for group ${groupId}`, code)
			} catch (err) {
				const retriable = err instanceof KafkaError && err.retriable
				if (!retriable || Date.now() > deadline || this.closing) throw err
				this.client.cluster.invalidateCoordinator('GROUP', groupId)
				await new Promise(resolve => setTimeout(resolve, 200))
			}
		}

		const admin = this.client.admin()
		for (const topic of response.topics) {
			const uncommitted: number[] = []
			for (const partition of topic.partitions) {
				if (partition.committedOffset < 0n) uncommitted.push(partition.partitionIndex)
			}
			if (uncommitted.length === 0) continue
			const earliest = await admin.fetchTopicOffsets(topic.name, uncommitted, 'earliest')
			for (const partition of uncommitted) {
				if (!worker.assignedPartitions.has(`${topic.name}:${partition}`)) continue
				worker.consumer.seek(topic.name, partition, earliest.get(partition) ?? 0n)
			}
		}
	}

	/** Whether assigned partitions need asynchronous preparation before records may flow. */
	private needsAssignmentPreparation(): boolean {
		if (!this.clusterAvailable) return false
		if (this.repartitionTopics.size > 0 || this.throughTopics.size > 0) return true
		for (const def of this.storeDefs.values()) {
			if (def.scope === 'partition') return true
		}
		return false
	}

	/**
	 * Partitions were revoked or lost: drop anything buffered for them and schedule a cleanup.
	 *
	 * The local copy is kept for now, because the partition often comes straight back (every
	 * rebalance revokes before it re-assigns) and can then be caught up from its checkpoint instead
	 * of being rebuilt from the start of the changelog.
	 */
	private releasePartitions(released: TopicPartition[]): void {
		if (released.length === 0) return
		const task = this.restorationQueue.then(async () => {
			for (const def of this.storeDefs.values()) {
				if (def.scope !== 'partition') continue
				const partitions = new Set(
					released.filter(tp => def.sourceTopics.has(tp.topic)).map(tp => tp.partition)
				)
				for (const partition of partitions) {
					if (this.isStatePartitionOwned(def, partition)) continue
					if (!def.partitions.materializedPartitions().includes(partition)) continue
					const stack = await def.partitions.forPartition(partition)
					stack.buffer.discardBuffer()
				}
			}
		})
		this.restorationQueue = task.catch(err => {
			this.logger.warn('failed to release state of revoked partitions', { error: toError(err).message })
		})
		this.schedulePrune()
	}

	/**
	 * Once assignments have settled, drop the in-memory state of partitions this instance no longer
	 * owns. Another instance is advancing that state now, so the local copy would only go stale and
	 * hold memory. Durable state (with durable checkpoints) is kept on disk for a later return.
	 */
	private schedulePrune(): void {
		if (this.durableCheckpoints) return
		if (this.pruneTimer) clearTimeout(this.pruneTimer)
		this.pruneTimer = setTimeout(() => {
			this.pruneTimer = null
			if (this.closing) return
			const task = this.restorationQueue.then(async () => {
				for (const def of this.storeDefs.values()) {
					if (def.scope !== 'partition') continue
					for (const partition of def.partitions.materializedPartitions()) {
						if (this.closing || this.isStatePartitionOwned(def, partition)) continue
						const stack = await def.partitions.forPartition(partition)
						stack.buffer.discardBuffer()
						await clearStore(stack.raw as unknown as KeyValueStore<unknown, unknown>)
						def.partitions.forget(partition)
						if (def.changelog) {
							this.processCheckpoints.delete(`${def.changelog.topicName}:${partition}`)
						}
					}
				}
			})
			this.restorationQueue = task.catch(err => {
				this.logger.warn('failed to prune state of unowned partitions', { error: toError(err).message })
			})
		}, UNOWNED_STATE_PRUNE_DELAY_MS)
		this.pruneTimer.unref?.()
	}

	private isStatePartitionOwned(def: StoreDef, partition: number): boolean {
		for (const worker of this.workers) {
			for (const tp of worker.assignedPartitions.values()) {
				if (tp.partition === partition && def.sourceTopics.has(tp.topic)) return true
			}
		}
		return false
	}

	// ------------------------------------------------------------------------------------------
	// Global tables
	// ------------------------------------------------------------------------------------------

	/**
	 * Load a global table completely, then keep it up to date in the background.
	 * Resolves once the table has caught up to the end of its topic as of startup.
	 */
	private async startGlobalTable(table: GlobalTableDef): Promise<void> {
		await table.store.init()
		// The topic is the source of truth: rebuild from scratch so removed rows don't survive.
		await clearStore(table.store)

		const metadata = await this.client.getMetadata([table.topic])
		const partitions = [...(metadata.topics.get(table.topic)?.partitions.keys() ?? [])]
		if (partitions.length === 0) {
			throw new SourceTopicNotFoundError(table.topic, table.storeName)
		}

		const admin = this.client.admin()
		const [startOffsets, endOffsets] = await Promise.all([
			admin.fetchTopicOffsets(table.topic, partitions, 'earliest', { isolationLevel: 'read_committed' }),
			admin.fetchTopicOffsets(table.topic, partitions, 'latest', { isolationLevel: 'read_committed' }),
		])

		type Progress = { end: bigint; position: bigint; fetched: number; handled: number }
		const progress = new Map<number, Progress>()
		const pending = new Set<number>()
		for (const partition of partitions) {
			const start = startOffsets.get(partition) ?? 0n
			const end = endOffsets.get(partition) ?? 0n
			progress.set(partition, { end, position: start, fetched: 0, handled: 0 })
			if (start < end) pending.add(partition)
		}

		let resolveBootstrap!: () => void
		let rejectBootstrap!: (err: Error) => void
		const bootstrapped = new Promise<void>((resolve, reject) => {
			resolveBootstrap = resolve
			rejectBootstrap = reject
		})
		bootstrapped.catch(() => {})
		const check = (partition: number) => {
			const state = progress.get(partition)
			if (!state || !pending.has(partition)) return
			if (state.position >= state.end && state.handled >= state.fetched) {
				pending.delete(partition)
				if (pending.size === 0) resolveBootstrap()
			}
		}
		if (pending.size === 0) resolveBootstrap()

		const positions = new Map<number, bigint>()
		for (const partition of partitions) positions.set(partition, startOffsets.get(partition) ?? 0n)

		const run = async () => {
			let attempt = 0
			while (!this.closing) {
				const consumer = this.client.consumer({
					groupId: `${this.config.applicationId}-global-${table.storeName}-${this.processId}`,
					autoOffsetReset: 'earliest',
					isolationLevel: 'read_committed',
				})
				table.consumer = consumer
				consumer.on('error', error => {
					this.lastError = error
				})
				consumer.on('fetchPosition', (position, recordCount) => {
					if (position.topic !== table.topic) return
					const state = progress.get(position.partition)
					if (!state) return
					state.fetched += recordCount
					if (position.offset > state.position) state.position = position.offset
					check(position.partition)
				})
				try {
					await consumer.runEach(
						[table.topic],
						async message => {
							try {
								if (message.key !== null) {
									const key = table.keyCodec.decode(message.key)
									if (message.value === null) {
										await table.store.delete(key)
									} else {
										await table.store.put(key, table.valueCodec.decode(message.value))
									}
								}
								positions.set(message.partition, message.offset + 1n)
							} finally {
								const state = progress.get(message.partition)
								if (state) state.handled += 1
								check(message.partition)
							}
						},
						{
							commitOffsets: false,
							assignment: partitions.map(partition => ({
								topic: table.topic,
								partition,
								offset: positions.get(partition) ?? 0n,
							})),
						}
					)
					if (this.closing) return
				} catch (err) {
					if (this.closing) return
					const error = toError(err)
					this.lastError = error
					if (pending.size > 0) {
						// Still bootstrapping: fail startup rather than serve a partial table.
						rejectBootstrap(error)
						return
					}
					if (!isInfrastructureRecoverable(error)) {
						this.logger.error('global table consumer failed', { topic: table.topic, error: error.message })
						this.currentState = 'ERROR'
						return
					}
					attempt += 1
					this.logger.warn('global table consumer failed; restarting', {
						topic: table.topic,
						attempt,
						error: error.message,
					})
				}
				// Resume from the last applied positions; fetch accounting restarts with them.
				for (const state of progress.values()) {
					state.fetched = 0
					state.handled = 0
				}
				await this.sleepUnlessClosing(
					Math.min(RESTART_MAX_BACKOFF_MS, RESTART_INITIAL_BACKOFF_MS * 2 ** Math.min(attempt, 10))
				)
			}
		}

		table.run = run()

		const idleTimeoutMs = this.config.changelog?.restoration?.idleTimeoutMs ?? 30_000
		let lastSnapshot = ''
		let lastChange = Date.now()
		const watchdog = setInterval(() => {
			const snapshot = [...progress.values()].map(s => `${s.position}:${s.handled}`).join(',')
			if (snapshot !== lastSnapshot) {
				lastSnapshot = snapshot
				lastChange = Date.now()
			} else if (Date.now() - lastChange > idleTimeoutMs) {
				rejectBootstrap(
					new Error(`Timed out loading global table "${table.topic}": no progress for ${idleTimeoutMs}ms`)
				)
			}
		}, 1000)
		const onClose = () => rejectBootstrap(new Error('Flow was closed while starting'))
		this.shutdownController.signal.addEventListener('abort', onClose, { once: true })
		try {
			await bootstrapped
		} finally {
			clearInterval(watchdog)
			this.shutdownController.signal.removeEventListener('abort', onClose)
		}
	}

	// ------------------------------------------------------------------------------------------
	// Workers
	// ------------------------------------------------------------------------------------------

	private buildWorkerSources(worker: WorkerContext): Map<string, SourceNode<unknown, unknown>[]> {
		const clonedByTopic = new Map<string, SourceNode<unknown, unknown>[]>()
		const cloneMap = new Map<AnyProcessor, AnyProcessor>()

		const add = (topic: string, source: SourceNode<unknown, unknown>) => {
			const cloned = source.cloneGraph(worker, cloneMap)
			const list = clonedByTopic.get(topic) ?? []
			list.push(cloned as SourceNode<unknown, unknown>)
			clonedByTopic.set(topic, list)
		}

		for (const [topic, sources] of this.sourcesByTopic) {
			for (const source of sources) add(topic, source)
		}
		if (!this.clusterAvailable) {
			for (const table of this.globalTables) add(table.topic, table.source)
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
		const transactionalId = base.transactionalId
			? threadCount > 1
				? `${base.transactionalId}-w${workerId}`
				: base.transactionalId
			: `${this.config.applicationId}-${this.processId}-w${workerId}`
		return {
			...base,
			transactionalId,
			idempotent: true,
			retries: base.retries ?? 10,
		}
	}

	private buildGroupInstanceId(workerId: number, threadCount: number): string | undefined {
		const base = this.config.consumer?.groupInstanceId
		if (!base) {
			return undefined
		}
		return threadCount > 1 ? `${base}-w${workerId}` : base
	}

	private enqueueEosTask<T>(worker: WorkerContext, fn: () => Promise<T>): Promise<T> {
		const run = worker.eosQueue.then(fn, fn)
		worker.eosQueue = run.then(
			() => {},
			() => {}
		)
		return run
	}

	// ------------------------------------------------------------------------------------------
	// Exactly-once processing
	// ------------------------------------------------------------------------------------------

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
		// A commit queued ahead of this record may have failed in the meantime.
		if (worker.failure) throw worker.failure.error

		try {
			// Start a new transaction if one isn't active
			if (!worker.transactionActive) {
				await this.beginTransaction(worker)
				worker.offsetCommitContext = ctx
			}

			// Process the message within the current transaction
			for (const source of sources) {
				await source.handleMessage(message, ctx)
			}

			// Track this offset for the batch commit
			const key: TopicPartitionKey = `${message.topic}:${message.partition}`
			const existingOffset = worker.pendingOffsets.get(key)
			if (!existingOffset || message.offset + 1n > existingOffset.offset) {
				worker.pendingOffsets.set(key, {
					topic: message.topic,
					partition: message.partition,
					offset: message.offset + 1n, // Commit the next offset
				})
			}
		} catch (err) {
			this.failWorker(worker, err)
			await this.abortTransactionBatch(worker, err)
			throw err
		}

		// Commit if the interval has elapsed, otherwise make sure a commit is scheduled.
		if (Date.now() - worker.lastCommitTime >= this.commitIntervalMs) {
			await this.commitTransactionBatch(worker)
		} else if (!worker.commitTimer) {
			this.scheduleCommit(worker)
		}
	}

	/**
	 * Begin a new transaction for the worker
	 */
	private async beginTransaction(worker: WorkerContext): Promise<void> {
		const started = await this.startProducerTransaction(worker.producer)
		worker.activeTransaction = started.tx
		worker.activeTransactionPromise = started.completion
		worker.transactionActive = true
		worker.lastCommitTime = Date.now()
	}

	/**
	 * Start a producer transaction and return the transaction handle
	 */
	private startProducerTransaction(
		producer: Producer
	): Promise<{ tx: ActiveTransaction; completion: Promise<void> }> {
		return new Promise((resolve, reject) => {
			let txResolved = false

			const completion = producer.transaction(async tx => {
				const txHandle = tx as ActiveTransaction & { _resolve?: () => void; _reject?: (err: Error) => void }

				const gate = new Promise<void>((resolveInner, rejectInner) => {
					txHandle._resolve = resolveInner
					txHandle._reject = (err: Error) => rejectInner(err)
				})

				txResolved = true
				queueMicrotask(() => resolve({ tx: txHandle, completion }))
				return gate
			})

			completion.catch(err => {
				if (!txResolved) {
					reject(toError(err))
				}
			})
		})
	}

	/**
	 * Commit the current transaction batch: offsets + outputs + changelog records atomically in
	 * Kafka, then apply the buffered state updates locally and advance the checkpoints.
	 *
	 * On failure the transaction is aborted, buffered state is dropped and the worker is failed,
	 * so nothing after the last successful commit is ever acknowledged or kept.
	 */
	private async commitTransactionBatch(worker: WorkerContext): Promise<void> {
		this.cancelCommitTimer(worker)

		if (!worker.transactionActive || !worker.activeTransaction || !worker.activeTransactionPromise) {
			return
		}

		const tx = worker.activeTransaction
		const txPromise = worker.activeTransactionPromise
		const offsets = Array.from(worker.pendingOffsets.values())
		const changelogOffsets = Array.from(worker.pendingChangelogOffsets.values())
		const dirtyStores = [...worker.dirtyStores]

		try {
			if (offsets.length > 0) {
				if (!worker.offsetCommitContext) {
					throw new Error('exactly_once requires a consume context for transactional offset commits')
				}
				await tx.sendOffsets(worker.offsetCommitContext, offsets)
			}

			// Complete the transaction by resolving the gate promise (commit happens inside producer.transaction()).
			const txWithResolve = tx as ActiveTransaction & { _resolve?: () => void }
			txWithResolve._resolve?.()
			await txPromise
		} catch (err) {
			const txWithReject = tx as ActiveTransaction & { _reject?: (err: Error) => void }
			txWithReject._reject?.(toError(err))
			await txPromise.catch(() => {})
			for (const store of dirtyStores) store.discardBuffer(worker.dirtyStores)
			this.clearTransactionState(worker)
			// The consumer's position is already past the records of this batch: it must not
			// continue, or the next commit would acknowledge records whose effects were aborted.
			this.failWorker(worker, err, isInfrastructureRecoverable(err))
			throw err
		}

		// The transaction is committed. Make the buffered state visible locally, then checkpoint.
		// A crash in between is safe: the store is at most behind the changelog and the checkpoint
		// still points at or before it, so restoration replays the difference.
		this.clearTransactionState(worker)
		worker.processedSinceRestart = true
		try {
			for (const store of dirtyStores) {
				await store.commitBuffer(worker.dirtyStores)
			}
		} catch (err) {
			// Local state is now behind the committed changelog; only a restore can repair it.
			this.failWorker(worker, err, true)
			throw err
		}
		await this.flushChangelogCheckpoints(changelogOffsets)
	}

	private clearTransactionState(worker: WorkerContext): void {
		worker.pendingOffsets.clear()
		worker.pendingChangelogOffsets.clear()
		worker.dirtyStores.clear()
		worker.activeTransaction = null
		worker.activeTransactionPromise = null
		worker.offsetCommitContext = null
		worker.transactionActive = false
		worker.lastCommitTime = Date.now()
	}

	private async abortTransactionBatch(worker: WorkerContext, error: unknown): Promise<void> {
		this.cancelCommitTimer(worker)

		const tx = worker.activeTransaction as (ActiveTransaction & { _reject?: (err: Error) => void }) | null
		const txPromise = worker.activeTransactionPromise

		for (const store of worker.dirtyStores) store.discardBuffer(worker.dirtyStores)
		this.clearTransactionState(worker)
		// Stream time advanced by the aborted records must not make their replay look late.
		worker.streamClocks.clear()

		if (tx && txPromise) {
			tx._reject?.(toError(error))
			await txPromise.catch(() => {})
		}
	}

	private async flushChangelogCheckpoints(
		checkpoints: Array<{ topic: string; partition: number; offset: bigint }>
	): Promise<void> {
		if (checkpoints.length === 0) {
			return
		}

		// Data is already durable post-EOS-commit; surface failures so a stuck checkpoint store doesn't silently cause restore-time over-replay.
		let anyFailure = false
		for (const checkpoint of checkpoints) {
			try {
				// `offset` is the position after the last changelog record of the transaction; the
				// transaction's commit marker occupies that position, so the committed state ends
				// one past it. (A state partition has a single writer, so nothing else can sit
				// in between; restoration tolerates either position.)
				await this.changelogCheckpointStore.set(checkpoint.topic, checkpoint.partition, checkpoint.offset + 1n)
			} catch (err) {
				anyFailure = true
				this.recordCheckpointError(err, { topic: checkpoint.topic, partition: checkpoint.partition })
			}
		}
		// One fsync covers all the puts above; awaiting per-set would be O(partitions) syscalls per commit.
		try {
			await this.changelogCheckpointStore.flush?.()
		} catch (err) {
			anyFailure = true
			this.recordCheckpointError(err, { phase: 'flush' })
		}
		if (!anyFailure && this.lastCheckpointErrorMessage !== null) {
			this.logger.info('checkpoint persistence recovered')
			this.lastCheckpointErrorMessage = null
		}
	}

	private recordCheckpointError(err: unknown, context: Record<string, unknown>): void {
		const error = toError(err)
		this.lastError = error
		// Dedupe log lines: persistent disk-full would otherwise emit per-partition every commit interval.
		if (this.lastCheckpointErrorMessage !== error.message) {
			this.lastCheckpointErrorMessage = error.message
			this.logger.error('checkpoint persistence failed', { ...context, error: error.message })
		}
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
			// A failed commit fails the worker inside commitTransactionBatch (fail-stop).
			this.enqueueEosTask(worker, () => this.commitTransactionBatch(worker)).catch(() => {})
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
		value: V | null,
		partitioner: (key: K | null, value: V | null, partitionCount: number) => number
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
		// Note: 'rebalance' is a synchronous notification; the actual EOS commit is wired through
		// onBeforeRebalance (passed to the consumer config) which is awaited by the rebalance protocol.
		consumer.on('rebalance', () => {
			if (this.currentState === 'RUNNING') {
				this.currentState = 'REBALANCING'
			}
		})
		consumer.on('partitionsAssigned', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.set(`${tp.topic}:${tp.partition}`, tp)
				// A (re)assigned partition restarts from its committed offset; so does its clock.
				const suffix = `|${tp.topic}:${tp.partition}`
				for (const key of worker.streamClocks.keys()) {
					if (key.endsWith(suffix)) worker.streamClocks.delete(key)
				}
			}

			if (this.needsAssignmentPreparation()) {
				// No record of these partitions may be processed until their state is restored
				// and internal-topic positions are settled.
				try {
					consumer.pause(partitions)
				} catch {
					// Ignore - pause is best effort and may fail if the consumer is stopping
				}

				void this.enqueueRestoration(partitions)
					.then(() => this.resetRepartitionPositions(worker, partitions))
					.then(() => {
						const stillAssigned = partitions.filter(tp =>
							worker.assignedPartitions.has(`${tp.topic}:${tp.partition}`)
						)
						if (stillAssigned.length === 0 || worker.failure || this.closing) return
						try {
							consumer.resume(stillAssigned)
						} catch {
							// Ignore - consumer may have been stopped while restoring
						}
					})
					.catch(err => {
						if (this.closing) return
						const error = toError(err)
						if (!this.started) {
							// start() reports the failure.
							this.startupError ??= error
						}
						// Never process on unrestored state: stop, then retry from scratch.
						this.failWorker(worker, error, true)
					})
			}

			if (this.started && this.currentState === 'REBALANCING') {
				this.currentState = 'RUNNING'
			}
		})
		consumer.on('partitionsRevoked', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
			this.releasePartitions(partitions)
		})
		consumer.on('partitionsLost', partitions => {
			for (const tp of partitions) {
				worker.assignedPartitions.delete(`${tp.topic}:${tp.partition}`)
			}
			if (this.eosEnabled && worker.transactionActive) {
				// The open transaction contains work for partitions we no longer own; it can
				// never commit. Abort it, and restart the worker if it still holds partitions
				// whose uncommitted records must be re-read.
				const error = new Error('Partitions were lost while a transaction was open')
				if (worker.assignedPartitions.size > 0) {
					this.failWorker(worker, error, true)
				} else {
					void this.enqueueEosTask(worker, () => this.abortTransactionBatch(worker, error))
				}
			}
			this.releasePartitions(partitions)
		})
		consumer.on('error', error => {
			// Informational: the consumer reports transient errors here too. Fatal failures
			// surface through the worker's run loop, which decides between restart and ERROR.
			this.lastError = error
		})
	}
}

export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}
