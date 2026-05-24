import { AsyncLocalStorage } from 'node:async_hooks'
import {
	KafkaClient,
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
 * AsyncLocalStorage for tracking the current worker context during message processing.
 * Used by changelog writers to access the correct producer/transaction.
 */
export const workerContextStorage = new AsyncLocalStorage<WorkerContext>()

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
	readonly stateStoreProvider: StateStoreProvider
	private readonly changelogCheckpointStore: ChangelogCheckpointStore | null
	readonly stateStores = new Map<string, KeyValueStore<unknown, unknown>>()
	private readonly changelogTopics = new Map<string, ChangelogTopicSpec>()
	private readonly changelogWriters = new Map<string, ChangelogWriter<unknown, unknown>>()
	private readonly logger: Logger
	private lastCheckpointErrorMessage: string | null = null
	private currentState: StreamState = 'CREATED'
	private workers: WorkerContext[] = []
	private runPromises: Promise<void>[] = []
	private stoppedWorkers = 0
	private totalWorkers = 0
	private lastError: Error | null = null
	private storeCounter = 0
	private startupProducer: Producer | null = null

	/** Get the next store ID and increment the counter */
	nextStoreId(): number {
		return this.storeCounter++
	}
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
		this.changelogCheckpointStore = this.stateStoreProvider.getChangelogCheckpointStore?.() ?? null
		this.logger = this.client.cluster.getLogger().child({ component: 'flow', applicationId: config.applicationId })
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
		let store = this.stateStores.get(storeName)
		if (!store) {
			const innerStore = this.stateStoreProvider.createKeyValueStore(storeName, {
				keyCodec: keyCodec as Codec<unknown>,
				valueCodec: valueCodec as Codec<unknown>,
			})

			// Resolve changelog config (defaults to enabled)
			const changelogConfig = resolveChangelogConfig(changelog)

			if (changelogConfig.enabled) {
				const writer = this.setupChangelog<K, V>(
					storeName,
					keyCodec,
					valueCodec,
					changelogConfig,
					sourceTopics ?? new Set(),
					restrictRestorationToSourcePartitions
				)
				// Wrap with changelog-backed store
				store = new ChangelogBackedKeyValueStore(innerStore, writer) as KeyValueStore<unknown, unknown>
			} else {
				store = innerStore
			}

			this.stateStores.set(storeName, store)
		}
		return store as KeyValueStore<K, V>
	}

	/**
	 * Register a changelog topic spec and writer for a store, returning the writer so the caller
	 * can wrap the inner store with the matching ChangelogBacked* wrapper.
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
		restrictRestorationToSourcePartitions: boolean,
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

		// Partition count is always inferred from source topics to ensure state locality.
		// Task N processing partition N must write to changelog partition N.
		const spec: ChangelogTopicSpec = {
			storeName,
			topicName,
			replicationFactor: changelogConfig.replicationFactor,
			configs: getDefaultTopicConfigs({ ...windowConfigOverrides, ...changelogConfig.topicConfigs }),
			keyCodec: changelogKeyCodec as Codec<unknown>,
			valueCodec: valueCodec as Codec<unknown>,
			skipRestoration: changelogConfig.skipRestoration ?? false,
			sourceTopics,
			restrictRestorationToSourcePartitions,
			validateOnly: this.config.changelog?.autoCreate === false,
		}
		this.changelogTopics.set(storeName, spec)

		// Create changelog writer with callbacks to get producer/transaction from worker context
		const writer = new ChangelogWriter<KK, VV>(
			topicName,
			changelogKeyCodec,
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
			},
			async result => {
				if (!this.changelogCheckpointStore) {
					return
				}
				const worker = workerContextStorage.getStore()
				if (worker?.activeTransaction) {
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
			}
		)
		this.changelogWriters.set(storeName, writer as ChangelogWriter<unknown, unknown>)
		return writer
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

		const inner = this.stateStoreProvider.createWindowStore<K, V>(storeName, {
			keyCodec,
			valueCodec,
			retentionMs: windowOptions.retentionMs,
			windowSizeMs: windowOptions.windowSizeMs,
		})

		const changelogConfig = resolveChangelogConfig(changelog)
		let store: WindowStore<K, V> = inner
		if (changelogConfig.enabled) {
			const writer = this.setupChangelog<WindowedKey<K>, V>(
				storeName,
				windowedKeyCodec(keyCodec),
				valueCodec,
				changelogConfig,
				sourceTopics ?? new Set(),
				restrictRestorationToSourcePartitions,
				windowOptions.retentionMs
			)
			store = new ChangelogBackedWindowStore(inner, writer)
		}

		this.stateStores.set(storeName, store as unknown as KeyValueStore<unknown, unknown>)
		return store
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

		const inner = this.stateStoreProvider.createSessionStore<K, V>(storeName, {
			keyCodec,
			valueCodec,
			retentionMs: sessionOptions.retentionMs,
		})

		const changelogConfig = resolveChangelogConfig(changelog)
		let store: SessionStore<K, V> = inner
		if (changelogConfig.enabled) {
			const writer = this.setupChangelog<WindowedKey<K>, V>(
				storeName,
				windowedKeyCodec(keyCodec),
				valueCodec,
				changelogConfig,
				sourceTopics ?? new Set(),
				restrictRestorationToSourcePartitions,
				sessionOptions.retentionMs
			)
			store = new ChangelogBackedSessionStore(inner, writer)
		}

		this.stateStores.set(storeName, store as unknown as KeyValueStore<unknown, unknown>)
		return store
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
		sourceNode.connect(tableStateNode)

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

		const storeName = options?.materialized?.storeName ?? `global-table-${resolved.topic}-${this.nextStoreId()}`
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
		sourceNode.connect(tableStateNode)

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

			// Forward reference: the consumer's onBeforeRebalance hook runs at rebalance time,
			// long after the worker has been constructed below. A holder lets the closure
			// resolve to the right WorkerContext without depending on let-binding shadowing.
			const workerRef: { current: WorkerContext | null } = { current: null }

			const userHook = this.config.consumer?.onBeforeRebalance
			const consumer = this.client.consumer({
				groupId: this.config.applicationId,
				...this.config.consumer,
				autoOffsetReset: this.resolveOffsetReset(),
				groupInstanceId,
				isolationLevel: this.eosEnabled ? 'read_committed' : this.config.consumer?.isolationLevel,
				// EOS: commit the in-flight transaction inside the awaited rebalance window
				// so offsets land before the group rejoins. Compose with any user-supplied
				// hook (user runs first; their hook can prep state before the EOS commit).
				// A commit failure rejects this promise → the rebalance protocol's catch
				// emits the error and the consumer halts before processing on stale state.
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
				groupInstanceId: groupInstanceId ?? null,
				sourcesByTopic: new Map(),
				assignedPartitions: new Map(),
				// Transaction batching state
				pendingOffsets: new Map(),
				pendingChangelogOffsets: new Map(),
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
						const handler = async () => {
							const sources = worker.sourcesByTopic.get(message.topic)
							if (!sources) return
							if (!this.eosEnabled) {
								for (const source of sources) {
									await source.handleMessage(message, ctx)
								}
								return
							}
							await this.processInTransaction(message, ctx, sources, worker)
						}

						if (!this.eosEnabled) {
							// Run within worker context for changelog writes
							await workerContextStorage.run(worker, handler)
							return
						}

						await this.enqueueEosTask(worker, () => workerContextStorage.run(worker, handler))
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
			await this.enqueueEosTask(worker, () => this.commitTransactionBatch(worker)).catch(() => {
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

			const partitions =
				spec.restrictRestorationToSourcePartitions !== false && spec.sourceTopics.size
					? [
							...new Set(
								assignedPartitions.filter(tp => spec.sourceTopics.has(tp.topic)).map(tp => tp.partition)
							),
						]
					: undefined
			if (partitions?.length === 0) {
				continue
			}

			// Get the inner store for restoration (unwrap changelog-backed store). Window/session
			// inner stores are driven as a KeyValueStore<WindowedKey, V>: the restorer decodes each
			// changelog key with spec.keyCodec (windowedKeyCodec) into a WindowedKey and calls the
			// inner store's put/delete, which already key on WindowedKey.
			let innerStore: KeyValueStore<unknown, unknown> = store
			if (store instanceof ChangelogBackedKeyValueStore) {
				innerStore = store.innerStore
			} else if (store instanceof ChangelogBackedWindowStore || store instanceof ChangelogBackedSessionStore) {
				innerStore = store.innerStore as unknown as KeyValueStore<unknown, unknown>
			}

			const restorer = new ChangelogRestorer(spec.topicName, spec.keyCodec, spec.valueCodec, innerStore)

			try {
				const restoredCount = await restorer.restore(
					this.client,
					this.config.changelog?.restoration,
					partitions,
					this.changelogCheckpointStore ?? undefined
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
		const cloneMap = new Map<AnyProcessor, AnyProcessor>()

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
			retries: base.retries ?? 10,
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

	private enqueueEosTask<T>(worker: WorkerContext, fn: () => Promise<T>): Promise<T> {
		const run = worker.eosQueue.then(fn, fn)
		worker.eosQueue = run.then(
			() => {},
			() => {}
		)
		return run
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

		try {
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
		} catch (err) {
			await this.abortTransactionBatch(worker, err)
			throw err
		}
	}

	/**
	 * Begin a new transaction for the worker
	 */
	private async beginTransaction(worker: WorkerContext): Promise<void> {
		const producer = worker.producer

		// Begin the transaction using the producer's internal transaction start
		// We access the producer's transaction method but don't await its completion yet
		const started = await this.startProducerTransaction(producer)
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
					reject(err instanceof Error ? err : new Error(String(err)))
				}
			})
		})
	}

	/**
	 * Commit the current transaction batch
	 */
	private async commitTransactionBatch(worker: WorkerContext): Promise<void> {
		// Cancel any pending commit timer
		this.cancelCommitTimer(worker)

		if (!worker.transactionActive || !worker.activeTransaction || !worker.activeTransactionPromise) {
			return
		}

		const tx = worker.activeTransaction
		const txPromise = worker.activeTransactionPromise
		const offsets = Array.from(worker.pendingOffsets.values())

		try {
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

			// Complete the transaction by resolving the gate promise (commit happens inside producer.transaction()).
			const txWithResolve = tx as ActiveTransaction & { _resolve?: () => void }
			txWithResolve._resolve?.()

			// Wait for the transaction commit to succeed before checkpointing.
			await txPromise

			await this.flushChangelogCheckpoints(worker)
		} catch (err) {
			const txWithReject = tx as ActiveTransaction & { _reject?: (err: Error) => void }
			txWithReject._reject?.(err instanceof Error ? err : new Error(String(err)))
			await txPromise.catch(() => {})
			throw err
		} finally {
			// Reset state for next batch
			worker.pendingOffsets.clear()
			worker.pendingChangelogOffsets.clear()
			worker.activeTransaction = null
			worker.activeTransactionPromise = null
			worker.transactionActive = false
			worker.lastCommitTime = Date.now()
		}
	}

	private async abortTransactionBatch(worker: WorkerContext, error: unknown): Promise<void> {
		this.cancelCommitTimer(worker)

		if (!worker.transactionActive || !worker.activeTransaction || !worker.activeTransactionPromise) {
			worker.pendingOffsets.clear()
			worker.pendingChangelogOffsets.clear()
			worker.activeTransaction = null
			worker.activeTransactionPromise = null
			worker.transactionActive = false
			worker.lastCommitTime = Date.now()
			return
		}

		const tx = worker.activeTransaction as ActiveTransaction & { _reject?: (err: Error) => void }
		const txPromise = worker.activeTransactionPromise

		tx._reject?.(error instanceof Error ? error : new Error(String(error)))
		await txPromise.catch(() => {})

		worker.pendingOffsets.clear()
		worker.pendingChangelogOffsets.clear()
		worker.activeTransaction = null
		worker.activeTransactionPromise = null
		worker.transactionActive = false
		worker.lastCommitTime = Date.now()
	}

	private async flushChangelogCheckpoints(worker: WorkerContext): Promise<void> {
		if (!this.changelogCheckpointStore) {
			return
		}

		// Data is already durable post-EOS-commit; surface failures so a stuck checkpoint store doesn't silently cause restore-time over-replay.
		let anyFailure = false
		for (const checkpoint of worker.pendingChangelogOffsets.values()) {
			const checkpointOffset = checkpoint.offset + 1n
			try {
				await this.changelogCheckpointStore.set(checkpoint.topic, checkpoint.partition, checkpointOffset)
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
		const error = err instanceof Error ? err : new Error(String(err))
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
			this.enqueueEosTask(worker, () => this.commitTransactionBatch(worker)).catch(err => {
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
			this.currentState = 'REBALANCING'
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

export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}
