import { randomUUID } from 'node:crypto'
import { KafkaClient, type Logger } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { buffer as bufferCodec } from '@/codec.js'
import type { KeyValueStore } from '@/state.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import { recordContextStorage } from '@/runtime/context.js'
import { Checkpoints } from '@/runtime/checkpoints.js'
import { toError } from '@/runtime/errors.js'
import { GlobalTables } from '@/runtime/global-tables.js'
import { validateTopicsOnceVisible } from '@/runtime/internal-topics.js'
import { RecordOutput } from '@/runtime/output.js'
import { Restoration } from '@/runtime/restoration.js'
import { StartPositions } from '@/runtime/start-positions.js'
import { AppStatus } from '@/runtime/status.js'
import { StoreRegistry } from '@/runtime/store-registry.js'
import { Topology } from '@/runtime/topology.js'
import { WorkerPool } from '@/runtime/worker-pool.js'

// Public types
import type { StreamState, FlowConfig, FlowApp, KStream, KTable, Topic, Consumed, Materialized } from '@/types.js'

// Re-export public types for backward compatibility
export * from '@/types.js'
export * from '@/windows.js'
export { topic } from '@/topic.js'

// Internal processor types
import { SourceNode, type StreamFormat } from '@/processors/index.js'
import { TableStateNode } from '@/processors/table.js'

// Stream implementations
import { KStreamImpl } from '@/streams/kstream.js'
import { KTableImpl } from '@/streams/ktable.js'
import type { TopologyBuilder } from '@/streams/topology-builder.js'

const DEFAULT_BUFFER_CODEC: Codec<Buffer> = bufferCodec()

/**
 * Tracks the record currently being processed (worker + source partition).
 * Used by state stores and changelog writers to pick the right state partition and transaction.
 */
export const workerContextStorage = recordContextStorage

/**
 * A flow application: the DSL entry points and the lifecycle of the runtime that executes the
 * topology (see `runtime/`).
 */
class FlowAppImpl implements FlowApp {
	private readonly client: KafkaClient
	private readonly ownsClient: boolean
	/** Internal instance identity used to keep derived transactional IDs unique across replicas. */
	private readonly processId = randomUUID()
	private readonly eosEnabled: boolean
	private readonly logger: Logger
	private readonly status = new AppStatus()
	private readonly topology: Topology
	private readonly stores: StoreRegistry
	private readonly checkpoints: Checkpoints
	private readonly restoration: Restoration
	private readonly startPositions: StartPositions
	private readonly globalTables: GlobalTables
	private readonly workers: WorkerPool
	private readonly output: RecordOutput
	/** The view of the runtime that the streams and tables built from this app work with. */
	private readonly builder: TopologyBuilder
	private startPromise: Promise<void> | null = null
	private closePromise: Promise<void> | null = null

	constructor(readonly config: FlowConfig) {
		this.eosEnabled = (config.processingGuarantee ?? 'at_least_once') === 'exactly_once'
		if (config.client instanceof KafkaClient) {
			this.client = config.client
			this.ownsClient = false
		} else {
			this.client = new KafkaClient(config.client)
			this.ownsClient = true
		}
		const provider = config.stateStoreProvider ?? new InMemoryStateStoreProvider()
		this.logger = this.client.cluster
			.getLogger()
			.child({ component: 'flow', applicationId: config.applicationId, processId: this.processId })

		const { client, logger, status, processId } = this
		this.topology = new Topology(config)
		this.checkpoints = new Checkpoints(provider, status, logger)
		this.stores = new StoreRegistry(config, provider, this.topology, this.checkpoints)
		this.restoration = new Restoration({
			client,
			config,
			stores: this.stores,
			checkpoints: this.checkpoints,
			status,
			logger,
			isStatePartitionOwned: (sourceTopics, partition) =>
				this.workers.ownsStatePartition(sourceTopics, partition),
		})
		this.startPositions = new StartPositions(client, config.applicationId, this.topology, status, logger)
		this.globalTables = new GlobalTables({
			client,
			config,
			processId,
			topology: this.topology,
			stores: this.stores,
			status,
			logger,
			onFatal: error => this.halt(error),
		})
		this.workers = new WorkerPool({
			client,
			config,
			eosEnabled: this.eosEnabled,
			processId,
			topology: this.topology,
			stores: this.stores,
			restoration: this.restoration,
			startPositions: this.startPositions,
			checkpoints: this.checkpoints,
			status,
			logger,
			onFatal: error => this.halt(error),
		})
		this.output = new RecordOutput(client, this.eosEnabled)
		this.builder = {
			stores: this.stores,
			topology: this.topology,
			output: this.output,
			stream: (topic, options) => this.stream(topic, options),
		}
	}

	// ------------------------------------------------------------------------------------------
	// DSL
	// ------------------------------------------------------------------------------------------

	stream<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: Consumed<K, V>): KStream<K, V> {
		const resolved = resolveSource(source, options)
		const node = new SourceNode<K, V>(resolved.topic, resolved.format)
		this.topology.registerSource(resolved.topic, node as SourceNode<unknown, unknown>, resolved.offsetReset)
		return new KStreamImpl<K, V>(this.builder, node, resolved.format, new Set([resolved.topic]))
	}

	table<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V> {
		const resolved = resolveSource(source, options)
		const sourceNode = new SourceNode<K, V>(resolved.topic, resolved.format)
		this.topology.registerSource(resolved.topic, sourceNode as SourceNode<unknown, unknown>, resolved.offsetReset)

		// Create state store for table - prefer materialized codecs over consumed codecs
		const keyCodec = options?.materialized?.key ?? resolved.format.keyCodec
		const valueCodec = options?.materialized?.value ?? resolved.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('table() requires both key and value codecs for stateful operations')
		}

		const storeName = options?.materialized?.storeName ?? `table-${resolved.topic}-${this.topology.nextStoreId()}`
		const sourceTopics = new Set([resolved.topic])
		const store = this.stores.getOrCreateKeyValueStore<K, V>(storeName, {
			keyCodec,
			valueCodec,
			changelog: options?.materialized?.changelog,
			sourceTopics,
		})
		const storeRef = { store }

		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		sourceNode.connect(tableStateNode)

		return new KTableImpl<K, V>(this.builder, tableStateNode, resolved.format, storeRef, sourceTopics)
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
		const resolved = resolveSource(source, options)
		const sourceNode = new SourceNode<K, V>(resolved.topic, resolved.format)

		const keyCodec = options?.materialized?.key ?? resolved.format.keyCodec
		const valueCodec = options?.materialized?.value ?? resolved.format.valueCodec
		if (!keyCodec || !valueCodec) {
			throw new Error('globalTable() requires both key and value codecs for stateful operations')
		}

		const storeName =
			options?.materialized?.storeName ?? `global-table-${resolved.topic}-${this.topology.nextStoreId()}`
		const store = this.stores.createGlobalStore<K, V>(storeName, keyCodec, valueCodec)
		const storeRef = { store }

		const tableStateNode = new TableStateNode<K, V>(storeName, storeRef)
		sourceNode.connect(tableStateNode)

		this.topology.addGlobalTable({
			topic: resolved.topic,
			storeName,
			storeRef: storeRef as { store: KeyValueStore<unknown, unknown> | null },
			storeKeyCodec: keyCodec as Codec<unknown>,
			storeValueCodec: valueCodec as Codec<unknown>,
			keyCodec: resolved.format.keyCodec as Codec<unknown>,
			valueCodec: resolved.format.valueCodec as Codec<unknown>,
			source: sourceNode as SourceNode<unknown, unknown>,
		})

		return new KTableImpl<K, V>(this.builder, tableStateNode, resolved.format, storeRef, new Set(), true)
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
		const status = this.status
		status.beginStart()
		this.closePromise = null

		try {
			await this.client.connect()
			status.assertNotClosing()

			// Validate topology against the cluster and create internal topics
			// (returns false if skipped because no cluster is reachable, e.g. mocked clients).
			status.clusterAvailable = await validateTopicsOnceVisible({
				client: this.client,
				config: this.config,
				topology: this.topology,
				stores: this.stores,
				status,
			})
			status.assertNotClosing()

			if (status.clusterAvailable) {
				// App-scoped stores and global tables must be complete before any record is processed.
				await this.restoration.restoreAppScopedStores()
				status.assertNotClosing()
				await this.globalTables.start()
				status.assertNotClosing()
			}

			const topics = [...this.topology.groupSources(status.clusterAvailable).keys()]
			if (topics.length > 0) {
				await this.workers.start(topics)
			}

			status.assertNotClosing()
			status.started = true
			if (status.state !== 'ERROR') {
				status.state = 'RUNNING'
			}
		} catch (err) {
			const error = toError(err)
			const wasClosing = status.closing
			await this.shutdown()
			// Not started: surface the failure and allow start() to be called again.
			status.lastError = error
			status.state = wasClosing ? 'STOPPED' : 'ERROR'
			throw error
		}
	}

	close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise
		}
		if (this.status.state === 'STOPPED' || (this.status.state === 'CREATED' && !this.startPromise)) {
			this.status.state = 'STOPPED'
			return Promise.resolve()
		}

		this.closePromise = (async () => {
			// Prevent a buffered record from opening a new transaction after the final commit
			// has been queued but before the consumer is stopped.
			this.status.beginShutdown()

			// Commit any pending transactions before stopping.
			await this.workers.commitAll()

			// If start() is still in flight it observes `closing`, fails, and shuts down itself.
			if (this.startPromise && !this.status.started) {
				await this.startPromise.catch(() => {})
			}
			await this.shutdown()
			this.status.state = 'STOPPED'
			this.startPromise = null
		})()
		return this.closePromise
	}

	/**
	 * A failure that processing must not continue past: a worker failed with an error that is not
	 * retried, or a global table can no longer be followed. While starting, start() fails with it.
	 * Afterwards every worker is stopped for good and the app reports 'ERROR' until it is closed
	 * (what Kafka Streams does by default: SHUTDOWN_CLIENT).
	 */
	private halt(error: Error): void {
		const status = this.status
		if (!status.started) {
			status.lastError = error
			status.failStartup(error)
			return
		}
		status.halt(error)
		this.workers.stop()
	}

	/** Stop and release everything. Safe to call on a partially started app. */
	private async shutdown(): Promise<void> {
		this.status.beginShutdown()

		this.workers.stop()
		this.globalTables.stopConsumers()

		await this.workers.settled()
		await this.globalTables.settled()
		await this.restoration.settled()

		await this.workers.disconnect()
		this.globalTables.clear()

		this.restoration.cancelPrune()
		this.checkpoints.clearInProcess()
		this.startPositions.clear()

		await this.stores.close().catch(err => {
			this.logger.warn('state store provider close failed', { error: toError(err).message })
		})

		if (this.ownsClient) {
			await this.client.disconnect().catch(() => {})
		}
		this.status.started = false
	}

	state(): StreamState {
		return this.status.state
	}

	getError(): Error | null {
		return this.status.lastError
	}
}

function resolveSource<K, V>(source: string | Topic<K, V>, options?: Consumed<K, V>) {
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

export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}
