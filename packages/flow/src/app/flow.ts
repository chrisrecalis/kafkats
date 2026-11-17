/**
 * flow() and the app (3.15): DSL entry points over the topology builder, and start/close through the AppLifecycle:
 * every start builds the topology once and runs it on a FlowRuntime of its own (4.2), which close() stops (4.11).
 */
import { randomUUID } from 'node:crypto'
import { KafkaClient } from '@kafkats/client'
import type { CloseOptions, FlowApp, FlowConfig, StreamState, TopologyDescription } from '@/api/app.js'
import type { KStream } from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type { Consumed } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import { resolveConfig } from '@/app/config.js'
import { INTERNAL, type AppInternals } from '@/app/internal.js'
import { AppLifecycle } from '@/app/lifecycle.js'
import { DslContext } from '@/dsl/context.js'
import { addStateStore } from '@/dsl/ops/process.js'
import { globalTable, table, type TableConsumed } from '@/dsl/ops/table-source.js'
import { createKafkaIO } from '@/runtime/kafka-io.js'
import { FlowRuntime } from '@/runtime/runtime.js'
import type { StateStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import type { Topic } from '@/topic.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import { describeTopology } from '@/topology/describe.js'

/** close() returns within this by default (D13); a failed start releases what it started within it too. */
const DEFAULT_CLOSE_TIMEOUT_MS = 20_000

/** Creates an app. Throws FlowConfigError for an invalid or removed config key. */
export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}

class FlowAppImpl implements FlowApp {
	readonly [INTERNAL]: AppInternals
	private readonly lifecycle = new AppLifecycle()
	private readonly builder: InternalTopologyBuilder
	private readonly dsl: DslContext
	private readonly client: KafkaClient
	/** A client the app created is disconnected when a start ends; the user's own client is not. */
	private readonly ownsClient: boolean
	/** The runtime of the current or last start. */
	private runtime: FlowRuntime | undefined

	constructor(private readonly config: FlowConfig) {
		const settings = resolveConfig(config)
		const { applicationId } = config
		this.ownsClient = !(config.client instanceof KafkaClient)
		const client = config.client instanceof KafkaClient ? config.client : new KafkaClient(config.client)
		this.client = client
		this.builder = new InternalTopologyBuilder(applicationId)
		this.dsl = new DslContext(this.builder, { applicationId, topicConfigs: config.changelog?.topicConfigs })
		this[INTERNAL] = {
			build: () => this.builder.build(),
			settings,
			stateStoreProvider: config.stateStoreProvider ?? inMemory(),
			logger: client.cluster.getLogger().child({ component: 'flow', applicationId }),
		}
	}

	stream<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: Consumed<K, V>): KStream<K, V> {
		const [topic, consumed] = resolveSource(source, options)
		return this.dsl.stream(topic, consumed)
	}

	table<K = Buffer, V = Buffer>(source: string | Topic<K, V>, options?: TableConsumed<K, V>): KTable<K, V> {
		return table(this.dsl, ...resolveSource(source, options))
	}

	globalTable<K = Buffer, V = Buffer>(
		source: string | Topic<K, V>,
		options?: TableConsumed<K, V>
	): GlobalKTable<K, V> {
		return globalTable(this.dsl, ...resolveSource(source, options))
	}

	addStateStore(definition: StoreDefinition<StateStore>): this {
		addStateStore(this.dsl, definition)
		return this
	}

	describe(): TopologyDescription {
		return describeTopology(this.builder.build())
	}

	/**
	 * Builds the topology (later DSL calls throw), then starts a runtime and resolves once every task of the first
	 * assignment is RUNNING (4.2). A failed start releases what it started and may be retried.
	 */
	start(): Promise<void> {
		return this.lifecycle.start(async () => {
			const runtime = this.createRuntime(this.builder.build())
			this.runtime = runtime
			try {
				await runtime.start(runtime.shutdown.signal)
			} catch (error) {
				await runtime.stop(Date.now() + DEFAULT_CLOSE_TIMEOUT_MS)
				throw error
			}
		})
	}

	/** Commits what was processed and releases everything within `timeoutMs` (4.11); a start in flight fails. */
	close(options: CloseOptions = {}): Promise<void> {
		const timeoutMs = options.timeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS
		return this.lifecycle.close(async () => {
			await this.runtime?.stop(Date.now() + timeoutMs)
		})
	}

	state(): StreamState {
		return this.lifecycle.state
	}

	getError(): Error | null {
		return this.lifecycle.error
	}

	/** One start's runtime: a new process id (D11), its checkpoints, state directory and Kafka IO (4.2 step 5). */
	private createRuntime(topology: BuiltTopology): FlowRuntime {
		const { settings, stateStoreProvider, logger } = this[INTERNAL]
		const { client, ownsClient, lifecycle } = this
		const config = { ...settings, processId: randomUUID() }
		const checkpoints = new Checkpoints(stateStoreProvider.getChangelogCheckpointStore?.(), error => {
			lifecycle.recordError(error)
			logger.warn('writing a changelog checkpoint failed: restoration replays more', { error: error.message })
		})
		return new FlowRuntime({
			topology,
			config,
			io: createKafkaIO({ client, config: this.config, settings: config, topology }),
			connection: {
				connect: () => client.connect(),
				disconnect: () => (ownsClient ? client.disconnect() : Promise.resolve()),
			},
			lifecycle,
			stateDirectory: new StateDirectory(config.applicationId, stateStoreProvider, checkpoints, logger),
			checkpoints,
			topics: this.config.changelog ?? {},
			logger,
		})
	}
}

/** The topic name and its codecs: the options' first, then the Topic's. */
function resolveSource<K, V, C extends Consumed<K, V>>(
	source: string | Topic<K, V>,
	options: C | undefined
): [string, C] {
	if (typeof source === 'string') {
		return [source, options ?? ({} as C)]
	}
	return [source.name, { ...options, key: options?.key ?? source.key, value: options?.value ?? source.value } as C]
}
