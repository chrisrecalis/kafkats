/**
 * flow() and the app (3.15): DSL entry points over the topology builder, and start/close through the AppLifecycle.
 * This build has no Kafka runtime yet (Stage 2): start() builds the topology, then fails.
 */
import { KafkaClient } from '@kafkats/client'
import type { CloseOptions, FlowApp, FlowConfig, StreamState, TopologyDescription } from '@/api/app.js'
import { StreamsError } from '@/api/errors.js'
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
import type { StateStore } from '@/state.js'
import { inMemory } from '@/state/memory.js'
import type { Topic } from '@/topic.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import { describeTopology } from '@/topology/describe.js'

/** Creates an app. Throws FlowConfigError for an invalid or removed config key. */
export function flow(config: FlowConfig): FlowApp {
	return new FlowAppImpl(config)
}

class FlowAppImpl implements FlowApp {
	readonly [INTERNAL]: AppInternals
	private readonly lifecycle = new AppLifecycle()
	private readonly builder: InternalTopologyBuilder
	private readonly dsl: DslContext

	constructor(config: FlowConfig) {
		const settings = resolveConfig(config)
		const { applicationId } = config
		const client = config.client instanceof KafkaClient ? config.client : new KafkaClient(config.client)
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

	start(): Promise<void> {
		return this.lifecycle.start(async () => {
			this.builder.build()
			throw new StreamsError('not available in this build: the task runtime starts in a later build')
		})
	}

	close(_options?: CloseOptions): Promise<void> {
		return this.lifecycle.close(async () => {})
	}

	state(): StreamState {
		return this.lifecycle.state
	}

	getError(): Error | null {
		return this.lifecycle.error
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
