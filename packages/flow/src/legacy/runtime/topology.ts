import type { Codec } from '@/codec.js'
import type { KeyValueStore } from '@/state.js'
import type { SourceNode } from '@/legacy/processors/index.js'
import type { TopologyRegistry } from '@/legacy/streams/topology-builder.js'
import type { FlowConfig } from '@/legacy/types.js'

export type OffsetReset = 'earliest' | 'latest' | 'none'

export type RepartitionTopicSpec = {
	topicName: string
	/** Topics whose records are written into this repartition topic. */
	upstreamTopics: Set<string>
}

export type GlobalTableDef = {
	topic: string
	storeName: string
	/** Shared with the table's processors; re-pointed when the store is re-created on start. */
	storeRef: { store: KeyValueStore<unknown, unknown> | null }
	/** Codecs the store was created with. */
	storeKeyCodec: Codec<unknown>
	storeValueCodec: Codec<unknown>
	keyCodec: Codec<unknown>
	valueCodec: Codec<unknown>
	source: SourceNode<unknown, unknown>
}

/** What the DSL registered: sources, internal topics, co-partitioning constraints and global tables. */
export class Topology implements TopologyRegistry {
	private readonly sources = new Map<string, SourceNode<unknown, unknown>[]>()
	private readonly offsetResetByTopic = new Map<string, OffsetReset>()
	private readonly repartitions = new Map<string, RepartitionTopicSpec>()
	private readonly throughTopics = new Set<string>()
	private readonly copartitionGroups: Array<Set<string>> = []
	private readonly globals: GlobalTableDef[] = []
	private storeCounter = 0
	private repartitionCounter = 0

	constructor(private readonly config: FlowConfig) {}

	/** Get the next store ID and increment the counter */
	nextStoreId(): number {
		return this.storeCounter++
	}

	registerSource(topic: string, node: SourceNode<unknown, unknown>, offsetReset?: OffsetReset): void {
		const existing = this.sources.get(topic)
		if (existing) {
			existing.push(node)
		} else {
			this.sources.set(topic, [node])
		}

		if (offsetReset) {
			this.offsetResetByTopic.set(topic, offsetReset)
		}
	}

	/** Source nodes by topic, as registered by the DSL (cloned per worker). */
	get sourcesByTopic(): ReadonlyMap<string, readonly SourceNode<unknown, unknown>[]> {
		return this.sources
	}

	/**
	 * Source nodes fed by the consumer group, by topic. Without a cluster (mocked clients) global
	 * tables are fed like regular sources; otherwise GlobalTables reads them outside the group.
	 */
	groupSources(clusterAvailable: boolean): ReadonlyMap<string, readonly SourceNode<unknown, unknown>[]> {
		if (clusterAvailable || this.globals.length === 0) return this.sources
		const sources = new Map<string, SourceNode<unknown, unknown>[]>()
		for (const [topic, nodes] of this.sources) sources.set(topic, [...nodes])
		for (const table of this.globals) sources.set(table.topic, [...(sources.get(table.topic) ?? []), table.source])
		return sources
	}

	resolveOffsetReset(): OffsetReset | undefined {
		const values = new Set(this.offsetResetByTopic.values())
		if (values.size > 1) {
			throw new Error('Multiple offsetReset values detected. Use a separate flow for each offset policy.')
		}
		return values.size === 1 ? [...values][0] : this.config.consumer?.autoOffsetReset
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

	/** Register the source topics of a partition-scoped store, which share its state partitions. */
	addStoreCopartitionGroup(topics: Set<string>): void {
		this.copartitionGroups.push(topics)
	}

	get copartitioned(): ReadonlyArray<ReadonlySet<string>> {
		return this.copartitionGroups
	}

	/**
	 * Register an internal repartition topic fed by the given upstream topics and return its name.
	 * The topic is created at startup with the upstream partition count.
	 */
	createRepartitionTopic(name: string | undefined, upstreamTopics: Set<string>): string {
		const topicName = `${this.config.applicationId}-${name ?? `repartition-${this.repartitionCounter++}`}-repartition`
		if (!this.repartitions.has(topicName)) {
			this.repartitions.set(topicName, { topicName, upstreamTopics: new Set(upstreamTopics) })
		}
		return topicName
	}

	get repartitionTopics(): ReadonlyMap<string, RepartitionTopicSpec> {
		return this.repartitions
	}

	registerThroughTopic(topic: string): void {
		this.throughTopics.add(topic)
	}

	/** Repartition and through topics: written by the topology itself and read from the beginning. */
	isInternalTopic(topic: string): boolean {
		return this.repartitions.has(topic) || this.throughTopics.has(topic)
	}

	hasInternalTopics(): boolean {
		return this.repartitions.size > 0 || this.throughTopics.size > 0
	}

	addGlobalTable(def: GlobalTableDef): void {
		this.globals.push(def)
	}

	get globalTables(): readonly GlobalTableDef[] {
		return this.globals
	}
}
