/** The built topology: subtopologies, internal topics, co-partition groups and per-topic lookups (3.3, 4.1). */
import type { TopicPartition } from '@kafkats/client'
import { IllegalStateError, TopologyError } from '@/api/errors.js'
import type {
	GlobalStoreDef,
	InternalTopicSpec,
	NodeDef,
	PartitionCounts,
	ResetPolicy,
	SourceDef,
	StoreBuilder,
} from '@/topology/nodes.js'

/** A connected part of the processor graph (KS node group); one task per partition (`<id>_<p>`). */
export interface Subtopology {
	/** KS numbering: order of first appearance of its nodes in insertion order, so upstream comes first. */
	readonly id: number
	/** Topological (insertion) order, with the final store connections and null-key marking. */
	readonly nodes: readonly NodeDef[]
	/** Source nodes of each topic, in registration order. */
	readonly sourcesByTopic: ReadonlyMap<string, readonly SourceDef[]>
	readonly sourceTopics: ReadonlySet<string>
	/** Repartition topics read by this subtopology: purged after commits. */
	readonly repartitionSourceTopics: ReadonlySet<string>
	/** Static sink topics (a TopicNameExtractor's topics are known only per record). */
	readonly sinkTopics: ReadonlySet<string>
	/** Stores connected to its processors, in registration order. */
	readonly stores: readonly StoreBuilder[]
	/** Store name -> changelog topic, for the stores with logging enabled. */
	readonly changelogTopics: ReadonlyMap<string, string>
	/** stores.length > 0 */
	readonly stateful: boolean
	/** Global stores: readable by every processor without being connected (KS). */
	readonly globalStores: readonly string[]
	/** Keys this subtopology may persist in commit metadata (4.8.5); anything else is dropped on seeding. */
	readonly metadataKeys: ReadonlySet<string>
}

export interface BuiltTopology {
	readonly applicationId: string
	readonly subtopologies: readonly Subtopology[]
	readonly globalStores: readonly GlobalStoreDef[]
	/** Repartition topics (registration order), then changelog topics (subtopology and store order). */
	readonly internalTopics: ReadonlyMap<string, InternalTopicSpec>
	/** Topics that must end with equal partition counts: the source topics of each stateful subtopology (4.1.3). */
	readonly copartitionGroups: readonly ReadonlySet<string>[]
	/** Subscribed by the group consumer: every source topic. */
	readonly groupTopics: ReadonlySet<string>
	/** Partition counts preloaded at start. */
	readonly staticSinkTopics: ReadonlySet<string>
	subtopologyOf(topic: string): Subtopology | undefined
	/** The source's offsetReset ?? appDefault; repartition and through topics always 'earliest'. */
	resetPolicy(topic: string, appDefault: ResetPolicy): ResetPolicy
	/** Expected input partitions of a task: `(t, partition)` for each source topic `t` with more partitions. */
	inputPartitions(subtopology: Subtopology, partition: number, counts: PartitionCounts): TopicPartition[]
}

/** What the builder registered, grouped into subtopologies. */
export interface TopologyParts {
	readonly applicationId: string
	readonly subtopologies: readonly Subtopology[]
	readonly globalStores: readonly GlobalStoreDef[]
	/** Decorated repartition topic -> explicit partition count (Repartitioned.numberOfPartitions). */
	readonly repartitionTopics: ReadonlyMap<string, number | undefined>
	readonly throughTopics: ReadonlySet<string>
}

export function createBuiltTopology(parts: TopologyParts): BuiltTopology {
	const { subtopologies, repartitionTopics, throughTopics } = parts
	const bySourceTopic = new Map<string, Subtopology>()
	for (const subtopology of subtopologies) {
		for (const topic of subtopology.sourceTopics) bySourceTopic.set(topic, subtopology)
	}
	return {
		applicationId: parts.applicationId,
		subtopologies,
		globalStores: parts.globalStores,
		internalTopics: internalTopics(subtopologies, repartitionTopics),
		copartitionGroups: subtopologies.filter(s => s.stateful && s.sourceTopics.size > 1).map(s => s.sourceTopics),
		groupTopics: new Set(bySourceTopic.keys()),
		staticSinkTopics: new Set(subtopologies.flatMap(s => [...s.sinkTopics])),
		subtopologyOf: topic => bySourceTopic.get(topic),
		resetPolicy(topic, appDefault) {
			if (repartitionTopics.has(topic) || throughTopics.has(topic)) return 'earliest'
			// Sources of one topic agree on the policy (checked at build).
			return bySourceTopic.get(topic)?.sourcesByTopic.get(topic)?.[0]?.offsetReset ?? appDefault
		},
		inputPartitions(subtopology, partition, counts) {
			const partitions: TopicPartition[] = []
			for (const topic of subtopology.sourceTopics) {
				const count = counts.get(topic)
				if (count === undefined) throw new IllegalStateError(`No partition count for topic '${topic}'`)
				if (partition < count) partitions.push({ topic, partition })
			}
			return partitions
		},
	}
}

/**
 * Changelog configs are the store's `logging.configs`, which the store builders resolve completely (defaults <-
 * `FlowConfig.changelog.topicConfigs` <- the store's own, 5.4). Repartition configs are empty: the creation path
 * applies `DEFAULT_REPARTITION_CONFIGS` <- `FlowConfig.changelog.topicConfigs` to them, and the default replication
 * factor and `autoCreate` to both kinds (4.1.5).
 */
function internalTopics(
	subtopologies: readonly Subtopology[],
	repartitionTopics: ReadonlyMap<string, number | undefined>
): Map<string, InternalTopicSpec> {
	const specs = new Map<string, InternalTopicSpec>()
	for (const [name, partitions] of repartitionTopics) {
		specs.set(name, { name, kind: 'repartition', partitions, configs: {}, validateOnly: false })
	}
	const changelogOwners = new Map<string, string>()
	for (const store of subtopologies.flatMap(s => s.stores)) {
		if (!store.logging.enabled) continue
		const { topic: name, configs, replicationFactor } = store.logging
		if (specs.has(name)) {
			const owner = changelogOwners.get(name)
			throw new TopologyError(
				`State store '${store.name}' logs to '${name}', which is already ` +
					(owner === undefined ? 'a repartition topic' : `the changelog topic of state store '${owner}'`)
			)
		}
		changelogOwners.set(name, store.name)
		specs.set(name, { name, kind: 'changelog', configs, replicationFactor, validateOnly: false })
	}
	return specs
}
