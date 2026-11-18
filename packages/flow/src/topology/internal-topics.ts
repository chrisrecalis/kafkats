/**
 * The topics of a topology at start (4.1.3, 4.1.5): partition counts from metadata, then every repartition and
 * changelog topic validated against its count and created where missing. A missing source topic is looked for again
 * for a while, since a multi-broker cluster's metadata may not show a topic created moments ago yet.
 */
import { TimeoutError, type Logger } from '@kafkats/client'
import { DEFAULT_REPARTITION_CONFIGS } from '@/changelog/config.js'
import {
	ChangelogPartitionMismatchError,
	InternalTopicNotFoundError,
	SourceTopicNotFoundError,
} from '@/changelog/errors.js'
import type { AdminPort, Clock } from '@/runtime/io.js'
import { delay } from '@/runtime/signals.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { InternalTopicSpec, PartitionCounts } from '@/topology/nodes.js'
import { resolvePartitionCounts } from '@/topology/partition-counts.js'

/** How long a missing source topic is looked for, and how often. */
const SOURCE_TOPIC_WAIT_MS = 5000
const SOURCE_TOPIC_RETRY_MS = 500
/** How long created topics may take to show up in metadata, and how often they are looked for. */
const CREATED_TOPIC_WAIT_MS = 30_000
const CREATED_TOPIC_RETRY_MS = 100

/** `FlowConfig.changelog`. */
export interface InternalTopicOptions {
	/** Of internal topics whose store sets none. */
	readonly replicationFactor?: number
	/** Over the repartition topic defaults (changelog specs carry them already, 5.4). */
	readonly topicConfigs?: Readonly<Record<string, string>>
	/** Default true; false only validates: a missing internal topic is an error. */
	readonly autoCreate?: boolean
}

export interface TopicDeps {
	readonly admin: Pick<AdminPort, 'describeTopics' | 'createTopics'>
	readonly clock: Clock
	readonly logger: Logger
}

/**
 * The partition count of every topic of the topology, once its internal topics exist with their counts.
 * @throws SourceTopicNotFoundError, InternalTopicNotFoundError, CoPartitioningError, TopologyError,
 *   ChangelogPartitionMismatchError, TimeoutError
 */
export async function prepareTopics(
	topology: BuiltTopology,
	deps: TopicDeps,
	options: InternalTopicOptions,
	signal: AbortSignal
): Promise<PartitionCounts> {
	const { counts, existing } = await resolveCounts(topology, deps, signal)
	const missing: InternalTopicSpec[] = []
	for (const spec of topology.internalTopics.values()) {
		const actual = existing.get(spec.name)
		if (actual !== undefined) {
			checkCount(topology, counts, spec.name, actual)
		} else if (options.autoCreate === false || spec.validateOnly) {
			throw new InternalTopicNotFoundError(spec.name, spec.kind, counts.get(spec.name) ?? 0)
		} else {
			missing.push(withDefaults(spec, options))
		}
	}
	if (missing.length > 0) {
		deps.logger.info('creating internal topics', { topics: missing.map(spec => spec.name) })
		await deps.admin.createTopics(missing, counts, { signal })
		// Another instance may have created one first: its count is checked like that of any existing topic.
		const created = await whenVisible(missing, deps, signal)
		for (const [name, actual] of created) checkCount(topology, counts, name, actual)
	}
	return counts
}

async function resolveCounts(
	topology: BuiltTopology,
	{ admin, clock }: TopicDeps,
	signal: AbortSignal
): Promise<{ counts: PartitionCounts; existing: ReadonlyMap<string, number> }> {
	const topics = [...externalTopics(topology), ...topology.internalTopics.keys()]
	const deadline = clock.now() + SOURCE_TOPIC_WAIT_MS
	for (;;) {
		const existing = await admin.describeTopics(topics, { signal })
		try {
			return { counts: resolvePartitionCounts(topology, existing), existing }
		} catch (error) {
			if (!(error instanceof SourceTopicNotFoundError) || clock.now() >= deadline) throw error
		}
		await delay(clock, SOURCE_TOPIC_RETRY_MS, signal)
	}
}

/** The counts of the created topics once metadata shows every one of them. */
async function whenVisible(
	created: readonly InternalTopicSpec[],
	{ admin, clock }: TopicDeps,
	signal: AbortSignal
): Promise<ReadonlyMap<string, number>> {
	const topics = created.map(spec => spec.name)
	const deadline = clock.now() + CREATED_TOPIC_WAIT_MS
	for (;;) {
		const visible = await admin.describeTopics(topics, { signal })
		const pending = topics.filter(topic => !visible.has(topic))
		if (pending.length === 0) return visible
		if (clock.now() >= deadline) {
			throw new TimeoutError(
				`Internal topics did not show up in metadata: ${pending.join(', ')}`,
				CREATED_TOPIC_WAIT_MS
			)
		}
		await delay(clock, CREATED_TOPIC_RETRY_MS, signal)
	}
}

/** Source topics that are not repartition topics (user and through topics), then global topics. */
function externalTopics(topology: BuiltTopology): Set<string> {
	const sources = [...topology.groupTopics].filter(topic => !topology.internalTopics.has(topic))
	return new Set([...sources, ...topology.globalStores.map(global => global.topic)])
}

function checkCount(topology: BuiltTopology, counts: PartitionCounts, topic: string, actual: number): void {
	const expected = counts.get(topic)
	if (expected !== undefined && actual !== expected) {
		throw new ChangelogPartitionMismatchError(topic, expected, actual, feedingTopics(topology, topic))
	}
}

/** The source topics of the subtopologies writing an internal topic (repartition sinks, store changelogs). */
function feedingTopics(topology: BuiltTopology, topic: string): string[] {
	return topology.subtopologies
		.filter(s => s.sinkTopics.has(topic) || [...s.changelogTopics.values()].includes(topic))
		.flatMap(s => [...s.sourceTopics])
}

/**
 * Replication factor: the store's, else the app's. Configs: a changelog's are resolved by its store builder; a
 * repartition topic gets Kafka Streams' repartition configs under the app's topic configs (B3, BC20).
 */
function withDefaults(spec: InternalTopicSpec, options: InternalTopicOptions): InternalTopicSpec {
	return {
		...spec,
		replicationFactor: spec.replicationFactor ?? options.replicationFactor,
		configs:
			spec.kind === 'repartition'
				? { ...DEFAULT_REPARTITION_CONFIGS, ...options.topicConfigs, ...spec.configs }
				: spec.configs,
	}
}
