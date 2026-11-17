import type { KafkaClient } from '@kafkats/client'
import { ChangelogPartitionMismatchError, CoPartitioningError, SourceTopicNotFoundError } from '@/legacy/changelog.js'
import type { FlowConfig } from '@/legacy/types.js'
import type { AppStatus } from '@/legacy/runtime/status.js'
import type { StoreRegistry } from '@/legacy/runtime/store-registry.js'
import type { Topology } from '@/legacy/runtime/topology.js'

/** How long start() waits for a missing source topic to show up in metadata before failing. */
const SOURCE_TOPIC_VISIBILITY_WAIT_MS = 5_000

type TopicToCreate = {
	name: string
	numPartitions: number
	replicationFactor?: number
	configs?: Record<string, string>
}

export type InternalTopicsDeps = {
	client: KafkaClient
	config: FlowConfig
	topology: Topology
	stores: StoreRegistry
	status: AppStatus
}

/**
 * Validate the topology against the cluster, tolerating metadata that has not caught up yet.
 * On a multi-broker cluster a topic created moments ago may be unknown to the broker that
 * answers the metadata request, so a missing source topic is re-checked for a short while
 * before it is reported.
 *
 * Returns false when validation was skipped because no cluster is reachable.
 */
export async function validateTopicsOnceVisible(deps: InternalTopicsDeps): Promise<boolean> {
	const deadline = Date.now() + SOURCE_TOPIC_VISIBILITY_WAIT_MS
	for (;;) {
		try {
			return await validateAndCreateInternalTopics(deps)
		} catch (err) {
			if (!(err instanceof SourceTopicNotFoundError) || Date.now() >= deadline || deps.status.closing) throw err
			await deps.status.sleepUnlessClosing(500)
		}
	}
}

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
async function validateAndCreateInternalTopics({
	client,
	config,
	topology,
	stores,
}: InternalTopicsDeps): Promise<boolean> {
	const repartitionTopics = topology.repartitionTopics
	const userTopics = new Set<string>()
	for (const topic of topology.sourcesByTopic.keys()) {
		if (!repartitionTopics.has(topic)) userTopics.add(topic)
	}
	for (const table of topology.globalTables) userTopics.add(table.topic)
	const internalTopics = new Set<string>(repartitionTopics.keys())
	for (const spec of stores.changelogSpecs.values()) {
		internalTopics.add(spec.topicName)
		for (const topic of spec.sourceTopics) {
			if (!repartitionTopics.has(topic)) userTopics.add(topic)
		}
	}
	for (const spec of repartitionTopics.values()) {
		for (const topic of spec.upstreamTopics) {
			if (!repartitionTopics.has(topic)) userTopics.add(topic)
		}
	}

	if (internalTopics.size === 0 && topology.globalTables.length === 0 && topology.copartitioned.length === 0) {
		// Stateless topology: nothing to validate, but report whether a cluster is reachable.
		return client.isConnected
	}

	let metadata
	try {
		metadata = await client.getMetadata([...userTopics, ...internalTopics])
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
		const repartition = repartitionTopics.get(topic)
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
	for (const group of topology.copartitioned) {
		const counts = new Map<string, number>()
		for (const topic of group) counts.set(topic, partitionsOf(topic, 'co-partitioned operation'))
		if (new Set(counts.values()).size > 1) {
			throw new CoPartitioningError(counts)
		}
	}

	const topicsToCreate: TopicToCreate[] = []
	const validateOnly = config.changelog?.autoCreate === false

	for (const spec of repartitionTopics.values()) {
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
			replicationFactor: config.changelog?.replicationFactor,
		})
	}

	for (const [storeName, spec] of stores.changelogSpecs) {
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
				replicationFactor: spec.replicationFactor ?? config.changelog?.replicationFactor,
				configs: spec.configs,
			})
		}
	}

	if (topicsToCreate.length > 0) {
		await client.createTopics(topicsToCreate)
		await waitForTopics(
			client,
			topicsToCreate.map(t => t.name)
		)
	}

	return true
}

/** Wait until newly created topics are visible in metadata with leaders, so consumers can subscribe. */
async function waitForTopics(client: KafkaClient, topics: string[]): Promise<void> {
	const deadline = Date.now() + 30_000
	for (;;) {
		try {
			const metadata = await client.getMetadata(topics)
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
