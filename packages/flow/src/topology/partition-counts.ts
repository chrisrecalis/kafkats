/**
 * Partition counts of every topic of a topology (4.1.3; KS RepartitionTopics + CopartitionedTopicsEnforcer). Pure:
 * the caller fetches the external counts and retries a SourceTopicNotFoundError while metadata catches up.
 */
import { TopologyError } from '@/api/errors.js'
import { CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { PartitionCounts } from '@/topology/nodes.js'

/**
 * `external`: counts from metadata (source, through and global topics). Repartition topics take their explicit
 * count, else the max count of the source topics of the subtopologies writing them (fixed point over chained
 * repartitions), and are coerced to the count of their co-partition group. Changelog topics get the task count of
 * their subtopology (max source count).
 * @throws SourceTopicNotFoundError, CoPartitioningError, TopologyError
 */
export function resolvePartitionCounts(
	topology: BuiltTopology,
	external: ReadonlyMap<string, number>
): PartitionCounts {
	const counts = new Map<string, number>()
	const explicit = new Map<string, number>()
	const upstream = new Map<string, Set<string>>()
	for (const spec of topology.internalTopics.values()) {
		if (spec.kind !== 'repartition') continue
		upstream.set(spec.name, new Set())
		if (spec.partitions !== undefined) explicit.set(spec.name, spec.partitions)
	}
	const isRepartition = (topic: string): boolean => upstream.has(topic)

	for (const subtopology of topology.subtopologies) {
		for (const topic of subtopology.sourceTopics) {
			if (isRepartition(topic)) continue
			counts.set(topic, externalCount(external, topic, subtopology.stores[0]?.name ?? 'topology'))
		}
		for (const topic of subtopology.sinkTopics) {
			for (const source of subtopology.sourceTopics) upstream.get(topic)?.add(source)
		}
	}
	for (const global of topology.globalStores) {
		counts.set(global.topic, externalCount(external, global.topic, global.store.name))
	}

	const pinned = new Map(explicit)
	do {
		resolveRepartitionCounts(upstream, pinned, counts)
	} while (coerceCopartitioned(topology.copartitionGroups, isRepartition, explicit, pinned, counts))

	for (const subtopology of topology.subtopologies) {
		const tasks = Math.max(...[...subtopology.sourceTopics].map(topic => counts.get(topic) ?? 0))
		for (const changelog of subtopology.changelogTopics.values()) counts.set(changelog, tasks)
	}
	return counts
}

function externalCount(external: ReadonlyMap<string, number>, topic: string, neededBy: string): number {
	const count = external.get(topic)
	if (count === undefined || count < 1) throw new SourceTopicNotFoundError(topic, neededBy)
	return count
}

/** Pinned topics keep their count; every other repartition topic is computed from its upstream topics. */
function resolveRepartitionCounts(
	upstream: ReadonlyMap<string, ReadonlySet<string>>,
	pinned: ReadonlyMap<string, number>,
	counts: Map<string, number>
): void {
	let pending: string[] = []
	for (const topic of upstream.keys()) {
		const count = pinned.get(topic)
		if (count === undefined) {
			counts.delete(topic)
			pending.push(topic)
		} else {
			counts.set(topic, count)
		}
	}
	while (pending.length > 0) {
		const unresolved: string[] = []
		for (const topic of pending) {
			const sources = [...(upstream.get(topic) ?? [])]
			if (sources.some(source => !counts.has(source))) unresolved.push(topic)
			else counts.set(topic, Math.max(1, ...sources.map(source => counts.get(source) ?? 0)))
		}
		if (unresolved.length === pending.length) {
			throw new TopologyError(
				`Repartition topic '${unresolved[0]}' depends on itself: its partition count is unknown`
			)
		}
		pending = unresolved
	}
}

/**
 * Enforces each co-partition group: external topics must agree (CoPartitioningError), explicit repartition counts must
 * match the group, and the other repartition topics of a group that disagrees are pinned to the group's count (all at
 * once, so a group never changes again). Returns true when something was pinned: downstream counts are recomputed.
 */
function coerceCopartitioned(
	groups: readonly ReadonlySet<string>[],
	isRepartition: (topic: string) => boolean,
	explicit: ReadonlyMap<string, number>,
	pinned: Map<string, number>,
	counts: ReadonlyMap<string, number>
): boolean {
	let changed = false
	for (const group of groups) {
		const target = groupCount(group, isRepartition, explicit, counts)
		const flexible: string[] = []
		for (const topic of group) {
			if (!isRepartition(topic)) continue
			const enforced = explicit.get(topic)
			if (enforced === undefined) flexible.push(topic)
			else if (enforced !== target) {
				throw new TopologyError(
					`Repartition topic '${topic}' has ${enforced} partitions, but the topics it is co-partitioned with ` +
						`have ${target}: ${[...group].join(', ')}`
				)
			}
		}
		if (flexible.some(topic => counts.get(topic) !== target)) {
			for (const topic of flexible) pinned.set(topic, target)
			changed = true
		}
	}
	return changed
}

/** The count of the external topics (all equal), else the explicit repartition count, else the max. */
function groupCount(
	group: ReadonlySet<string>,
	isRepartition: (topic: string) => boolean,
	explicit: ReadonlyMap<string, number>,
	counts: ReadonlyMap<string, number>
): number {
	const external = new Map<string, number>()
	const enforced = new Set<number>()
	let max = 0
	for (const topic of group) {
		const count = counts.get(topic) ?? 0
		if (!isRepartition(topic)) external.set(topic, count)
		const fixed = explicit.get(topic)
		if (fixed !== undefined) enforced.add(fixed)
		max = Math.max(max, count)
	}
	if (external.size > 0) {
		if (new Set(external.values()).size > 1) throw new CoPartitioningError(external)
		return external.values().next().value ?? 0
	}
	if (enforced.size > 1) {
		throw new TopologyError(
			`Co-partitioned repartition topics have different explicit partition counts: ${[...group].join(', ')}`
		)
	}
	return enforced.values().next().value ?? max
}
