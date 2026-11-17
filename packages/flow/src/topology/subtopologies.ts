/**
 * Subtopologies: union-find over the node graph (KS InternalTopologyBuilder node groups, 4.1.2), numbering, and
 * the per-subtopology checks of 4.1.1 (sources of one topic, commit metadata keys).
 */
import { TopologyError } from '@/api/errors.js'
import type { Subtopology } from '@/topology/built-topology.js'
import type { GlobalStoreDef, NodeDef, NodeName, SourceDef, StoreBuilder } from '@/topology/nodes.js'

/** Broker default `offset.metadata.max.bytes` is 4096; the encoded metadata must stay well below it (4.8.5). */
export const MAX_COMMIT_METADATA_BYTES = 3000

export interface SubtopologyInput {
	/** Every node in insertion order, with its final store connections. */
	readonly nodes: readonly NodeDef[]
	/** Every non-global store in registration order. */
	readonly stores: readonly StoreBuilder[]
	readonly globalStores: readonly GlobalStoreDef[]
}

/**
 * Unites every processor and sink with its parents, processors sharing a store, and source nodes of the same topic
 * (a partition belongs to exactly one task; KS rejects such topologies, flow allows `stream(t)` with `table(t)`).
 * Sources are never united with sinks, so a topic between a sink and a source splits subtopologies.
 */
export function groupSubtopologies(input: SubtopologyInput): Subtopology[] {
	checkSources(input.nodes, input.globalStores)
	const { find, unite } = unionFind()
	const firstSourceOf = new Map<string, NodeName>()
	const firstUserOf = new Map<string, NodeName>()
	const uniteFirst = (first: Map<string, NodeName>, key: string, node: NodeName): void => {
		const existing = first.get(key)
		if (existing === undefined) first.set(key, node)
		else unite(existing, node)
	}
	for (const node of input.nodes) {
		if (node.kind === 'source') {
			for (const topic of node.topics) uniteFirst(firstSourceOf, topic, node.name)
			continue
		}
		for (const parent of node.parents) unite(parent, node.name)
		if (node.kind === 'processor') {
			for (const store of node.stores) uniteFirst(firstUserOf, store, node.name)
		}
	}

	const groups = new Map<NodeName, NodeDef[]>()
	for (const node of input.nodes) append(groups, find(node.name), node)
	const globalStores = input.globalStores.map(g => g.store.name)
	return [...groups.values()].map((nodes, id) => subtopology(id, nodes, input.stores, globalStores))
}

function subtopology(
	id: number,
	nodes: readonly NodeDef[],
	allStores: readonly StoreBuilder[],
	globalStores: readonly string[]
): Subtopology {
	const sourcesByTopic = new Map<string, SourceDef[]>()
	const repartitionSourceTopics = new Set<string>()
	const sinkTopics = new Set<string>()
	const storeNames = new Set<string>()
	for (const node of nodes) {
		if (node.kind === 'source') {
			for (const topic of node.topics) {
				append(sourcesByTopic, topic, node)
				if (node.internal === 'repartition') repartitionSourceTopics.add(topic)
			}
		} else if (node.kind === 'sink') {
			if (typeof node.topic === 'string') sinkTopics.add(node.topic)
		} else {
			for (const store of node.stores) storeNames.add(store)
		}
	}
	const stores = allStores.filter(store => storeNames.has(store.name))
	const changelogTopics = new Map<string, string>()
	for (const store of stores) {
		if (store.logging.enabled) changelogTopics.set(store.name, store.logging.topic)
	}
	return {
		id,
		nodes,
		sourcesByTopic,
		sourceTopics: new Set(sourcesByTopic.keys()),
		repartitionSourceTopics,
		sinkTopics,
		stores,
		changelogTopics,
		stateful: stores.length > 0,
		globalStores,
		metadataKeys: metadataKeys(id, stores),
	}
}

/** Sources of one topic share its record queue, so they must agree on how it is read (4.1.1). */
function checkSources(nodes: readonly NodeDef[], globalStores: readonly GlobalStoreDef[]): void {
	const firstSourceOf = new Map<string, SourceDef>()
	for (const node of nodes) {
		if (node.kind !== 'source') continue
		for (const topic of node.topics) {
			const first = firstSourceOf.get(topic)
			if (first === undefined) {
				firstSourceOf.set(topic, node)
			} else if (first.timestampExtractor !== node.timestampExtractor) {
				throw new TopologyError(
					`Sources '${first.name}' and '${node.name}' read topic '${topic}' with different timestamp extractors`
				)
			} else if (first.offsetReset !== node.offsetReset) {
				throw new TopologyError(
					`Sources '${first.name}' and '${node.name}' read topic '${topic}' with different offset reset policies ` +
						`(${first.offsetReset ?? 'default'} and ${node.offsetReset ?? 'default'})`
				)
			}
		}
	}
	for (const global of globalStores) {
		const source = firstSourceOf.get(global.topic)
		if (source !== undefined) {
			throw new TopologyError(
				`Topic '${global.topic}' is read by global store '${global.store.name}' and by source '${source.name}'; ` +
					'a global table cannot be streamed'
			)
		}
	}
}

/**
 * Commit metadata keys of the stores (4.8.5): `<store>` (the clock of the processor owning the store; allowed for
 * every store, since the topology does not know which processors keep one), `<store>.observed` (store clock) and
 * `<store>.emit` (emit-on-window-close progress). Keys must not collide, and their encoding must fit the broker limit.
 */
function metadataKeys(id: number, stores: readonly StoreBuilder[]): Set<string> {
	const owners = new Map<string, string>()
	for (const store of stores) {
		const keys = [store.name]
		if (store.clock) keys.push(`${store.name}.observed`)
		if (store.clock?.emitFinal) keys.push(`${store.name}.emit`)
		for (const key of keys) {
			const owner = owners.get(key)
			if (owner !== undefined) {
				throw new TopologyError(
					`State stores '${owner}' and '${store.name}' both use the commit metadata key '${key}'; rename one of them`
				)
			}
			owners.set(key, store.name)
		}
	}
	// base64( [magic][int64 partition time][int32 n] n x ([int32 key length][key][int64 value]) )
	let bytes = 1 + 8 + 4
	for (const key of owners.keys()) bytes += 4 + Buffer.byteLength(key) + 8
	const encoded = 4 * Math.ceil(bytes / 3)
	if (encoded > MAX_COMMIT_METADATA_BYTES) {
		throw new TopologyError(
			`The commit metadata of subtopology ${id} would take ${encoded} bytes (limit ${MAX_COMMIT_METADATA_BYTES}); ` +
				`use shorter or fewer state stores: ${stores.map(s => s.name).join(', ')}`
		)
	}
	return new Set(owners.keys())
}

function append<K, V>(map: Map<K, V[]>, key: K, value: V): void {
	const list = map.get(key)
	if (list) list.push(value)
	else map.set(key, [value])
}

function unionFind(): { find(name: NodeName): NodeName; unite(a: NodeName, b: NodeName): void } {
	const parent = new Map<NodeName, NodeName>()
	const find = (name: NodeName): NodeName => {
		const up = parent.get(name)
		if (up === undefined) return name
		const root = find(up)
		parent.set(name, root)
		return root
	}
	return {
		find,
		unite(a, b) {
			const rootA = find(a)
			const rootB = find(b)
			if (rootA !== rootB) parent.set(rootB, rootA)
		},
	}
}
