/**
 * The description of a built topology (6.24): Kafka Streams' `TopologyDescription`, with its node order and its
 * `toString()` format (KS `InternalTopologyBuilder.describe`, `KS3/processor_internals_InternalTopologyBuilder.java:
 * 1594-2245`).
 */
import type {
	GlobalStoreDescription,
	SubtopologyDescription,
	TopologyDescription,
	TopologyNodeDescription,
} from '@/api/app.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { GlobalStoreDef, NodeDef, NodeName } from '@/topology/nodes.js'

export function describeTopology(topology: BuiltTopology): TopologyDescription {
	const subtopologies = topology.subtopologies.map(({ id, nodes }) => ({ id, nodes: describeNodes(nodes) }))
	const globalStores = topology.globalStores.map((store, i) => describeGlobalStore(store, subtopologies.length + i))
	return new Description(subtopologies, globalStores)
}

class Description implements TopologyDescription {
	constructor(
		readonly subtopologies: readonly SubtopologyDescription[],
		readonly globalStores: readonly GlobalStoreDescription[]
	) {}

	/** KS's layout verbatim, the extra space before the first sub-topology included. */
	toString(): string {
		const blocks = [
			...this.subtopologies.map(({ id, nodes }) => `Sub-topology: ${id}\n${nodes.map(line).join('')}\n`),
			...this.globalStores.map(
				({ id, source, processor }) =>
					`Sub-topology: ${id} for global store (will not generate tasks)\n${line(source)}${line(processor)}`
			),
		]
		return `Topologies:\n ${blocks.map(block => `  ${block}`).join('')}`
	}
}

/**
 * Nodes and their neighbours in KS `NodeComparator` order: larger subtrees first, then by name. A subtree's size counts
 * the paths from its root (a node reached twice counts twice). Parents precede their children in insertion order, so
 * one backwards pass computes every size.
 */
function describeNodes(nodes: readonly NodeDef[]): TopologyNodeDescription[] {
	const children = new Map<NodeName, NodeName[]>(nodes.map(node => [node.name, []]))
	for (const node of nodes) {
		for (const parent of parentsOf(node)) children.get(parent)?.push(node.name)
	}
	const sizes = new Map<NodeName, number>()
	const size = (name: NodeName): number => sizes.get(name) ?? 0
	for (const { name } of [...nodes].reverse()) {
		sizes.set(name, children.get(name)?.reduce((total, child) => total + size(child), 1) ?? 1)
	}
	const order = (a: NodeName, b: NodeName): number => size(b) - size(a) || compareNames(a, b)
	return nodes
		.map(node => describeNode(node, parentsOf(node).sort(order), children.get(node.name)?.sort(order) ?? []))
		.sort((a, b) => order(a.name, b.name))
}

function describeNode(node: NodeDef, predecessors: NodeName[], successors: NodeName[]): TopologyNodeDescription {
	const stores = node.kind === 'processor' ? [...node.stores] : []
	return { name: node.name, kind: node.kind, topics: topicsOf(node), stores, predecessors, successors }
}

/** A sink's topic is unknown when an extractor picks it per record. */
function topicsOf(node: NodeDef): string[] {
	if (node.kind === 'source') return [...node.topics]
	return node.kind === 'sink' && typeof node.topic === 'string' ? [node.topic] : []
}

function describeGlobalStore(
	{ topic, sourceName, processorName, store }: GlobalStoreDef,
	id: number
): GlobalStoreDescription {
	return {
		id,
		source: {
			name: sourceName,
			kind: 'source',
			topics: [topic],
			stores: [],
			predecessors: [],
			successors: [processorName],
		},
		processor: {
			name: processorName,
			kind: 'processor',
			topics: [],
			stores: [store.name],
			predecessors: [sourceName],
			successors: [],
		},
	}
}

function parentsOf(node: NodeDef): NodeName[] {
	return node.kind === 'source' ? [] : [...new Set(node.parents)]
}

/** `    <node>\n` as KS prints it; a sink whose topic an extractor picks shows `(topic extractor)`. */
function line(node: TopologyNodeDescription): string {
	const successors = `\n      --> ${names(node.successors)}`
	const predecessors = `\n      <-- ${names(node.predecessors)}`
	switch (node.kind) {
		case 'source':
			return `    Source: ${node.name} (topics: [${node.topics.join(', ')}])${successors}\n`
		case 'processor':
			return `    Processor: ${node.name} (stores: [${node.stores.join(', ')}])${successors}${predecessors}\n`
		case 'sink': {
			const [topic] = node.topics
			return `    Sink: ${node.name} (${topic === undefined ? 'topic extractor' : `topic: ${topic}`})${predecessors}\n`
		}
	}
}

function names(nodes: readonly NodeName[]): string {
	return nodes.length === 0 ? 'none' : nodes.join(', ')
}

/** Java's `String.compareTo`: by UTF-16 code units. */
function compareNames(a: string, b: string): number {
	return a < b ? -1 : a > b ? 1 : 0
}
