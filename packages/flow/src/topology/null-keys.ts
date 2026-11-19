/**
 * Null keys on repartition topics (4.1.6, KF12; KS NodesWithRelaxedNullKeyJoinDownstream): a repartition sink keeps
 * null keys iff a processor with `relaxedNullKeyJoin` (left/outer stream-stream, left stream-table, left global join)
 * is downstream of it, through any number of nodes and further repartition topics; every other repartition sink
 * drops them.
 */
import type { NodeDef, NodeName, SinkDef } from '@/topology/nodes.js'

/** The nodes with every repartition sink marked `nullKeys: 'keep' | 'drop'`; other nodes unchanged. */
export function markNullKeys(nodes: readonly NodeDef[]): NodeDef[] {
	const children = new Map<NodeName, NodeDef[]>()
	const repartitionReaders = new Map<string, NodeDef[]>()
	for (const node of nodes) {
		if (node.kind !== 'source') {
			for (const parent of node.parents) append(children, parent, node)
		} else if (node.internal === 'repartition') {
			for (const topic of node.topics) append(repartitionReaders, topic, node)
		}
	}
	const successors = (node: NodeDef): readonly NodeDef[] => {
		if (node.kind !== 'sink') return children.get(node.name) ?? []
		return isRepartitionSink(node) && typeof node.topic === 'string'
			? (repartitionReaders.get(node.topic) ?? [])
			: []
	}

	const reaches = new Map<NodeName, boolean>()
	const reachesRelaxedJoin = (node: NodeDef): boolean => {
		const known = reaches.get(node.name)
		if (known !== undefined) return known
		reaches.set(node.name, false) // a cycle through repartition topics adds nothing
		const result = successors(node).some(
			child => (child.kind === 'processor' && child.relaxedNullKeyJoin === true) || reachesRelaxedJoin(child)
		)
		reaches.set(node.name, result)
		return result
	}

	return nodes.map(node =>
		isRepartitionSink(node) ? { ...node, nullKeys: reachesRelaxedJoin(node) ? 'keep' : 'drop' } : node
	)
}

function isRepartitionSink(node: NodeDef): node is SinkDef {
	return node.kind === 'sink' && node.internal === 'repartition'
}

function append<K, V>(map: Map<K, V[]>, key: K, value: V): void {
	const list = map.get(key)
	if (list) list.push(value)
	else map.set(key, [value])
}
