/** The description of a built topology in Kafka Streams' format (6.24, P1). */
import type { TopologyDescription } from '@/api/app.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { BuiltTopology } from '@/topology/built-topology.js'

export function describeTopology(_topology: BuiltTopology): TopologyDescription {
	return notImplemented('FlowApp.describe')
}
