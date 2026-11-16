import type { ProcessorContextImpl } from '@/processor/context.js'
import { ProcessorNode } from '@/processor/node.js'
import type { ProcessingErrorRoute } from '@/runtime/config.js'
import type { SourceDef } from '@/topology/nodes.js'

/**
 * Where a decoded input record enters the task's processor graph (KS `SourceNode`): it forwards the record to its
 * children. Decoding and timestamp extraction happen in the record queue, before the record is selected.
 */
export class SourceNode<K, V> extends ProcessorNode<K, V, K, V> {
	constructor(
		readonly def: SourceDef,
		context: ProcessorContextImpl<K, V>,
		onError: ProcessingErrorRoute
	) {
		super(def.name, { process: record => context.forward(record) }, context, onError)
	}
}
