import type { BranchedKStream, KStream } from '@/api/kstream.js'
import type { BranchOptions } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { addBranch, defaultBranch, noDefaultBranch } from '@/dsl/ops/stateless.js'
import type { BranchRoute } from '@/operators/stateless.js'
import type { NodeName } from '@/topology/nodes.js'

/** The split builder (KS BranchedKStreamImpl): branches are added in order; the first matching predicate wins. */
export class BranchedKStreamImpl<K, V> implements BranchedKStream<K, V> {
	/** The result: each branch (or what its chain returned) by `<split name><branch name>`. */
	readonly branches: Record<string, KStream<K, V>> = {}
	/** Set once defaultBranch() or noDefaultBranch() returned the branches. */
	ended = false

	constructor(
		readonly stream: KStreamImpl<K, V>,
		/** The splitter node; its name prefixes every branch name. */
		readonly node: NodeName,
		/** The splitter's branches, in order; the default branch, if any, is last. */
		readonly routes: BranchRoute<K | null, V | null>[]
	) {}

	branch(predicate: (key: K | null, value: V | null) => boolean, options?: BranchOptions<K, V>): this {
		addBranch(this, predicate, options)
		return this
	}

	defaultBranch(options?: BranchOptions<K, V>): Record<string, KStream<K, V>> {
		return defaultBranch(this, options)
	}

	noDefaultBranch(): Record<string, KStream<K, V>> {
		return noDefaultBranch(this)
	}
}
