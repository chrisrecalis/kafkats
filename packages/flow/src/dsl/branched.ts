import type { BranchedKStream, KStream } from '@/api/kstream.js'
import type { BranchOptions, SplitOptions } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { addBranch, defaultBranch, noDefaultBranch } from '@/dsl/ops/stateless.js'

/** The split builder: branches are added in order; the first matching predicate wins. */
export class BranchedKStreamImpl<K, V> implements BranchedKStream<K, V> {
	constructor(
		readonly stream: KStreamImpl<K, V>,
		readonly options: SplitOptions | undefined
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
