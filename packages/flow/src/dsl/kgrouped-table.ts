import type { KGroupedTable } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import type { AggregateOptions } from '@/api/options.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { aggregate, count, reduce } from '@/dsl/ops/table-group-by.js'
import type { NodeName } from '@/topology/nodes.js'

export class KGroupedTableImpl<K, V> implements KGroupedTable<K, V> {
	constructor(
		readonly ctx: DslContext,
		/** The repartition-map node: the grouped rows, keyed by the new key (additions and retractions in this build). */
		readonly node: NodeName,
		/** Grouped codecs of the new key and value. */
		readonly format: StreamFormat,
		/** Grouped.name: one repartition topic shared by every aggregation of this grouping. */
		readonly name: string | undefined
	) {}

	/** The rows read back from the repartition topic of a named grouping, once an aggregation created it. */
	repartitioned: KStreamImpl<K, V> | undefined

	count(options?: AggregateOptions<K, number>): KTable<K, number> {
		return count(this, options)
	}

	reduce(
		adder: (aggregate: V, value: V) => V,
		subtractor: (aggregate: V, value: V) => V,
		options?: AggregateOptions<K, V>
	): KTable<K, V> {
		return reduce(this, adder, subtractor, options)
	}

	aggregate<A>(
		initializer: () => A,
		adder: (key: K, value: V, aggregate: A) => A,
		subtractor: (key: K, value: V, aggregate: A) => A,
		options?: AggregateOptions<K, A>
	): KTable<K, A> {
		return aggregate(this, initializer, adder, subtractor, options)
	}
}
