import type { KGroupedTable } from '@/api/grouped.js'
import type { KStream, KeyValue, ValueMapper, ValueMapperWithKey } from '@/api/kstream.js'
import type { KTable } from '@/api/ktable.js'
import type {
	Grouped,
	Joined,
	NamedOptions,
	SuppressOptions,
	TableOptions,
	TransformValuesOptions,
} from '@/api/options.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'
import { transformValues } from '@/dsl/ops/process.js'
import { toStream } from '@/dsl/ops/stateless.js'
import { suppress } from '@/dsl/ops/suppress.js'
import { groupBy } from '@/dsl/ops/table-group-by.js'
import { tableJoin } from '@/dsl/ops/table-join.js'
import * as tableOps from '@/dsl/ops/table-ops.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import type { ValueTransformerWithKeySupplier } from '@/processor/api.js'
import type { NodeName } from '@/topology/nodes.js'

export class KTableImpl<K, V> implements KTable<K, V> {
	constructor(
		readonly ctx: DslContext,
		/** The node whose output (`Change<V>` records) this table is. */
		readonly node: NodeName,
		readonly format: StreamFormat,
		/** Processor supplier, old-value propagation and value getter of the node. */
		readonly table: KTableNode<K, V>
	) {}

	filter(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V> {
		return tableOps.filter(this, predicate, options)
	}

	filterNot(predicate: (key: K, value: V) => boolean, options?: TableOptions<K, V>): KTable<K, V> {
		return tableOps.filterNot(this, predicate, options)
	}

	mapValues<VR>(mapper: ValueMapper<V, VR | null>, options?: TableOptions<K, VR>): KTable<K, VR> {
		return tableOps.mapValues(this, (_key, value) => mapper(value), options)
	}

	mapValuesWithKey<VR>(mapper: ValueMapperWithKey<K, V, VR | null>, options?: TableOptions<K, VR>): KTable<K, VR> {
		return tableOps.mapValues(this, mapper, options)
	}

	transformValues<VR>(
		supplier: ValueTransformerWithKeySupplier<K, V, VR>,
		options?: TransformValuesOptions<K, VR>
	): KTable<K, VR> {
		return transformValues(this, supplier, options)
	}

	toStream<KR>(mapper: (key: K, value: V | null) => KR, options?: NamedOptions): KStream<KR, V>
	toStream(options?: NamedOptions): KStream<K, V>
	toStream<KR>(
		mapperOrOptions?: ((key: K, value: V | null) => KR) | NamedOptions,
		options?: NamedOptions
	): KStream<K | KR, V> {
		return toStream(this, mapperOrOptions, options)
	}

	groupBy<KR, VR>(
		selector: (key: K, value: V) => KeyValue<KR, VR>,
		options?: Grouped<KR, VR>
	): KGroupedTable<KR, VR> {
		return groupBy(this, selector, options)
	}

	join<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V, otherValue: VO) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR> {
		return tableJoin(this, 'inner', other, joiner, options)
	}

	leftJoin<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V, otherValue: VO | null) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR> {
		return tableJoin(this, 'left', other, joiner, options)
	}

	outerJoin<VO, VR>(
		other: KTable<K, VO>,
		joiner: (value: V | null, otherValue: VO | null) => VR,
		options?: TableOptions<K, VR> | Joined<K, V, VO>
	): KTable<K, VR> {
		return tableJoin(this, 'outer', other, joiner, options)
	}

	suppress(options: SuppressOptions): KTable<K, V> {
		return suppress(this, options)
	}
}
