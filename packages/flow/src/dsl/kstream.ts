import type { KGroupedStream } from '@/api/grouped.js'
import type { BranchedKStream, KStream, KeyValue, ValueMapper, ValueMapperWithKey } from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type {
	Grouped,
	Materialized,
	NamedOptions,
	ProcessOptions,
	Produced,
	Repartitioned,
	SplitOptions,
} from '@/api/options.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'
import { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { globalJoin, type GlobalJoinArgs } from '@/dsl/ops/global-join.js'
import { groupBy, groupByKey } from '@/dsl/ops/grouping.js'
import { process, processValues } from '@/dsl/ops/process.js'
import { repartition, through } from '@/dsl/ops/repartition.js'
import * as stateless from '@/dsl/ops/stateless.js'
import { streamStreamJoin, type StreamStreamJoinArgs } from '@/dsl/ops/stream-stream-join.js'
import { streamTableJoin, type StreamTableJoinArgs } from '@/dsl/ops/stream-table-join.js'
import { toTable } from '@/dsl/ops/table-source.js'
import { to } from '@/dsl/ops/to.js'
import type { FixedKeyProcessorSupplier, ProcessorSupplier } from '@/processor/api.js'
import type { NodeName, TopicNameExtractor } from '@/topology/nodes.js'

type JoinArgs<K, V, VO, VR> =
	| StreamStreamJoinArgs<K, V, VO, VR>
	| StreamTableJoinArgs<K, V, VO, VR>
	| GlobalJoinArgs<K, V, unknown, VO, VR>

export class KStreamImpl<K, V> implements KStream<K, V> {
	constructor(
		readonly ctx: DslContext,
		/** The node whose output this stream is. */
		readonly node: NodeName,
		readonly format: StreamFormat,
		/** Keys changed upstream: a key-based operation repartitions first. */
		readonly repartitionRequired: boolean
	) {}

	filter(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V> {
		return stateless.filter(this, predicate, options)
	}

	filterNot(predicate: (key: K | null, value: V | null) => boolean, options?: NamedOptions): KStream<K, V> {
		return stateless.filterNot(this, predicate, options)
	}

	map<KR, VR>(mapper: (key: K | null, value: V | null) => KeyValue<KR, VR>, options?: NamedOptions): KStream<KR, VR> {
		return stateless.map(this, mapper, options)
	}

	mapValues<VR>(mapper: ValueMapper<V | null, VR>, options?: NamedOptions): KStream<K, VR> {
		return stateless.mapValues(this, (_key, value) => mapper(value), options)
	}

	mapValuesWithKey<VR>(mapper: ValueMapperWithKey<K | null, V | null, VR>, options?: NamedOptions): KStream<K, VR> {
		return stateless.mapValues(this, mapper, options)
	}

	flatMap<KR, VR>(
		mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
		options?: NamedOptions
	): KStream<KR, VR> {
		return stateless.flatMap(this, mapper, options)
	}

	flatMapValues<VR>(mapper: ValueMapper<V | null, Iterable<VR>>, options?: NamedOptions): KStream<K, VR> {
		return stateless.flatMapValues(this, (_key, value) => mapper(value), options)
	}

	flatMapValuesWithKey<VR>(
		mapper: ValueMapperWithKey<K | null, V | null, Iterable<VR>>,
		options?: NamedOptions
	): KStream<K, VR> {
		return stateless.flatMapValues(this, mapper, options)
	}

	peek(action: (key: K | null, value: V | null) => void, options?: NamedOptions): KStream<K, V> {
		return stateless.peek(this, action, options)
	}

	foreach(action: (key: K | null, value: V | null) => void, options?: NamedOptions): void {
		stateless.foreach(this, action, options)
	}

	selectKey<KR>(mapper: (key: K | null, value: V | null) => KR, options?: NamedOptions): KStream<KR, V> {
		return stateless.selectKey(this, mapper, options)
	}

	merge(other: KStream<K, V>, options?: NamedOptions): KStream<K, V> {
		return stateless.merge(this, other, options)
	}

	split(options?: SplitOptions): BranchedKStream<K, V> {
		return stateless.split(this, options)
	}

	branch(...predicates: ((key: K | null, value: V | null) => boolean)[]): KStream<K, V>[] {
		return stateless.branch(this, predicates)
	}

	to(topic: string | TopicNameExtractor<K | null, V | null>, options?: Produced<K, V>): void {
		to(this, topic, options)
	}

	through(topic: string, options?: Produced<K, V>): KStream<K, V> {
		return through(this, topic, options)
	}

	repartition(options?: Repartitioned<K, V>): KStream<K, V> {
		return repartition(this, options)
	}

	toTable(options?: Materialized<K, V> & NamedOptions): KTable<K, V> {
		return toTable(this, options)
	}

	groupBy<KR>(selector: (key: K | null, value: V | null) => KR, options?: Grouped<KR, V>): KGroupedStream<KR, V> {
		return groupBy(this, selector, options)
	}

	groupByKey(options?: Grouped<K, V>): KGroupedStream<K, V> {
		return groupByKey(this, options)
	}

	join<VO, VR>(
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		...args: JoinArgs<K, V, VO, VR>
	): KStream<K, VR> {
		return this.joinWith('inner', other, args)
	}

	leftJoin<VO, VR>(
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		...args: JoinArgs<K, V, VO, VR>
	): KStream<K, VR> {
		return this.joinWith('left', other, args)
	}

	outerJoin<VO, VR>(other: KStream<K, VO>, ...args: StreamStreamJoinArgs<K, V, VO, VR>): KStream<K, VR> {
		return streamStreamJoin(this, 'outer', other, ...args)
	}

	process<KR, VR>(
		supplier: ProcessorSupplier<K | null, V | null, KR, VR>,
		options?: ProcessOptions
	): KStream<KR, VR> {
		return process(this, supplier, options)
	}

	processValues<VR>(
		supplier: FixedKeyProcessorSupplier<K | null, V | null, VR>,
		options?: ProcessOptions
	): KStream<K, VR> {
		return processValues(this, supplier, options)
	}

	/** Dispatches a join on the kind of the other side. */
	private joinWith<VO, VR>(
		kind: 'inner' | 'left',
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		args: JoinArgs<K, V, VO, VR>
	): KStream<K, VR> {
		if (other instanceof GlobalKTableImpl) {
			return globalJoin(this, kind, other, ...(args as GlobalJoinArgs<K, V, unknown, VO, VR>))
		}
		if (other instanceof KTableImpl) {
			return streamTableJoin(this, kind, other, ...(args as StreamTableJoinArgs<K, V, VO, VR>))
		}
		return streamStreamJoin(this, kind, other as KStream<K, VO>, ...(args as StreamStreamJoinArgs<K, V, VO, VR>))
	}
}
