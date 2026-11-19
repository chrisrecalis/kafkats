import type { KGroupedStream } from '@/api/grouped.js'
import type {
	BranchedKStream,
	KeyValueMapper,
	KStream,
	KeyValue,
	ValueJoiner,
	ValueJoinerWithKey,
	ValueMapper,
	ValueMapperWithKey,
} from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type {
	Grouped,
	Joined,
	Materialized,
	NamedOptions,
	ProcessOptions,
	Produced,
	Repartitioned,
	SplitOptions,
	StreamJoined,
} from '@/api/options.js'
import type { DslContext } from '@/dsl/context.js'
import type { StreamFormat } from '@/dsl/format.js'
import { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { globalJoin } from '@/dsl/ops/global-join.js'
import { groupBy, groupByKey } from '@/dsl/ops/grouping.js'
import { process, processValues } from '@/dsl/ops/process.js'
import { repartition, through } from '@/dsl/ops/repartition.js'
import * as stateless from '@/dsl/ops/stateless.js'
import { streamStreamJoin, type KeyedStreamJoiner, type StreamStreamJoinArgs } from '@/dsl/ops/stream-stream-join.js'
import { streamTableJoin } from '@/dsl/ops/stream-table-join.js'
import { toTable } from '@/dsl/ops/table-source.js'
import { to } from '@/dsl/ops/to.js'
import type { JoinWindows } from '@/dsl/windows.js'
import type { FixedKeyProcessorSupplier, ProcessorSupplier } from '@/processor/api.js'
import type { NodeName, TopicNameExtractor } from '@/topology/nodes.js'

/** `(joiner, options?)` of a table; `(keySelector, joiner, options?)`, or the deprecated `(joiner)`, of a global table. */
type TableJoinArgs<K, V, VO, J> =
	| [joiner: J, options?: Joined<K, V, VO>]
	| [keySelector: KeyValueMapper<K | null, V, unknown>, joiner: J, options?: NamedOptions]

type JoinArgs<K, V, VO, VR> =
	| StreamStreamJoinArgs<K, V, VO, ValueJoiner<V, VO, VR>>
	| TableJoinArgs<K, V, VO, ValueJoiner<V, VO | null, VR>>

/** `(joiner, windows, options?)` of a stream. */
type StreamJoinWithKeyArgs<K, V, VO, J> = [joiner: J, windows: JoinWindows, options?: StreamJoined<K, V, VO>]

type JoinWithKeyArgs<K, V, VO, VR> =
	| StreamJoinWithKeyArgs<K, V, VO, ValueJoinerWithKey<K, V, VO, VR>>
	| TableJoinArgs<K, V, VO, ValueJoinerWithKey<K, V, VO, VR>>

type LeftJoinWithKeyArgs<K, V, VO, VR> =
	| StreamJoinWithKeyArgs<K, V, VO, ValueJoinerWithKey<K | null, V, VO | null, VR>>
	| TableJoinArgs<K, V, VO, ValueJoinerWithKey<K | null, V, VO | null, VR>>

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
		return this.joinWith('inner', other, args, false)
	}

	leftJoin<VO, VR>(
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		...args: JoinArgs<K, V, VO, VR>
	): KStream<K, VR> {
		return this.joinWith('left', other, args, false)
	}

	joinWithKey<VO, VR>(
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		...args: JoinWithKeyArgs<K, V, VO, VR>
	): KStream<K, VR> {
		return this.joinWith('inner', other, args, true)
	}

	leftJoinWithKey<VO, VR>(
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		...args: LeftJoinWithKeyArgs<K, V, VO, VR>
	): KStream<K, VR> {
		return this.joinWith('left', other, args, true)
	}

	outerJoin<VO, VR>(
		other: KStream<K, VO>,
		...[joiner, ...rest]: StreamStreamJoinArgs<K, V, VO, ValueJoiner<V | null, VO | null, VR>>
	): KStream<K, VR> {
		return streamStreamJoin(this, 'outer', 'outerJoin()', other, withoutKey(joiner), ...rest)
	}

	outerJoinWithKey<VO, VR>(
		other: KStream<K, VO>,
		joiner: ValueJoinerWithKey<K | null, V | null, VO | null, VR>,
		windows: JoinWindows,
		options?: StreamJoined<K, V, VO>
	): KStream<K, VR> {
		return streamStreamJoin(this, 'outer', 'outerJoinWithKey()', other, joiner, windows, options)
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

	/**
	 * Dispatches a join on the kind of the other side. `withKey`: the joiner takes the record's key first
	 * (joinWithKey, leftJoinWithKey); otherwise it is called with the values only. Errors name the method called.
	 */
	private joinWith<VO, VR>(
		kind: 'inner' | 'left',
		other: KStream<K, VO> | KTable<K, VO> | GlobalKTable<unknown, VO>,
		args: readonly unknown[],
		withKey: boolean
	): KStream<K, VR> {
		type Joiner = KeyedStreamJoiner<K, V, VO, VR>
		const operator = `${kind === 'inner' ? 'join' : 'leftJoin'}${withKey ? 'WithKey' : ''}()`
		const keyed = (joiner: unknown): Joiner =>
			withKey ? (joiner as Joiner) : withoutKey(joiner as ValueJoiner<V | null, VO | null, VR>)
		if (other instanceof GlobalKTableImpl) {
			// The deprecated `(globalTable, joiner)` looks up the record key.
			const [keySelector, joiner, options] = args.length === 1 ? [(key: K | null) => key, args[0]] : args
			return globalJoin(
				this,
				kind,
				operator,
				other,
				keySelector as KeyValueMapper<K | null, V, unknown>,
				keyed(joiner),
				options as NamedOptions | undefined
			)
		}
		if (other instanceof KTableImpl) {
			const [joiner, options] = args
			return streamTableJoin(this, kind, operator, other, keyed(joiner), options as Joined<K, V, VO> | undefined)
		}
		const [joiner, ...rest] = args as StreamStreamJoinArgs<K, V, VO, unknown>
		return streamStreamJoin(this, kind, operator, other as KStream<K, VO>, keyed(joiner), ...rest)
	}
}

/** A joiner of the values only, called as the join processors call a joiner. */
function withoutKey<K, V, VO, VR>(joiner: ValueJoiner<V | null, VO | null, VR>): KeyedStreamJoiner<K, V, VO, VR> {
	return (_key, value, otherValue) => joiner(value, otherValue)
}
