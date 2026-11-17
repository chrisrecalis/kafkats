/**
 * Table-table joins: inner, left and outer (6.13). Each side's processor reads the other table through its value
 * getter, whose stores it is connected to, so both tables end in one subtopology and their topics must be
 * co-partitioned (4.1.3); tables are never repartitioned for a join. The merger is the result table, materialized when
 * Materialized names a store. The deprecated Joined options are accepted for their name; as today, their codecs and
 * changelog setting are not used.
 */
import { TopologyError } from '@/api/errors.js'
import type { KTable } from '@/api/ktable.js'
import type { Joined, TableOptions } from '@/api/options.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import { derivedFormat, materializedStore } from '@/dsl/ops/table-ops.js'
import { KTableJoin, type TableJoiner, type TableJoinKind } from '@/operators/ktable-join.js'

const OPERATOR: Readonly<Record<TableJoinKind, string>> = { inner: 'join()', left: 'leftJoin()', outer: 'outerJoin()' }

/** The joiner receives null for the missing side of left and outer results. */
export function tableJoin<K, V, VO, VR>(
	table: KTableImpl<K, V>,
	kind: TableJoinKind,
	other: KTable<K, VO>,
	joiner: (value: V, otherValue: VO) => VR,
	options: TableOptions<K, VR> | Joined<K, V, VO> = {}
): KTable<K, VR> {
	const operator = OPERATOR[kind]
	if (!(other instanceof KTableImpl) || other.ctx !== table.ctx) {
		throw new TopologyError(`${operator} needs a table of the same app`)
	}
	const right = other as KTableImpl<K, VO>
	const { ctx } = table
	const { name } = options
	const materialized = 'materialized' in options ? options.materialized : undefined
	const format = derivedFormat(table.format, materialized, false)
	const storeName = materializedStore(ctx, materialized, format, operator)
	const join = new KTableJoin(table.table, right.table, kind, joiner as TableJoiner<V, VO, VR>, storeName)

	// Kafka Streams' naming order: the merger, then this side, then the other side.
	const merger = ctx.builder.nodeName(NodePrefix.TABLE_MERGE, name)
	const named = (suffix: string) => (name === undefined ? undefined : `${name}-${suffix}`)
	const thisSide = ctx.processor(
		NodePrefix.TABLE_JOIN_THIS,
		named('join-this'),
		[table.node],
		join.thisSide,
		right.table.valueGetter.storeNames
	)
	const otherSide = ctx.processor(
		NodePrefix.TABLE_JOIN_OTHER,
		named('join-other'),
		[right.node],
		join.otherSide,
		table.table.valueGetter.storeNames
	)
	const stores = storeName === undefined ? [] : [storeName]
	ctx.processor(NodePrefix.TABLE_MERGE, merger, [thisSide, otherSide], join.supplier, stores)
	return new KTableImpl<K, VR>(ctx, merger, format, join)
}
