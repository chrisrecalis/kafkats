import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { KStream } from '@/api/kstream.js'
import { codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import { BranchedKStreamImpl } from '@/dsl/branched.js'
import { GlobalKTableImpl } from '@/dsl/global-ktable.js'
import { KGroupedStreamImpl } from '@/dsl/kgrouped-stream.js'
import { KGroupedTableImpl } from '@/dsl/kgrouped-table.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { KTableImpl } from '@/dsl/ktable.js'
import { SessionWindowedKStreamImpl } from '@/dsl/session-windowed-kstream.js'
import { SlidingWindowedKStreamImpl } from '@/dsl/sliding-windowed-kstream.js'
import { TimeWindowedKStreamImpl } from '@/dsl/time-windowed-kstream.js'
import { JoinWindows, SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import * as aggregate from '@/dsl/ops/aggregate.js'
import * as globalJoin from '@/dsl/ops/global-join.js'
import * as grouping from '@/dsl/ops/grouping.js'
import * as processOps from '@/dsl/ops/process.js'
import * as repartition from '@/dsl/ops/repartition.js'
import * as sliding from '@/dsl/ops/sliding-aggregate.js'
import * as stateless from '@/dsl/ops/stateless.js'
import * as streamStreamJoin from '@/dsl/ops/stream-stream-join.js'
import * as streamTableJoin from '@/dsl/ops/stream-table-join.js'
import * as suppress from '@/dsl/ops/suppress.js'
import * as tableGroupBy from '@/dsl/ops/table-group-by.js'
import * as tableJoin from '@/dsl/ops/table-join.js'
import * as tableOps from '@/dsl/ops/table-ops.js'
import * as tableSource from '@/dsl/ops/table-source.js'
import * as to from '@/dsl/ops/to.js'
import * as windowed from '@/dsl/ops/windowed-aggregate.js'

vi.mock('@/dsl/ops/aggregate.js')
vi.mock('@/dsl/ops/global-join.js')
vi.mock('@/dsl/ops/grouping.js')
vi.mock('@/dsl/ops/process.js')
vi.mock('@/dsl/ops/repartition.js')
vi.mock('@/dsl/ops/sliding-aggregate.js')
vi.mock('@/dsl/ops/stateless.js')
vi.mock('@/dsl/ops/stream-stream-join.js')
vi.mock('@/dsl/ops/stream-table-join.js')
vi.mock('@/dsl/ops/suppress.js')
vi.mock('@/dsl/ops/table-group-by.js')
vi.mock('@/dsl/ops/table-join.js')
vi.mock('@/dsl/ops/table-ops.js')
vi.mock('@/dsl/ops/table-source.js')
vi.mock('@/dsl/ops/to.js')
vi.mock('@/dsl/ops/windowed-aggregate.js')

/** Window instances without the factories (their bodies belong to a later work package). */
function instance<T extends object>(type: { prototype: T }): T {
	return Object.create(type.prototype) as T
}

const ctx = {} as DslContext
const fn = vi.fn()
const options = { name: 'op' }

describe('DSL shells delegate every method to its operator builder', () => {
	let stream: KStreamImpl<string, number>
	let table: KTableImpl<string, number>

	beforeEach(() => {
		vi.clearAllMocks()
		stream = new KStreamImpl(ctx, 'source', {}, false)
		table = new KTableImpl(ctx, 'table', {}, {} as KTableNode<string, number>)
	})

	it('KStream stateless operators, sinks, repartitioning, grouping and the Processor API', () => {
		const other = new KStreamImpl<string, number>(ctx, 'other', {}, false)
		const supplier = () => ({ process: fn })
		const cases: [() => unknown, unknown, unknown[]][] = [
			[() => stream.filter(fn, options), stateless.filter, [fn, options]],
			[() => stream.filterNot(fn, options), stateless.filterNot, [fn, options]],
			[() => stream.map(() => ['k', 1], options), stateless.map, [expect.any(Function), options]],
			[() => stream.mapValues(fn, options), stateless.mapValues, [expect.any(Function), options]],
			[() => stream.mapValuesWithKey(fn, options), stateless.mapValues, [fn, options]],
			[() => stream.flatMap(() => [], options), stateless.flatMap, [expect.any(Function), options]],
			[() => stream.flatMapValues(() => [], options), stateless.flatMapValues, [expect.any(Function), options]],
			[() => stream.flatMapValuesWithKey(fn, options), stateless.flatMapValues, [fn, options]],
			[() => stream.peek(fn, options), stateless.peek, [fn, options]],
			[() => stream.foreach(fn, options), stateless.foreach, [fn, options]],
			[() => stream.selectKey(fn, options), stateless.selectKey, [fn, options]],
			[() => stream.merge(other, options), stateless.merge, [other, options]],
			[() => stream.split(options), stateless.split, [options]],
			[() => stream.branch(fn, fn), stateless.branch, [[fn, fn]]],
			[() => stream.to('out', options), to.to, ['out', options]],
			[() => stream.through('through', options), repartition.through, ['through', options]],
			[() => stream.repartition(options), repartition.repartition, [options]],
			[() => stream.toTable(options), tableSource.toTable, [options]],
			[() => stream.groupBy(fn, options), grouping.groupBy, [fn, options]],
			[() => stream.groupByKey(options), grouping.groupByKey, [options]],
			[() => stream.process(supplier, options), processOps.process, [supplier, options]],
			[() => stream.processValues(supplier, options), processOps.processValues, [supplier, options]],
		]
		for (const [call, op, args] of cases) {
			call()
			expect(op).toHaveBeenCalledWith(stream, ...args)
		}
	})

	it('KStream joins dispatch on the other side: stream, table or global table', () => {
		const other = new KStreamImpl<string, number>(ctx, 'other', {}, false)
		const global = new GlobalKTableImpl<string, number>(ctx, 'global-store')
		const windows = instance(JoinWindows)
		const inner = (value: number, otherValue: number) => value + otherValue
		const left = (value: number, otherValue: number | null) => value + (otherValue ?? 0)
		const outer = (value: number | null, otherValue: number | null) => (value ?? 0) + (otherValue ?? 0)
		const selector = (key: string | null) => key
		// Users hold the public interface, whose overloads type the arguments.
		const api: KStream<string, number> = stream

		api.join(other, inner, windows, options)
		expect(streamStreamJoin.streamStreamJoin).toHaveBeenCalledWith(stream, 'inner', other, inner, windows, options)
		api.leftJoin(other, left, { within: windows })
		expect(streamStreamJoin.streamStreamJoin).toHaveBeenCalledWith(stream, 'left', other, left, { within: windows })
		api.outerJoin(other, outer, windows)
		expect(streamStreamJoin.streamStreamJoin).toHaveBeenCalledWith(stream, 'outer', other, outer, windows)

		api.join(table, inner, options)
		expect(streamTableJoin.streamTableJoin).toHaveBeenCalledWith(stream, 'inner', table, inner, options)
		api.leftJoin(table, left)
		expect(streamTableJoin.streamTableJoin).toHaveBeenCalledWith(stream, 'left', table, left)

		api.join(global, selector, inner, options)
		expect(globalJoin.globalJoin).toHaveBeenCalledWith(stream, 'inner', global, selector, inner, options)
		api.leftJoin(global, left)
		expect(globalJoin.globalJoin).toHaveBeenCalledWith(stream, 'left', global, left)
	})

	it('KTable operators', () => {
		const other = new KTableImpl<string, number>(ctx, 'other', {}, {} as KTableNode<string, number>)
		const supplier = () => ({ transform: fn })
		const suppressed = { until: 'window-close' } as const
		const cases: [() => unknown, unknown, unknown[]][] = [
			[() => table.filter(fn, options), tableOps.filter, [fn, options]],
			[() => table.filterNot(fn, options), tableOps.filterNot, [fn, options]],
			[() => table.mapValues(fn, options), tableOps.mapValues, [expect.any(Function), options]],
			[() => table.mapValuesWithKey(fn, options), tableOps.mapValues, [fn, options]],
			[() => table.transformValues(supplier, options), processOps.transformValues, [supplier, options]],
			[() => table.toStream(options), stateless.toStream, [options, undefined]],
			[() => table.toStream(fn, options), stateless.toStream, [fn, options]],
			[() => table.groupBy(() => ['k', 1], options), tableGroupBy.groupBy, [expect.any(Function), options]],
			[() => table.join(other, fn, options), tableJoin.tableJoin, ['inner', other, fn, options]],
			[() => table.leftJoin(other, fn), tableJoin.tableJoin, ['left', other, fn, undefined]],
			[() => table.outerJoin(other, fn, options), tableJoin.tableJoin, ['outer', other, fn, options]],
			[() => table.suppress(suppressed), suppress.suppress, [suppressed]],
		]
		for (const [call, op, args] of cases) {
			call()
			expect(op).toHaveBeenCalledWith(table, ...args)
		}
	})

	it('grouped streams aggregate and dispatch windowedBy on the window kind', () => {
		const grouped = new KGroupedStreamImpl(stream, 'grouping')
		grouped.count(options)
		expect(aggregate.count).toHaveBeenCalledWith(grouped, options)
		grouped.reduce(fn, options)
		expect(aggregate.reduce).toHaveBeenCalledWith(grouped, fn, options)
		grouped.aggregate(fn, fn, options)
		expect(aggregate.aggregate).toHaveBeenCalledWith(grouped, fn, fn, options)

		const time = grouped.windowedBy(instance(TimeWindows))
		const session = grouped.windowedBy(instance(SessionWindows))
		const slidingWindows = grouped.windowedBy(instance(SlidingWindows))
		expect(time).toBeInstanceOf(TimeWindowedKStreamImpl)
		expect(session).toBeInstanceOf(SessionWindowedKStreamImpl)
		expect(slidingWindows).toBeInstanceOf(SlidingWindowedKStreamImpl)

		const cases: [() => unknown, unknown, unknown, unknown[]][] = [
			[() => time.count(options), windowed.timeWindowedCount, time, [options]],
			[() => time.reduce(fn, options), windowed.timeWindowedReduce, time, [fn, options]],
			[() => time.aggregate(fn, fn, options), windowed.timeWindowedAggregate, time, [fn, fn, options]],
			[() => session.count(options), windowed.sessionWindowedCount, session, [options]],
			[() => session.reduce(fn, options), windowed.sessionWindowedReduce, session, [fn, options]],
			[
				() => session.aggregate(fn, fn, fn, options),
				windowed.sessionWindowedAggregate,
				session,
				[fn, fn, fn, options],
			],
			[() => slidingWindows.count(options), sliding.slidingWindowedCount, slidingWindows, [options]],
			[() => slidingWindows.reduce(fn, options), sliding.slidingWindowedReduce, slidingWindows, [fn, options]],
			[
				() => slidingWindows.aggregate(fn, fn, options),
				sliding.slidingWindowedAggregate,
				slidingWindows,
				[fn, fn, options],
			],
		]
		for (const [call, op, target, args] of cases) {
			call()
			expect(op).toHaveBeenCalledWith(target, ...args)
		}
	})

	it('grouped tables aggregate', () => {
		const codecs = {} as KGroupedTableImpl<string, number>['codecs']
		const grouped = new KGroupedTableImpl<string, number>(ctx, 'map', codecs, undefined)
		grouped.count(options)
		expect(tableGroupBy.count).toHaveBeenCalledWith(grouped, options)
		grouped.reduce(fn, fn, options)
		expect(tableGroupBy.reduce).toHaveBeenCalledWith(grouped, fn, fn, options)
		const aggregateOptions = { ...options, value: codec.json<number>() }
		grouped.aggregate(fn, fn, fn, aggregateOptions)
		expect(tableGroupBy.aggregate).toHaveBeenCalledWith(grouped, fn, fn, fn, aggregateOptions)
	})

	it('the split builder adds branches in order and returns itself', () => {
		const branched = new BranchedKStreamImpl(stream, 'splitter', [])
		expect(branched.branch(fn, { name: 'a' }).branch(fn)).toBe(branched)
		expect(stateless.addBranch).toHaveBeenNthCalledWith(1, branched, fn, { name: 'a' })
		expect(stateless.addBranch).toHaveBeenNthCalledWith(2, branched, fn, undefined)
		branched.defaultBranch(options)
		expect(stateless.defaultBranch).toHaveBeenCalledWith(branched, options)
		branched.noDefaultBranch()
		expect(stateless.noDefaultBranch).toHaveBeenCalledWith(branched)
	})
})
