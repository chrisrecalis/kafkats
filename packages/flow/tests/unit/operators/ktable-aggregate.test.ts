import { describe, expect, it } from 'vitest'
import { codec, type Codec } from '@/codec.js'
import { COUNT_CODEC } from '@/dsl/ops/aggregate.js'
import type { Change } from '@/operators/change.js'
import {
	KTableAggregate,
	tableAggregate,
	tableCount,
	tableReduce,
	type TableAggregator,
} from '@/operators/ktable-aggregate.js'
import type { RecordHeaders } from '@/processor/record.js'
import type { ValueAndTimestamp } from '@/state.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from '../support/processor-harness.js'

const str = codec.string()

/** A KGroupedTable aggregation of the changes of 'groups' into the store 'agg', sending old values. */
async function aggregation<A>(aggregator: TableAggregator<string, number, A>, valueCodec: Codec<A>) {
	const store = new InMemoryKeyValueStore<string, ValueAndTimestamp<A>>('agg', {
		keyCodec: str,
		valueCodec: valueAndTimestampCodec(valueCodec),
	})
	const node = new KTableAggregate('agg', aggregator)
	node.enableSendingOldValues()
	const out = capture<string, Change<A>>('out', ['aggregate'])
	const harness = await ProcessorHarness.start(
		[sourceDef('source', ['groups']), processorDef('aggregate', ['source'], node.supplier, ['agg']), out.def],
		{ stores: [store] }
	)
	const change = (
		group: string | null,
		newValue: number | null,
		oldValue: number | null,
		timestamp = 0,
		headers: RecordHeaders = {}
	) => harness.process('groups', { key: group, value: { newValue, oldValue }, timestamp, headers })
	/** `[group, new, old, timestamp]` of every result. */
	const results = () => out.records.map(r => [r.key, r.value.newValue, r.value.oldValue, r.timestamp])
	return { harness, store, out, change, results }
}

const sum = tableAggregate<string, number, number>(
	() => 0,
	(_key, value, total) => total + value,
	(_key, value, total) => total - value
)
const sumReduce = tableReduce<number>(
	(total, value) => total + value,
	(total, value) => total - value
)

describe('KGroupedTable aggregations (KS KTableAggregate, KTableReduce; 6.15)', () => {
	it('subtracts the old value, then adds the new one; results carry the max timestamp and the update’s headers', async () => {
		const { out, change, results, store } = await aggregation(sum, codec.json<number>())
		const trace = { trace: Buffer.from('t') }
		await change('g', 10, null, 5)
		await change('g', 3, null, 2)
		await change('g', 7, 10, 4, trace)
		expect(results()).toEqual([
			['g', 10, null, 5],
			['g', 13, 10, 5],
			['g', 10, 13, 5],
		])
		expect(out.records.at(-1)?.headers).toEqual(trace)
		await change('g', null, 3, 9)
		expect(await store.get('g')).toEqual({ value: 7, timestamp: 9 })
	})

	it('BC11: count keeps a group whose count drops to 0, and emits the 0', async () => {
		const { change, results, store } = await aggregation(tableCount, COUNT_CODEC)
		await change('g', 1, null)
		await change('g', null, 1)
		await change('g', 1, null)
		expect(results()).toEqual([
			['g', 1, null, 0],
			['g', 0, 1, 0],
			['g', 1, 0, 0],
		])
		expect(await store.get('g')).toEqual({ value: 1, timestamp: 0 })
	})

	it('B10: a value leaving a group without an aggregate subtracts nothing, writes and forwards nothing', async () => {
		for (const [aggregator, valueCodec] of [
			[tableCount, COUNT_CODEC],
			[sum, codec.json<number>()],
			[sumReduce, codec.json<number>()],
		] as const) {
			const { change, results, store } = await aggregation<number>(aggregator, valueCodec)
			await change('missing', null, 1)
			expect(results()).toEqual([])
			expect(await store.get('missing')).toBeUndefined()
			// A change with both values adds the new one to the initial aggregate (nothing to subtract from).
			await change('missing', 5, 1)
			expect(results().map(r => r[1])).toEqual([aggregator === tableCount ? 1 : 5])
		}
	})

	it('reduce: the first value is the aggregate; a null result deletes it with a tombstone, then the next value restarts', async () => {
		const reduce = tableReduce<number>(
			(total, value) => total + value,
			(total, value) => (total === value ? (null as unknown as number) : total - value)
		)
		const { change, results, store } = await aggregation(reduce, codec.json<number>())
		await change('g', 4, null, 1)
		await change('g', 6, null, 2)
		await change('g', null, 4, 3)
		await change('g', null, 6, 4)
		expect(await store.get('g')).toBeUndefined()
		await change('g', 8, null, 1)
		expect(results()).toEqual([
			['g', 4, null, 1],
			['g', 10, 4, 2],
			['g', 6, 10, 3],
			['g', null, 6, 4],
			['g', 8, null, 1],
		])
	})

	it('reduce: a value replacing the only one in its group becomes the aggregate when the subtraction empties it', async () => {
		const reduce = tableReduce<number>(
			(total, value) => total + value,
			(total, value) => (total === value ? (null as unknown as number) : total - value)
		)
		const { change, results } = await aggregation(reduce, codec.json<number>())
		await change('g', 4, null, 3)
		await change('g', 9, 4, 1)
		expect(results()).toEqual([
			['g', 4, null, 3],
			['g', 9, 4, 3],
		])
	})

	it('a change without a group or without values is dropped and counted', async () => {
		const { harness, results } = await aggregation(tableCount, COUNT_CODEC)
		await harness.process('groups', { key: null, value: { newValue: 1, oldValue: null }, timestamp: 0 })
		await harness.process('groups', { key: 'g', value: null, timestamp: 0 })
		expect(results()).toEqual([])
		expect([harness.task.droppedCount('null-key'), harness.task.droppedCount('null-value')]).toEqual([1, 1])
	})

	it('old values are sent only once downstream asked for them', async () => {
		const store = new InMemoryKeyValueStore<string, ValueAndTimestamp<number>>('agg', {
			keyCodec: str,
			valueCodec: valueAndTimestampCodec(COUNT_CODEC),
		})
		const node = new KTableAggregate<string, number, number>('agg', tableCount)
		const out = capture<string, Change<number>>('out', ['aggregate'])
		const harness = await ProcessorHarness.start(
			[sourceDef('source', ['groups']), processorDef('aggregate', ['source'], node.supplier, ['agg']), out.def],
			{ stores: [store] }
		)
		const add = () => harness.process('groups', { key: 'g', value: { newValue: 1, oldValue: null }, timestamp: 0 })
		await add()
		await add()
		expect(node.enableSendingOldValues()).toBe(true)
		await add()
		expect(out.records.map(r => r.value)).toEqual([
			{ newValue: 1, oldValue: null },
			{ newValue: 2, oldValue: null },
			{ newValue: 3, oldValue: 2 },
		])
	})
})
