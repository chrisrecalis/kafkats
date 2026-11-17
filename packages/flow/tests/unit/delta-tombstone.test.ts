import { describe, expect, it, vi } from 'vitest'
import { codec, type Codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import {
	KTableAggregate,
	tableAggregate,
	tableCount,
	tableReduce,
	type GroupAggregator,
} from '@/operators/ktable-aggregate.js'
import { DELTA_OP_HEADER } from '@/operators/ktable-repartition-map.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from './support/processor-harness.js'

const str = codec.string()
const add = { [DELTA_OP_HEADER]: Buffer.from('add') }
const sub = { [DELTA_OP_HEADER]: Buffer.from('sub') }
const json = <T>(): Codec<T> => codec.json<T>()

/** A KGroupedTable aggregation reading grouped rows of 'rows' into the store 'agg' (and 'agg-members'). */
async function aggregation<A>(aggregator: GroupAggregator<string, number, A>, members = false) {
	const store = new InMemoryKeyValueStore('agg', { keyCodec: str, valueCodec: valueAndTimestampCodec(json<A>()) })
	const membersStore = new InMemoryKeyValueStore('agg-members', { keyCodec: str, valueCodec: json<number>() })
	const deleted = vi.spyOn(store, 'delete')
	const node = new KTableAggregate(
		{ storeName: 'agg', membersStore: members ? 'agg-members' : undefined },
		aggregator
	)
	node.enableSendingOldValues()
	const out = capture<string, Change<A>>('out', ['aggregate'])
	const harness = await ProcessorHarness.start(
		[
			sourceDef('source', ['rows']),
			processorDef('aggregate', ['source'], node.supplier, members ? ['agg', 'agg-members'] : ['agg']),
			out.def,
		],
		{ stores: members ? [store, membersStore] : [store] }
	)
	let offset = 0
	const row = (group: string, value: number, headers: Record<string, Buffer>) =>
		harness.process('rows', { key: group, value, timestamp: offset++, headers })
	const last = () => {
		const record = out.records.at(-1)
		return record && { key: record.key, value: record.value.newValue, headers: record.headers }
	}
	return { row, last, out, deleted, store, membersStore }
}

/** Price sums whose subtractor returns null once the total hits zero (the group is empty). */
const emptiedAtZero = (total: number, value: number) =>
	total - value === 0 ? (null as unknown as number) : total - value

describe('KGroupedTable aggregations: a null result deletes the group and forwards a tombstone', () => {
	it('reduce: deletes the group when the subtractor empties it; a re-add restarts from the new value', async () => {
		const { row, last, deleted, store } = await aggregation(tableReduce((total, v) => total + v, emptiedAtZero))

		await row('books', 10, add)
		expect(last()).toEqual({ key: 'books', value: 10, headers: {} })

		// Last item removed: subtractor(10, 10) === null -> the group is empty.
		await row('books', 10, sub)
		expect(deleted).toHaveBeenCalledWith('books')
		expect(await store.get('books')).toBeUndefined()
		expect(last()).toEqual({ key: 'books', value: null, headers: {} })

		// The store was cleared, so the adder does not see a leftover null.
		await row('books', 7, add)
		expect(last()).toEqual({ key: 'books', value: 7, headers: {} })
	})

	it('aggregate: deletes the group when the subtractor empties it; a re-add starts from the initializer', async () => {
		const aggregator = tableAggregate<string, number, number>(
			() => 0,
			(_key, v, total) => total + v,
			(_key, v, total) => emptiedAtZero(total, v)
		)
		const { row, last, deleted } = await aggregation(aggregator)

		await row('books', 10, add)
		expect(last()?.value).toBe(10)

		await row('books', 10, sub)
		expect(deleted).toHaveBeenCalledWith('books')
		expect(last()?.value).toBeNull()

		// Re-add after the tombstone: 0 + 7, not an aggregation onto a stale null.
		await row('books', 7, add)
		expect(last()?.value).toBe(7)
	})

	it('aggregate with a members store: the last row leaving deletes the group whatever the subtractor returned', async () => {
		const aggregator = tableAggregate<string, number, number>(
			() => 100,
			(_key, v, total) => total + v,
			(_key, v, total) => total - v
		)
		const { row, last, membersStore } = await aggregation(aggregator, true)

		await row('books', 1, add)
		await row('books', 2, add)
		expect(last()?.value).toBe(103)
		expect(await membersStore.get('books')).toBe(2)

		await row('books', 1, sub)
		expect(last()?.value).toBe(102)
		// The subtractor returns the initializer's 100, but the group has no rows left.
		await row('books', 2, sub)
		expect(last()?.value).toBeNull()
		expect(await membersStore.get('books')).toBeUndefined()
	})

	it('reduce and aggregate ignore a subtraction from a group without an aggregate', async () => {
		const reduce = await aggregation(
			tableReduce<number>(
				(total, v) => total + v,
				(total, v) => total - v
			)
		)
		await reduce.row('books', 5, sub)
		const aggregate = await aggregation(
			tableAggregate<string, number, number>(
				() => 0,
				(_key, v, total) => total + v,
				(_key, v, total) => total - v
			),
			true
		)
		await aggregate.row('books', 5, sub)
		expect(reduce.out.records).toEqual([])
		expect(aggregate.out.records).toEqual([])
		expect(await aggregate.membersStore.get('books')).toBeUndefined()
	})

	it('count: a group whose count drops to 0 is deleted; B10 (kept until WP3.8): a subtraction from a missing group forwards a tombstone', async () => {
		const { row, last, out, store } = await aggregation(tableCount as GroupAggregator<string, number, number>)
		await row('books', 1, add)
		await row('books', 1, add)
		await row('books', 1, sub)
		expect(last()?.value).toBe(1)
		await row('books', 1, sub)
		expect(last()?.value).toBeNull()
		expect(await store.get('books')).toBeUndefined()

		await row('films', 1, sub)
		expect(last()).toEqual({ key: 'films', value: null, headers: {} })
		expect(out.records.map(r => r.value.oldValue)).toEqual([null, 1, 2, 1, null])
	})
})
