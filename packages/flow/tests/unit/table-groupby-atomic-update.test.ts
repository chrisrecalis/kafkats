import { describe, it, expect } from 'vitest'

import {
	TableGroupByNode,
	TableDeltaCountNode,
	TableDeltaAggregateNode,
	TableGroupedComputeCountNode,
	type GroupedTableMapping,
} from '@/processors/table.js'
import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import type { KeyValueStore } from '@/state.js'

const stringCodec = {
	encode: (k: string) => Buffer.from(k),
	decode: (b: Buffer) => b.toString(),
}

// Minimal Map-backed KeyValueStore (same pattern as delta-tombstone.test.ts).
function mapStore<K, V>(): KeyValueStore<K, V> {
	const data = new Map<K, V>()
	return {
		name: 'store',
		get: async (k: K) => (data.has(k) ? data.get(k)! : undefined),
		put: async (k: K, v: V) => {
			data.set(k, v)
		},
		delete: async (k: K) => {
			data.delete(k)
		},
		all: async function* () {
			for (const [k, v] of data) yield [k, v] as [K, V]
		},
		range: async function* () {},
		approximateNumEntries: async () => data.size,
		init: async () => {},
		flush: async () => {},
		close: async () => {},
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any
}

class CollectorNode<K, V> extends Processor<K, V> {
	readonly emissions: Array<{ key: K | null; value: V | null }> = []

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return this
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		this.emissions.push({ key: record.key, value: record.value })
	}
}

type Member = { group: string; amount: number }

function record(key: string, value: Member, offset: number): StreamRecord<string, Member> {
	return {
		key,
		value,
		timestamp: 0n,
		topic: 't',
		partition: 0,
		offset: BigInt(offset),
		headers: {},
	}
}

function buildCountPipeline() {
	const mappingStore = mapStore<string, GroupedTableMapping<string, Member>>()
	const groupBy = new TableGroupByNode<string, Member, string>(
		(_key, value) => [value.group, value],
		{ store: mappingStore },
		stringCodec
	)
	const countStore = mapStore<string, number>()
	const count = new TableDeltaCountNode<string, Member>('count', { store: countStore })
	const collector = new CollectorNode<string, number>()
	groupBy.connect(count)
	count.connect(collector)
	return { groupBy, collector }
}

describe('KTable groupBy delta re-aggregation applies same-key updates atomically (EOS path)', () => {
	it('count: single-member value update with unchanged grouped key emits no spurious tombstone', async () => {
		const { groupBy, collector } = buildCountPipeline()

		await groupBy.process(record('s1', { group: 'grp', amount: 1 }, 0))
		expect(collector.emissions).toEqual([{ key: 'grp', value: 1 }])

		collector.emissions.length = 0
		// Same source key, SAME grouped key, new value: Kafka Streams applies subtractor+adder
		// atomically and emits ONE update — not [tombstone, 1].
		await groupBy.process(record('s1', { group: 'grp', amount: 2 }, 1))
		expect(collector.emissions).toEqual([{ key: 'grp', value: 1 }])
	})

	it('count: multi-member same-key update does not emit a transient dip (n -> n-1 -> n)', async () => {
		const { groupBy, collector } = buildCountPipeline()

		await groupBy.process(record('s1', { group: 'grp', amount: 1 }, 0))
		await groupBy.process(record('s2', { group: 'grp', amount: 5 }, 1))
		expect(collector.emissions).toEqual([
			{ key: 'grp', value: 1 },
			{ key: 'grp', value: 2 },
		])

		collector.emissions.length = 0
		await groupBy.process(record('s1', { group: 'grp', amount: 9 }, 2))
		// Pre-fix this dipped: [1, 2]. Post-fix: single [2].
		expect(collector.emissions).toEqual([{ key: 'grp', value: 2 }])
	})

	it('count: key-CHANGE updates still emit the two-record SUB/ADD flow', async () => {
		const { groupBy, collector } = buildCountPipeline()

		await groupBy.process(record('s1', { group: 'grp-a', amount: 1 }, 0))
		collector.emissions.length = 0

		await groupBy.process(record('s1', { group: 'grp-b', amount: 1 }, 1))
		expect(collector.emissions).toEqual([
			{ key: 'grp-a', value: null },
			{ key: 'grp-b', value: 1 },
		])
	})

	it('aggregate: same-key update applies subtractor and adder before emitting once', async () => {
		const mappingStore = mapStore<string, GroupedTableMapping<string, Member>>()
		const groupBy = new TableGroupByNode<string, Member, string>(
			(_key, value) => [value.group, value],
			{ store: mappingStore },
			stringCodec
		)
		const aggStore = mapStore<string, number>()
		const aggregate = new TableDeltaAggregateNode<string, Member, number>(
			'agg',
			{ store: aggStore },
			() => 0,
			(_key, value, agg) => agg + value.amount,
			(_key, value, agg) => agg - value.amount
		)
		const collector = new CollectorNode<string, number>()
		groupBy.connect(aggregate)
		aggregate.connect(collector)

		await groupBy.process(record('s1', { group: 'grp', amount: 1 }, 0))
		await groupBy.process(record('s2', { group: 'grp', amount: 3 }, 1))
		expect(collector.emissions).toEqual([
			{ key: 'grp', value: 1 },
			{ key: 'grp', value: 4 },
		])

		collector.emissions.length = 0
		// s1: 1 -> 5. Pre-fix emitted the intermediate [3, 8]; post-fix a single [8].
		await groupBy.process(record('s1', { group: 'grp', amount: 5 }, 2))
		expect(collector.emissions).toEqual([{ key: 'grp', value: 8 }])
	})

	it('at_least_once recompute path stays correct for same-key updates', async () => {
		const mappingStore = mapStore<string, GroupedTableMapping<string, Member>>()
		const mappingStoreRef = { store: mappingStore }
		const groupBy = new TableGroupByNode<string, Member, string>(
			(_key, value) => [value.group, value],
			mappingStoreRef,
			stringCodec
		)
		const countStore = mapStore<string, number>()
		const count = new TableGroupedComputeCountNode<string, string, Member>(
			'count',
			{ store: countStore },
			mappingStoreRef,
			stringCodec
		)
		const collector = new CollectorNode<string, number>()
		groupBy.connect(count)
		count.connect(collector)

		await groupBy.process(record('s1', { group: 'grp', amount: 1 }, 0))
		collector.emissions.length = 0

		await groupBy.process(record('s1', { group: 'grp', amount: 2 }, 1))
		// Recompute from the mapping store: count for 'grp' is still 1, emitted with no dip.
		expect(collector.emissions).toEqual([{ key: 'grp', value: 1 }])
	})
})
