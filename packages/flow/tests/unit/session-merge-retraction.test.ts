import { describe, expect, it } from 'vitest'

import { SessionReduceNode, SessionAggregateNode } from '../../src/processors/aggregation.js'
import { Processor, type StreamRecord } from '../../src/processors/base.js'
import type { Windowed } from '../../src/types.js'
import { InMemorySessionStore } from '../../src/state/memory.js'

const stringCodec = {
	encode: (s: string) => Buffer.from(s, 'utf-8'),
	decode: (b: Buffer) => b.toString('utf-8'),
}
const numberCodec = {
	encode: (n: number) => {
		const b = Buffer.alloc(8)
		b.writeDoubleLE(n, 0)
		return b
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

class Collector<K, V> extends Processor<K, V> {
	readonly records: Array<{ start?: number; end?: number; value: V | null }> = []
	clone(): Processor<K, V> {
		return this
	}
	async process(record: StreamRecord<K, V>): Promise<void> {
		const key = record.key as Windowed<unknown> | null
		this.records.push({ start: key?.window.start, end: key?.window.end, value: record.value })
	}
}

function rec(key: string, value: number, timestamp: number): StreamRecord<string, number> {
	return { key, value, timestamp: BigInt(timestamp), topic: 't', partition: 0, offset: 0n, headers: {} }
}

async function newStore() {
	const store = new InMemorySessionStore<string, number>('s', {
		keyCodec: stringCodec,
		valueCodec: numberCodec,
		retentionMs: 60_000,
	})
	await store.init()
	return store
}

describe('session merge retraction', () => {
	it('SessionReduceNode retracts the merged-away session before emitting the merged result', async () => {
		const store = await newStore()
		const node = new SessionReduceNode<string, number>('s', { store }, (a, b) => a + b, 10)
		const collector = new Collector<Windowed<string>, number>()
		node.connect(collector)

		await node.process(rec('a', 1, 0)) // session [0,0] = 1
		await node.process(rec('a', 1, 5)) // merges [0,0] into [0,5] = 2

		// The old [0,0] window must be retracted (tombstone) before the merged [0,5] result, so a
		// downstream table/consumer keyed on the windowed key doesn't keep a stale [0,0] entry.
		expect(collector.records).toEqual([
			{ start: 0, end: 0, value: 1 },
			{ start: 0, end: 0, value: null },
			{ start: 0, end: 5, value: 2 },
		])
	})

	it('SessionAggregateNode retracts the merged-away session before emitting the merged result', async () => {
		const store = await newStore()
		const node = new SessionAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			10
		)
		const collector = new Collector<Windowed<string>, number>()
		node.connect(collector)

		await node.process(rec('a', 1, 0)) // session [0,0] = 1
		await node.process(rec('a', 1, 5)) // merges [0,0] into [0,5] = 2

		expect(collector.records).toEqual([
			{ start: 0, end: 0, value: 1 },
			{ start: 0, end: 0, value: null },
			{ start: 0, end: 5, value: 2 },
		])
	})

	it('does not retract when the merged window equals the existing session (in-place update)', async () => {
		const store = await newStore()
		const node = new SessionReduceNode<string, number>('s', { store }, (a, b) => a + b, 10)
		const collector = new Collector<Windowed<string>, number>()
		node.connect(collector)

		await node.process(rec('a', 1, 0)) // session [0,5]... first establish a wider session
		await node.process(rec('a', 1, 5)) // -> [0,5] = 2
		collector.records.length = 0

		// A record fully inside [0,5] keeps the same window bounds: no tombstone, just an updated value.
		await node.process(rec('a', 1, 3))
		expect(collector.records).toEqual([{ start: 0, end: 5, value: 3 }])
	})

	it('SessionReduceNode retracts both sessions a middle record bridges', async () => {
		const store = await newStore()
		const node = new SessionReduceNode<string, number>('s', { store }, (a, b) => a + b, 10)
		const collector = new Collector<Windowed<string>, number>()
		node.connect(collector)

		await node.process(rec('a', 1, 0)) // session [0,0]
		await node.process(rec('a', 1, 20)) // separate session [20,20] (gap 10)
		collector.records.length = 0

		// A record at t=10 is within the gap of both sessions, bridging them into [0,20]=3. Both
		// absorbed windows differ from the merged window, so both must be retracted.
		await node.process(rec('a', 1, 10))

		expect(collector.records).toHaveLength(3)
		const tombstones = collector.records.slice(0, 2).sort((x, y) => (x.start ?? 0) - (y.start ?? 0))
		expect(tombstones).toEqual([
			{ start: 0, end: 0, value: null },
			{ start: 20, end: 20, value: null },
		])
		expect(collector.records[2]).toEqual({ start: 0, end: 20, value: 3 })
	})

	it('SessionAggregateNode retracts both sessions a middle record bridges', async () => {
		const store = await newStore()
		const node = new SessionAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			10
		)
		const collector = new Collector<Windowed<string>, number>()
		node.connect(collector)

		await node.process(rec('a', 1, 0))
		await node.process(rec('a', 1, 20))
		collector.records.length = 0

		await node.process(rec('a', 1, 10))

		expect(collector.records).toHaveLength(3)
		const tombstones = collector.records.slice(0, 2).sort((x, y) => (x.start ?? 0) - (y.start ?? 0))
		expect(tombstones).toEqual([
			{ start: 0, end: 0, value: null },
			{ start: 20, end: 20, value: null },
		])
		expect(collector.records[2]).toEqual({ start: 0, end: 20, value: 3 })
	})
})
