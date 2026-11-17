import { describe, it, expect } from 'vitest'

import { TestDriver } from '../../src/testing.js'
import { codec } from '../../src/index.js'
import { TimeWindows, SessionWindows } from '../../src/legacy/windows.js'
import { InMemoryWindowStore, InMemorySessionStore } from '../../src/state/memory.js'
import { StreamStreamJoinNode } from '../../src/legacy/processors/joins/stream-stream.js'
import { SessionReduceNode, SessionAggregateNode } from '../../src/legacy/processors/aggregation.js'
import { Processor, type StreamRecord } from '../../src/legacy/processors/base.js'

const str = codec.string()
const num = {
	encode: (n: number) => {
		const b = Buffer.alloc(8)
		b.writeDoubleLE(n, 0)
		return b
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

class Collector<K, V> extends Processor<K, V> {
	readonly records: Array<{ key: K | null; value: V | null }> = []
	clone(): Processor<K, V> {
		return this
	}
	async process(record: StreamRecord<K, V>): Promise<void> {
		this.records.push({ key: record.key, value: record.value })
	}
}

function rec<V>(key: string, value: V, timestamp: number): StreamRecord<string, V> {
	return { key, value, timestamp: BigInt(timestamp), topic: 't', partition: 0, offset: 0n, headers: {} }
}

const send = (
	ctx: { send: (...a: any[]) => Promise<void> },
	topic: string,
	key: string,
	value: string,
	ts: number,
	partition = 0
) => ctx.send(topic, Buffer.from(value), { key: Buffer.from(key), timestamp: BigInt(ts), partition })

describe('operator semantics: stream-stream join', () => {
	it('two records with same key + same timestamp on one side both join (no overwrite in window store)', async () => {
		const mk = (name: string) =>
			new InMemoryWindowStore<string, string>(name, {
				keyCodec: str,
				valueCodec: str,
				retentionMs: 20_000,
				windowSizeMs: 1000,
			})
		const left = mk('l')
		const right = mk('r')
		const joinL = new StreamStreamJoinNode<string, string, string, string>(
			{ store: left },
			{ store: right },
			(a, b) => `${a}+${b}`,
			1000
		)
		const joinR = new StreamStreamJoinNode<string, string, string, string>(
			{ store: right },
			{ store: left },
			(b, a) => `${a}+${b}`,
			1000
		)
		const out = new Collector<string, string>()
		joinL.connect(out)
		joinR.connect(out)
		await joinL.process(rec('k', 'L1', 100))
		await joinL.process(rec('k', 'L2', 100)) // same key + timestamp as L1
		await joinR.process(rec('k', 'R1', 100))
		// Kafka Streams stores both (retainDuplicates) -> 2 results
		expect(out.records.map(r => r.value).sort()).toEqual(['L1+R1', 'L2+R1'])
	})

	it('same-key same-timestamp duplicates via DSL both join', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', { key: str, value: str })
		const r = driver.input('r', { key: str, value: str })
		l.join(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L1', 100)
			await send(ctx, 'l', 'k', 'L2', 100)
			await send(ctx, 'r', 'k', 'R1', 100)
			const vals = ctx.output('out', { key: str, value: str }).map(o => o.value)
			expect(vals.sort()).toEqual(['L1+R1', 'L2+R1'])
		})
	})

	it('window is inclusive at +/- window size and exclusive beyond', async () => {
		const driver = new TestDriver()
		const l = driver.input('l', { key: str, value: str })
		const r = driver.input('r', { key: str, value: str })
		l.join(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'Rlo', 4000)
			await send(ctx, 'r', 'k', 'Rhi', 6000)
			await send(ctx, 'r', 'k', 'Rout1', 3999)
			await send(ctx, 'r', 'k', 'Rout2', 6001)
			const vals = ctx.output('out', { key: str, value: str }).map(o => o.value)
			expect(vals).toEqual(['L+Rlo', 'L+Rhi'])
		})
	})

	// Known divergence: left/outer stream joins emit the unmatched result eagerly (pre-3.1 Kafka Streams semantics).
	it.skip('left join does not emit (l,null) when a matching right record is already in the window... and not both null and match for same left', async () => {
		// Kafka Streams >= 3.? defers the unmatched-left result until the window closes, so a left record that
		// later finds a match never produces a (l, null) result.
		const driver = new TestDriver()
		const l = driver.input('l', { key: str, value: str })
		const r = driver.input('r', { key: str, value: str })
		l.leftJoin(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'R', 5500)
			const vals = ctx.output('out', { key: str, value: str }).map(o => o.value)
			expect(vals).toEqual(['L+R'])
		})
	})
})

describe('operator semantics: session windows', () => {
	async function newStore() {
		const s = new InMemorySessionStore<string, string>('s', {
			keyCodec: str,
			valueCodec: str,
			retentionMs: 1_000_000,
		})
		await s.init()
		return s
	}

	it('SessionReduceNode applies reducer in event-time order (non-commutative reducer)', async () => {
		const store = await newStore()
		const node = new SessionReduceNode<string, string>('s', { store }, (agg, v) => agg + v, 100)
		const out = new Collector<any, string>()
		node.connect(out)
		await node.process(rec('k', 'a', 0))
		await node.process(rec('k', 'b', 10))
		await node.process(rec('k', 'c', 20))
		const last = out.records[out.records.length - 1]!
		// Kafka Streams: in-order records concatenate in order => "abc"
		expect(last.value).toBe('abc')
	})

	it('SessionReduceNode merging bridged sessions keeps time order', async () => {
		const store = await newStore()
		const node = new SessionReduceNode<string, string>('s', { store }, (agg, v) => agg + v, 100)
		const out = new Collector<any, string>()
		node.connect(out)
		await node.process(rec('k', 'a', 0))
		await node.process(rec('k', 'c', 200))
		await node.process(rec('k', 'b', 100)) // bridges [0,0] and [200,200]
		const last = out.records[out.records.length - 1]!
		// Kafka Streams semantics: existing sessions are merged in start order ('a' + 'c'),
		// then the new record is applied last.
		expect(last.value).toBe('acb')
	})

	it('SessionAggregateNode merging bridged sessions passes sessions to merger in start order', async () => {
		const store = await newStore()
		const node = new SessionAggregateNode<string, string, string>(
			's',
			{ store },
			() => '',
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			100
		)
		const out = new Collector<any, string>()
		node.connect(out)
		await node.process(rec('k', 'c', 200))
		await node.process(rec('k', 'a', 0))
		await node.process(rec('k', 'b', 100)) // bridges
		const last = out.records[out.records.length - 1]!
		// sessions must be merged in start order (a then c) then new value applied: "ac" + "b"... Kafka: merger(agg, a), merger(agg, c), aggregator(b)
		expect(last.value).toBe('acb')
	})

	it('session gap is inclusive (event exactly gap after session end merges)', async () => {
		const driver = new TestDriver()
		const results: Array<{ s: number; e: number; v: number | null }> = []
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap(100))
			.count()
			.toStream()
			.peek((k: any, v) => results.push({ s: k.window.start, e: k.window.end, v }))
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'x', 1000)
			await send(ctx, 'in', 'k', 'x', 1100)
			const last = results[results.length - 1]!
			expect(last).toEqual({ s: 1000, e: 1100, v: 2 })
		})
	})
})

describe('operator semantics: KTable', () => {
	it('KTable.filter emits a tombstone when a previously-passing key stops passing', async () => {
		const driver = new TestDriver()
		driver
			.table('t', { key: str, value: str })
			.filter((_k, v) => v === 'keep')
			.toStream()
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'keep', 1)
			await send(ctx, 't', 'k', 'drop', 2)
			const out = ctx.output('out', { key: str, value: str })
			expect(out.map(o => o.value)).toEqual(['keep', null])
		})
	})

	it('KTable.filter does not invoke the predicate with a null value (tombstone)', async () => {
		const driver = new TestDriver()
		const seen: Array<string | null> = []
		driver
			.table('t', { key: str, value: str })
			.filter((_k, v) => {
				seen.push(v)
				return v !== null && v.length > 0
			})
			.toStream()
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'v', 1)
			await ctx.consumer.sendMessage('t', null as unknown as Buffer, { key: Buffer.from('k'), timestamp: 2n })
			expect(seen).not.toContain(null)
		})
	})

	it('KTable.filter then groupBy().count() un-counts rows that stop passing the filter', async () => {
		const driver = new TestDriver()
		driver
			.table('t', { key: str, value: str })
			.filter((_k, v) => v === 'keep')
			.groupBy((_k, v) => [v, v] as [string, string], { key: str, value: str })
			.count()
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'keep', 1)
			await send(ctx, 't', 'k', 'drop', 2)
			const out = ctx.output('out', { key: str, value: num })
			expect(out.map(o => o.value)).toEqual([1, null])
		})
	})

	it('KTable.mapValues does not turn a tombstone into a non-null record', async () => {
		const driver = new TestDriver()
		driver
			.table('t', { key: str, value: str })
			.mapValues(v => (v === null ? 'NULLMAPPED' : v.toUpperCase()))
			.toStream()
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 't', 'k', 'a', 1)
			await ctx.consumer.sendMessage('t', null as unknown as Buffer, { key: Buffer.from('k'), timestamp: 2n })
			const out = ctx.output('out', { key: str, value: str })
			expect(out.map(o => o.value)).toEqual(['A', null])
		})
	})

	it('stream can be joined with an aggregated table (groupByKey().count())', async () => {
		const driver = new TestDriver()
		const counts = driver.input('a', { key: str, value: str }).groupByKey().count()
		driver
			.input('b', { key: str, value: str })
			.join(counts, (v, c) => `${v}:${c}`)
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			expect(ctx.output('out', { key: str, value: str }).map(o => o.value)).toEqual(['y:1'])
		})
	})

	// Known limitation: derived (filter/mapValues) tables are not materialized, so joining them throws loudly.
	it.skip('filtered table can be joined with another table', async () => {
		const driver = new TestDriver()
		const a = driver.table('a', { key: str, value: str }).filter((_k, v) => v !== null)
		const b = driver.table('b', { key: str, value: str })
		a.join(b, (x, y) => `${x}+${y}`)
			.toStream()
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'a', 'k', 'x', 1)
			await send(ctx, 'b', 'k', 'y', 2)
			expect(ctx.output('out', { key: str, value: str }).map(o => o.value)).toEqual(['x+y'])
		})
	})
})

describe('operator semantics: stream time / partitions', () => {
	it('a far-future record on one partition must not make late-ish records on another partition be dropped', async () => {
		// Kafka Streams tracks stream time per task (partition); here the cleanup state is shared by all clones.
		const driver = new TestDriver()
		const results: Array<{ s: number; v: number | null }> = []
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.windowedBy(TimeWindows.of(1000))
			.count()
			.toStream()
			.peek((k: any, v) => results.push({ s: k.window.start, v }))
		await driver.run(async ctx => {
			await send(ctx, 'in', 'a', 'x', 10_000_000, 0) // partition 0 far in the future
			await send(ctx, 'in', 'b', 'x', 5_000, 1) // partition 1, perfectly in-order for its own partition
			expect(results.find(r => r.s === 5000)).toBeDefined()
		})
	})
})

describe('operator semantics: sanity', () => {
	it('sanity: partition-1 record alone is counted', async () => {
		const driver = new TestDriver()
		const results: Array<{ s: number; v: number | null }> = []
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.windowedBy(TimeWindows.of(1000))
			.count()
			.toStream()
			.peek((k: any, v) => results.push({ s: k.window.start, v }))
		await driver.run(async ctx => {
			await send(ctx, 'in', 'b', 'x', 5_000, 1)
			expect(results.find(r => r.s === 5000)).toBeDefined()
		})
	})
})

describe('operator semantics: output record timestamps', () => {
	it('to() forwards the event timestamp of the record to the producer', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { key: str, value: str })
			.mapValues(v => v)
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			const sent: any[] = []
			const producer = driver.producers[0]!
			const orig = producer.send.bind(producer)
			producer.send = ((topic: string, msg: any) => {
				sent.push(msg)
				return orig(topic, msg)
			}) as any
			await send(ctx, 'in', 'k', 'v', 123_456)
			expect(sent).toHaveLength(1)
			// Kafka Streams preserves the input record timestamp on output records
			expect(sent[0].timestamp).toEqual(new Date(123_456))
		})
	})
})
