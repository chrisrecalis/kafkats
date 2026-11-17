import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import { internals } from '@/app/internal.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import { codec, type Codec } from '@/codec.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { SessionWindows, SlidingWindows, TimeWindows } from '@/dsl/windows.js'
import type { Change } from '@/operators/change.js'
import type { InternalProcessorContext, ProcessorContext } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import type { SessionStore, TimestampedKeyValueStore, WindowStore } from '@/state.js'
import { TestDriver } from '@/testing/driver.js'
import type { BuiltTopology } from '@/topology/built-topology.js'

const str = codec.string()
const json = codec.json<unknown>()
const key = (k: string) => ({ key: Buffer.from(k) })
const at = (k: string, timestamp: number, partition = 0) => ({ key: Buffer.from(k), timestamp, partition, codec: str })
/** The default count codec: an int64 big-endian (Kafka Streams' `Serdes.Long`). */
const num: Codec<number> = {
	encode: n => {
		const b = Buffer.alloc(8)
		b.writeBigInt64BE(BigInt(n))
		return b
	},
	decode: b => Number(b.readBigInt64BE()),
}

function int64(value: number): Buffer {
	const bytes = Buffer.alloc(8)
	bytes.writeBigInt64BE(BigInt(value))
	return bytes
}

function topologyOf(driver: TestDriver): BuiltTopology {
	return internals(driver.flow).build()
}

/** The builder of store `name` and the subtopology it belongs to. */
function storeOf(topology: BuiltTopology, name: string) {
	for (const subtopology of topology.subtopologies) {
		const store = subtopology.stores.find(s => s.name === name)
		if (store) return { store, subtopology }
	}
	throw new Error(`no store '${name}'`)
}

function repartitionTopics(topology: BuiltTopology): string[] {
	return [...topology.internalTopics.values()].filter(t => t.kind === 'repartition').map(t => t.name)
}

/** A processor under `table` that records the changes it forwards. */
function changesOf<K, V>(table: KTableImpl<K, V>): Change<V>[] {
	const changes: Change<V>[] = []
	table.ctx.processor('CHANGES', undefined, [table.node], () => ({
		process: (record: StreamRecord<K, Change<V>>) => void changes.push(record.value),
	}))
	return changes
}

/** Looks up the key of every `lookups` record in `table` through its value getter. */
function lookUp<K, V>(driver: TestDriver, table: KTableImpl<K, V>, keyCodec: Codec<K>): unknown[] {
	const looked: unknown[] = []
	const lookups = driver.input('lookups', { key: keyCodec }) as KStreamImpl<K, Buffer>
	const getter = table.table.valueGetter
	lookups.ctx.processor(
		'LOOKUP',
		undefined,
		[lookups.node],
		() => {
			const values = getter.create()
			return {
				init: (context: ProcessorContext) => values.init(context as InternalProcessorContext<unknown, unknown>),
				process: async (record: StreamRecord<K, Buffer>) => void looked.push(await values.get(record.key)),
			}
		},
		getter.storeNames
	)
	return looked
}

describe('aggregations (copied from flow.test.ts)', () => {
	it('counts records per key with groupByKey().count()', async () => {
		const driver = new TestDriver()
		const results: Array<{ key: string; count: number }> = []

		driver
			.input('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.count()
			.toStream()
			.peek((key, count) => {
				if (key === null || count === null) {
					throw new Error('Unexpected null key/value in count() results')
				}
				results.push({ key, count })
			})

		await driver.run(async ({ send }) => {
			await send('events', { type: 'click' }, key('user1'))
			await send('events', { type: 'click' }, key('user1'))
			await send('events', { type: 'click' }, key('user2'))
			await send('events', { type: 'click' }, key('user1'))
		})

		expect(results).toEqual([
			{ key: 'user1', count: 1 },
			{ key: 'user1', count: 2 },
			{ key: 'user2', count: 1 },
			{ key: 'user1', count: 3 },
		])
	})

	it('reduces records per key with groupByKey().reduce()', async () => {
		const driver = new TestDriver()
		type Order = { amount: number }
		const results: Array<{ key: string; total: Order }> = []

		driver
			.input('orders', { key: codec.string(), value: codec.json<Order>() })
			.groupByKey()
			.reduce((agg, value) => ({ amount: agg.amount + value.amount }))
			.toStream()
			.peek((key, total) => {
				if (key === null || total === null) {
					throw new Error('Unexpected null key/value in reduce() results')
				}
				results.push({ key, total })
			})

		await driver.run(async ({ send }) => {
			await send('orders', { amount: 100 }, key('user1'))
			await send('orders', { amount: 50 }, key('user1'))
			await send('orders', { amount: 200 }, key('user2'))
			await send('orders', { amount: 75 }, key('user1'))
		})

		expect(results).toEqual([
			{ key: 'user1', total: { amount: 100 } },
			{ key: 'user1', total: { amount: 150 } },
			{ key: 'user2', total: { amount: 200 } },
			{ key: 'user1', total: { amount: 225 } },
		])
	})

	it('aggregates records per key with groupByKey().aggregate()', async () => {
		const driver = new TestDriver()
		type Event = { type: string; count: number }
		type Stats = { total: number; max: number }
		const results: Array<{ key: string; stats: Stats }> = []

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.groupByKey()
			.aggregate<Stats>(
				() => ({ total: 0, max: 0 }),
				(_key, value, agg) => ({
					total: agg.total + value.count,
					max: Math.max(agg.max, value.count),
				}),
				{ value: codec.json<Stats>() }
			)
			.toStream()
			.peek((key, stats) => {
				if (key === null || stats === null) {
					throw new Error('Unexpected null key/value in aggregate() results')
				}
				results.push({ key, stats })
			})

		await driver.run(async ({ send }) => {
			await send('events', { type: 'a', count: 5 }, key('k1'))
			await send('events', { type: 'b', count: 10 }, key('k1'))
			await send('events', { type: 'a', count: 3 }, key('k1'))
		})

		expect(results).toEqual([
			{ key: 'k1', stats: { total: 5, max: 5 } },
			{ key: 'k1', stats: { total: 15, max: 10 } },
			{ key: 'k1', stats: { total: 18, max: 10 } },
		])
	})

	it('groups by a different key with groupBy()', async () => {
		const driver = new TestDriver()
		type Event = { category: string; value: number }
		const results: Array<{ key: string; count: number }> = []

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.groupBy(
				(_key, value) => {
					if (value === null) {
						throw new Error('Unexpected tombstone in groupBy()')
					}
					return value.category
				},
				{ key: codec.string() }
			)
			.count()
			.toStream()
			.peek((key, count) => {
				if (key === null || count === null) {
					throw new Error('Unexpected null key/value in count() results')
				}
				results.push({ key, count })
			})

		await driver.run(async ({ send }) => {
			await send('events', { category: 'electronics', value: 100 }, key('item1'))
			await send('events', { category: 'books', value: 20 }, key('item2'))
			await send('events', { category: 'electronics', value: 200 }, key('item3'))
		})

		expect(results).toEqual([
			{ key: 'electronics', count: 1 },
			{ key: 'books', count: 1 },
			{ key: 'electronics', count: 2 },
		])
	})

	it('skips records with null keys in aggregations', async () => {
		const driver = new TestDriver()
		const results: Array<{ key: string; count: number }> = []

		driver
			.input('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.count()
			.toStream()
			.peek((key, count) => {
				if (key === null || count === null) {
					throw new Error('Unexpected null key/value in count() results')
				}
				results.push({ key, count })
			})

		await driver.run(async ({ send }) => {
			await send('events', { type: 'a' }, key('user1'))
			await send('events', { type: 'b' }) // no key: skipped
			await send('events', { type: 'c' }, key('user1'))
		})

		expect(results).toEqual([
			{ key: 'user1', count: 1 },
			{ key: 'user1', count: 2 },
		])
	})
})

describe('windowed/session store changelog wiring (copied from flow.test.ts)', () => {
	it('time-windowed aggregations create a changelog-backed window store', () => {
		const driver = new TestDriver()
		driver
			.input('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeAndGrace('10s', '5s'))
			.count({ storeName: 'win-counts' })

		const { store } = storeOf(topologyOf(driver), 'win-counts')
		expect(store.kind).toBe('window')
		if (!store.logging.enabled) throw new Error('not logged')
		expect(store.logging.topic).toBe('test-app-win-counts-changelog')
		// The changelog key round-trips a WindowedKey, so restoration can decode it.
		const changelogKey = windowedKeyCodec(store.keyCodec as Codec<string>)
		const encoded = changelogKey.encode({ key: 'k', windowStart: 1000, windowEnd: 2000 })
		expect(changelogKey.decode(encoded)).toEqual({ key: 'k', windowStart: 1000, windowEnd: 2000 })
		// delete,compact with the store's retention (size + grace) plus a day, so the broker prunes expired windows.
		expect(store.logging.configs['cleanup.policy']).toBe('delete,compact')
		expect(store.logging.configs['retention.ms']).toBe(String(15_000 + 86_400_000))
	})

	it('session-windowed aggregations create a changelog-backed session store', () => {
		const driver = new TestDriver()
		driver
			.input('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.windowedBy(SessionWindows.ofInactivityGapAndGrace('30s', '10s'))
			.count({ storeName: 'session-counts' })

		const { store, subtopology } = storeOf(topologyOf(driver), 'session-counts')
		expect(store.kind).toBe('session')
		expect(subtopology.changelogTopics.get('session-counts')).toBe('test-app-session-counts-changelog')
		// delete,compact with the store's retention (gap + grace) plus a day, like a window store.
		if (!store.logging.enabled) throw new Error('not logged')
		expect(store.logging.configs['cleanup.policy']).toBe('delete,compact')
		expect(store.logging.configs['retention.ms']).toBe(String(40_000 + 86_400_000))
	})

	it('co-partitions windowedBy state with its source or repartition topic', () => {
		const driver = new TestDriver()

		// groupByKey() keeps the source key, so state is co-partitioned with the source topic.
		driver
			.input('events', { key: codec.string(), value: codec.json<{ region: string }>() })
			.groupByKey()
			.windowedBy(TimeWindows.of('10s'))
			.count({ storeName: 'by-key' })

		// groupBy() re-keys through a repartition topic (named after the store of an unnamed grouping's aggregation), so
		// its state is co-partitioned with that topic.
		driver
			.input('events2', { key: codec.string(), value: codec.json<{ region: string }>() })
			.groupBy((_key, value) => value!.region, { key: codec.string() })
			.windowedBy(TimeWindows.of('10s'))
			.count({ storeName: 'by-region' })

		const topology = topologyOf(driver)
		expect([...storeOf(topology, 'by-key').subtopology.sourceTopics]).toEqual(['events'])
		expect([...storeOf(topology, 'by-region').subtopology.sourceTopics]).toEqual(['test-app-by-region-repartition'])
	})

	it('BC10: key-based operations on windowed results do not repartition them', () => {
		const driver = new TestDriver()

		// A windowed result sits in the partition of its base key, which is where a windowed key is written to and
		// read from (WindowedCodecs and the windowed partitioner).
		for (const [topic, windows] of [
			['events', TimeWindows.ofSizeWithNoGrace('10s')],
			['visits', SessionWindows.ofInactivityGapWithNoGrace('10s')],
		] as const) {
			const grouped = driver
				.input(topic, { key: codec.string(), value: codec.json<{ type: string }>() })
				.groupByKey()
			const windowed = windows instanceof TimeWindows ? grouped.windowedBy(windows) : grouped.windowedBy(windows)
			windowed
				.count({ storeName: `${topic}-windows` })
				.toStream()
				.groupByKey()
				.count({ storeName: `${topic}-updates` })
		}

		const topology = topologyOf(driver)
		expect(repartitionTopics(topology)).toEqual([])
		for (const topic of ['events', 'visits']) {
			const { subtopology } = storeOf(topology, `${topic}-updates`)
			expect([...subtopology.sourceTopics]).toEqual([topic])
			expect(subtopology).toBe(storeOf(topology, `${topic}-windows`).subtopology)
		}
	})
})

describe('aggregations (copied from operator-semantics*.test.ts)', () => {
	const S = { key: str, value: str }

	it('hopping size 10 advance 3 assigns ts=10 to [3,13) [6,16) [9,19)', async () => {
		const driver = new TestDriver()
		const windows: number[] = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.of(10).advanceBy(3))
			.count()
			.toStream()
			.peek(k => windows.push(k!.window.start))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('k', 10))
		})
		expect(windows).toEqual([3, 6, 9])
	})

	it('tumbling windows at timestamp 0 and exact boundary', async () => {
		const driver = new TestDriver()
		const out: Array<[number, number | null]> = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.of(10))
			.count()
			.toStream()
			.peek((k, v) => out.push([k!.window.start, v]))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('k', 0))
			await send('in', 'x', at('k', 9))
			await send('in', 'x', at('k', 10))
		})
		expect(out).toEqual([
			[0, 1],
			[0, 2],
			[10, 1],
		])
	})

	it('out-of-order record within retention updates its old window', async () => {
		const driver = new TestDriver()
		const out: Array<[number, number | null]> = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.of(1000))
			.count()
			.toStream()
			.peek((k, v) => out.push([k!.window.start, v]))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('k', 5000))
			await send('in', 'x', at('k', 4500))
		})
		expect(out).toEqual([
			[5000, 1],
			[4000, 1],
		])
	})

	it('aggregate returning falsy values (0, empty string, false) is stored and continues', async () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.groupByKey()
			.aggregate(
				() => 5,
				(_k, v, agg) => (v === 'zero' ? 0 : agg + 1),
				{ value: num }
			)
			.toStream()
			.to('out', { key: str, value: num })
		await driver.run(async ({ send, output }) => {
			await send('in', 'zero', at('k', 1))
			await send('in', 'x', at('k', 2))
			expect(output('out', { key: str, value: num }).map(o => o.value)).toEqual([0, 1])
		})
	})

	it('merges sessions with the provided merger instead of using fixed windows', async () => {
		const driver = new TestDriver()
		const out: Array<{ start: number; end: number; value: string | null }> = []
		driver
			.input('events', { key: codec.string(), value: codec.string() })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap('100ms'))
			.aggregate<string>(
				() => '',
				(_key, value, aggregate) => aggregate + value,
				{ value: codec.string(), merger: (a, b) => a + b }
			)
			.toStream()
			.peek((key, value) => {
				if (key) out.push({ start: key.window.start, end: key.window.end, value })
			})

		await driver.run(async ({ send }) => {
			await send('events', 'a', at('k', 1000))
			await send('events', 'c', at('k', 1200))
			// Bridges both sessions: one session [1000, 1200] remains.
			await send('events', 'b', at('k', 1100))
		})

		expect(out[out.length - 1]).toEqual({ start: 1000, end: 1200, value: 'acb' })
	})

	it('requires a merger for session windows', () => {
		const driver = new TestDriver()
		const windowed = driver
			.input('events', { key: codec.string(), value: codec.string() })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap('100ms'))
		const withoutMerger = { value: codec.string() } as Parameters<typeof windowed.aggregate<string>>[2]
		expect(() =>
			windowed.aggregate<string>(
				() => '',
				(_key, value, aggregate) => aggregate + value,
				withoutMerger
			)
		).toThrow(/merger/)
	})

	it('session gap is inclusive (event exactly gap after session end merges)', async () => {
		const driver = new TestDriver()
		const results: Array<{ s: number; e: number; v: number | null }> = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap(100))
			.count()
			.toStream()
			.peek((k, v) => results.push({ s: k!.window.start, e: k!.window.end, v }))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('k', 1000))
			await send('in', 'x', at('k', 1100))
		})
		expect(results[results.length - 1]).toEqual({ s: 1000, e: 1100, v: 2 })
	})

	describe('session reduce and aggregate merge in event-time order', () => {
		function sessions(driver: TestDriver, aggregate: boolean): Array<string | null> {
			const out: Array<string | null> = []
			const windowed = driver.input('in', S).groupByKey().windowedBy(SessionWindows.withInactivityGap(100))
			const table = aggregate
				? windowed.aggregate(
						() => '',
						(_k, v, agg) => agg + v,
						(_k, a, b) => a + b,
						{ value: str }
					)
				: windowed.reduce((agg, v) => agg + v)
			table.toStream().peek((_k, v) => out.push(v))
			return out
		}

		it('a non-commutative reducer sees in-order records in order', async () => {
			const driver = new TestDriver()
			const out = sessions(driver, false)
			await driver.run(async ({ send }) => {
				await send('in', 'a', at('k', 0))
				await send('in', 'b', at('k', 10))
				await send('in', 'c', at('k', 20))
			})
			expect(out[out.length - 1]).toBe('abc')
		})

		it('bridged sessions are merged in start order, then the new record is applied', async () => {
			const driver = new TestDriver()
			const out = sessions(driver, false)
			await driver.run(async ({ send }) => {
				await send('in', 'a', at('k', 0))
				await send('in', 'c', at('k', 200))
				await send('in', 'b', at('k', 100)) // bridges [0,0] and [200,200]
			})
			expect(out[out.length - 1]).toBe('acb')
		})

		it('the merger sees the sessions in start order whatever order they arrived in', async () => {
			const driver = new TestDriver()
			const out = sessions(driver, true)
			await driver.run(async ({ send }) => {
				await send('in', 'c', at('k', 200))
				await send('in', 'a', at('k', 0))
				await send('in', 'b', at('k', 100))
			})
			expect(out[out.length - 1]).toBe('acb')
		})
	})

	it('a far-future record on one partition does not make in-order records of another partition late', async () => {
		// Stream time is kept per task (partition), so partition 0's clock does not judge partition 1's records.
		const driver = new TestDriver()
		const results: Array<{ s: number; v: number | null }> = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.of(1000))
			.count()
			.toStream()
			.peek((k, v) => results.push({ s: k!.window.start, v }))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('a', 10_000_000, 0))
			await send('in', 'x', at('b', 5_000, 1))
		})
		expect(results).toContainEqual({ s: 5000, v: 1 })
	})

	it('sanity: partition-1 record alone is counted', async () => {
		const driver = new TestDriver()
		const results: Array<{ s: number; v: number | null }> = []
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(TimeWindows.of(1000))
			.count()
			.toStream()
			.peek((k, v) => results.push({ s: k!.window.start, v }))
		await driver.run(async ({ send }) => {
			await send('in', 'x', at('b', 5_000, 1))
		})
		expect(results).toEqual([{ s: 5000, v: 1 }])
	})
})

describe('aggregations', () => {
	it('windowed results carry the headers of the record that produced them; a session result its end as timestamp', async () => {
		const driver = new TestDriver()
		const grouped = driver.input('in', { key: str, value: str }).groupByKey()
		grouped.windowedBy(TimeWindows.of(10)).count().toStream().to('windows', { value: num })
		grouped.windowedBy(SessionWindows.withInactivityGap(10)).count().toStream().to('sessions', { value: num })
		const first = { trace: Buffer.from('t1') }
		const second = { trace: Buffer.from('t2') }
		await driver.run(async ({ send, producer }) => {
			await send('in', 'x', { ...at('k', 15), headers: first })
			await send('in', 'x', { ...at('k', 18), headers: second })
			const sent = (topic: string) => producer.messagesFor(topic).map(r => [r.timestamp, r.headers])
			expect(sent('windows')).toEqual([
				[15n, first],
				[18n, second],
			])
			// The record at 18 merges [15,15] into [15,18]: the retraction of [15,15], at its end, is its result too.
			expect(sent('sessions')).toEqual([
				[15n, first],
				[15n, second],
				[18n, second],
			])
		})
	})

	it('drops records with a null value; windowed aggregations aggregate them (Kafka Streams)', async () => {
		const driver = new TestDriver()
		const counts: unknown[] = []
		const grouped = driver.input('in', { key: str, value: str }).groupByKey()
		grouped
			.count()
			.toStream()
			.peek((_k, v) => counts.push(['kv', v]))
		grouped
			.windowedBy(TimeWindows.of(10))
			.count()
			.toStream()
			.peek((_k, v) => counts.push(['window', v]))
		grouped
			.windowedBy(SessionWindows.withInactivityGap(10))
			.count()
			.toStream()
			.peek((_k, v) => counts.push(['session', v]))
		await driver.run(async ({ send }) => {
			await send('in', null, key('k'))
		})
		expect(counts).toEqual([
			['window', 1],
			['session', 1],
		])
	})

	it('a null aggregate deletes the key and is forwarded as a tombstone', async () => {
		const driver = new TestDriver()
		const out: Array<string | null> = []
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.reduce((agg, v) => (v === 'reset' ? (null as unknown as string) : agg + v), { storeName: 'reduced' })
			.toStream()
			.peek((_k, v) => out.push(v))
		await driver.run(async ({ send, getStateStore }) => {
			await send('in', 'a', at('k', 1))
			await send('in', 'reset', at('k', 2))
			expect(await getStateStore<TimestampedKeyValueStore<string, string>>('reduced').get('k')).toBeUndefined()
			await send('in', 'b', at('k', 3))
		})
		expect(out).toEqual(['a', null, 'b'])
	})

	it('a null windowed aggregate deletes its window or session and is forwarded as a tombstone', async () => {
		const driver = new TestDriver()
		const out: unknown[] = []
		const reset = (agg: string, v: string | null) => (v === 'reset' ? (null as unknown as string) : agg + v)
		const grouped = driver.input('in', { key: str, value: str }).groupByKey()
		grouped
			.windowedBy(TimeWindows.of(10))
			.reduce(reset, { storeName: 'windows' })
			.toStream()
			.peek((k, v) => out.push(['window', k!.window, v]))
		grouped
			.windowedBy(SessionWindows.withInactivityGap(10))
			.reduce(reset, { storeName: 'sessions' })
			.toStream()
			.peek((k, v) => out.push(['session', k!.window, v]))
		await driver.run(async ({ send, getStateStore }) => {
			await send('in', 'a', at('k', 1))
			await send('in', 'reset', at('k', 2))
			const windows = getStateStore<WindowStore<string, string>>('windows')
			expect(await windows.get({ key: 'k', windowStart: 0, windowEnd: 10 })).toBeUndefined()
			const sessions: unknown[] = []
			for await (const session of getStateStore<SessionStore<string, string>>('sessions').findSessions(
				'k',
				0,
				10
			)) {
				sessions.push(session)
			}
			expect(sessions).toEqual([])
		})
		expect(out).toEqual([
			['window', { start: 0, end: 10 }, 'a'],
			['session', { start: 1, end: 1 }, 'a'],
			['window', { start: 0, end: 10 }, null],
			// [1,1] is merged into [1,2], so it is retracted; the merged session's null aggregate is a tombstone.
			['session', { start: 1, end: 1 }, null],
			['session', { start: 1, end: 2 }, null],
		])
	})

	it('aggregation stores keep the aggregate with the max timestamp of its records (Kafka Streams)', async () => {
		const driver = new TestDriver()
		driver.input('in', { key: str, value: str }).groupByKey().count({ storeName: 'counts' })
		await driver.run(async ({ send, getStateStore }) => {
			await send('in', 'x', at('k', 7))
			await send('in', 'x', at('k', 3))
			const store = getStateStore<TimestampedKeyValueStore<string, number>>('counts')
			expect(await store.get('k')).toEqual({ value: 2, timestamp: 7 })
		})
	})

	it('exactly-once gives the same results', async () => {
		const driver = new TestDriver({ processingGuarantee: 'exactly_once' })
		const out: unknown[] = []
		const grouped = driver.input('in', { key: str, value: str }).groupByKey()
		grouped
			.count()
			.toStream()
			.peek((k, v) => out.push([k, v]))
		grouped
			.windowedBy(SessionWindows.withInactivityGap(10))
			.reduce((a, b) => a + b)
			.toStream()
			.peek((k, v) => out.push([k?.window, v]))
		await driver.run(async ({ send }) => {
			await send('in', 'a', at('k', 0))
			await send('in', 'b', at('k', 5))
		})
		expect(out).toEqual([
			['k', 1],
			[{ start: 0, end: 0 }, 'a'],
			['k', 2],
			[{ start: 0, end: 0 }, null],
			[{ start: 0, end: 5 }, 'ab'],
		])
	})

	it('exactly-once merges uncommitted sessions in start order too (the order comes from the store)', async () => {
		const driver = new TestDriver({ processingGuarantee: 'exactly_once', commitEvery: 'stop' })
		const out: Array<string | null> = []
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap(100))
			.aggregate(
				() => '',
				(_k, v, agg) => agg + v,
				(_k, a, b) => a + b,
				{ value: str }
			)
			.toStream()
			.peek((_k, v) => out.push(v))
		await driver.run(async ({ send }) => {
			// Both sessions are only in the write buffer when the bridging record reads them.
			await send('in', 'c', at('k', 200))
			await send('in', 'a', at('k', 0))
			await send('in', 'b', at('k', 100))
		})
		expect(out[out.length - 1]).toBe('acb')
	})

	describe('codecs', () => {
		it('count() needs a key codec once the keys changed upstream', () => {
			const driver = new TestDriver()
			const grouped = driver
				.input('in', { key: str, value: str })
				.selectKey((_k, v) => v)
				.groupByKey()
			expect(() => grouped.count()).toThrow(TopologyError)
			expect(() => grouped.count()).toThrow('count() needs a key codec')
		})

		it('the repartitioning of a grouping needs the value codec of the Grouped options', () => {
			const driver = new TestDriver()
			const grouped = driver
				.input('in', { key: str, value: str })
				.selectKey((_k, v) => v)
				.mapValues(v => v)
				.groupByKey({ key: str })
			expect(() => grouped.count()).toThrow('groupBy()/groupByKey() needs a value codec')
		})

		it('a grouping is repartitioned with its Grouped key codec; Materialized.key keys the store (Kafka Streams)', async () => {
			const driver = new TestDriver()
			const upper: Codec<string> = {
				encode: s => Buffer.from(s.toUpperCase()),
				decode: b => b.toString().toLowerCase(),
			}
			const grouped = driver
				.input('in', { key: str, value: str })
				.groupBy((_k, v) => v, { key: str, value: str, name: 'by-value' })
			grouped.count({ key: upper, storeName: 'counts' })
			grouped.windowedBy(TimeWindows.of(10)).count({ key: upper, storeName: 'windows' })
			grouped.windowedBy(SessionWindows.withInactivityGap(10)).count({ key: upper, storeName: 'sessions' })
			await driver.run(async ({ send, producer }) => {
				await send('in', 'x', at('k', 1))
				const keys = (topic: string) => producer.messagesFor(topic).map(m => m.key?.toString())
				expect(keys('test-app-by-value-repartition')).toEqual(['x'])
				expect(keys('test-app-counts-changelog')).toEqual(['X'])
				// Window and session changelog keys: [key][start int64BE][end int64BE].
				const baseKeys = (topic: string) =>
					producer.messagesFor(topic).map(m => m.key?.subarray(0, -16).toString())
				expect(baseKeys('test-app-windows-changelog')).toEqual(['X'])
				expect(baseKeys('test-app-sessions-changelog')).toEqual(['X'])
			})
		})

		it('groupByKey() repartitions with its Grouped value codec, which the aggregation keeps', async () => {
			const driver = new TestDriver()
			const sums: unknown[] = []
			driver
				.input('in', { key: str, value: str })
				.selectKey((_k, v) => v)
				.mapValues(v => ({ n: v!.length }))
				.groupByKey({ key: str, value: codec.json<{ n: number }>(), name: 'by-value' })
				.reduce((a, b) => ({ n: a.n + b.n }))
				.toStream()
				.peek((k, v) => sums.push([k, v]))
			await driver.run(async ({ send, producer }) => {
				await send('in', 'xy', at('k', 1))
				await send('in', 'xy', at('k', 2))
				const values = producer.messagesFor('test-app-by-value-repartition').map(m => m.value?.toString())
				expect(values).toEqual(['{"n":2}', '{"n":2}'])
			})
			expect(sums).toEqual([
				['xy', { n: 2 }],
				['xy', { n: 4 }],
			])
		})

		it('count() keeps its counts with Materialized.value when given', async () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			const value = codec.json<number>()
			grouped.count({ value }).toStream().to('counts')
			grouped.windowedBy(TimeWindows.of(10)).count({ value }).toStream().to('windows')
			grouped.windowedBy(SessionWindows.withInactivityGap(10)).count({ value }).toStream().to('sessions')
			await driver.run(async ({ send, producer }) => {
				await send('in', 'x', at('k', 15))
				const values = (topic: string) => producer.messagesFor(topic).map(m => m.value?.toString())
				expect([values('counts'), values('windows'), values('sessions')]).toEqual([['1'], ['1'], ['1']])
			})
		})

		it('aggregate() keeps aggregates of the value type with the value codec, windowed or not', async () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			const append = (_k: string, v: string | null, text: string) => text + v
			grouped
				.aggregate(() => '', append)
				.toStream()
				.to('texts')
			grouped
				.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
				.aggregate(() => '', append)
				.toStream()
				.to('windows')
			grouped
				.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10))
				.aggregate(
					() => '',
					append,
					(_k, a, b) => a + b
				)
				.toStream()
				.to('sessions')
			grouped
				.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
				.aggregate(() => '', append)
				.toStream()
				.to('slides')
			await driver.run(async ({ send, producer }) => {
				await send('in', 'a', at('k', 15))
				await send('in', 'b', at('k', 16))
				const last = (topic: string) => producer.messagesFor(topic).at(-1)?.value?.toString()
				expect(['texts', 'windows', 'sessions', 'slides'].map(last)).toEqual(['ab', 'ab', 'ab', 'ab'])
			})
		})

		it('aggregate() needs a codec for its aggregates once a value-changing operator dropped the value codec', () => {
			const driver = new TestDriver()
			const lengths = driver
				.input('lengths', { key: str, value: str })
				.mapValues(v => v?.length ?? 0)
				.groupByKey()
			const add = (_k: string, v: number | null, sum: number) => sum + (v ?? 0)
			expect(() => lengths.aggregate(() => 0, add)).toThrow('aggregate() needs a codec for its aggregates')
			for (const windowed of [
				() => lengths.windowedBy(TimeWindows.of(10)).aggregate(() => 0, add),
				() =>
					lengths.windowedBy(SessionWindows.withInactivityGap(10)).aggregate(
						() => 0,
						add,
						(_k, a, b) => a + b
					),
				() => lengths.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10)).aggregate(() => 0, add),
			]) {
				expect(windowed).toThrow('windowed aggregate() needs a codec for its aggregates')
			}
		})

		it('reduce() needs a value codec once the values changed upstream', () => {
			const driver = new TestDriver()
			const grouped = driver
				.input('in', { key: str, value: str })
				.mapValues(v => v ?? '')
				.groupByKey()
			expect(() => grouped.reduce((a, b) => a + b)).toThrow('reduce() needs a value codec')
		})

		it("the table of an aggregation has the store codecs; windowed tables key by Windowed in Kafka Streams' formats", async () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			grouped.count().toStream().to('counts')
			grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count().toStream().to('windows')
			grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10)).count().toStream().to('sessions')
			await driver.run(async ({ send, output }) => {
				await send('in', 'x', at('k', 15))
				expect(output('counts', { key: str, value: num })).toEqual([
					{ topic: 'counts', key: 'k', value: 1, partition: undefined },
				])
				const [windowRecord] = output('windows', { value: num })
				// [key][start int64BE] (BC10)
				expect(windowRecord!.key).toEqual(Buffer.concat([Buffer.from('k'), int64(10)]))
				expect(windowRecord!.value).toBe(1)
				// [key][end int64BE][start int64BE]
				const [sessionRecord] = output('sessions', { value: num })
				expect(sessionRecord!.key).toEqual(Buffer.concat([Buffer.from('k'), int64(15), int64(15)]))
			})
		})
	})

	describe('stores', () => {
		it("unnamed aggregations keep today's store names, which name their changelogs", () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			const concat = (a: string, b: string | null) => a + b
			const append = (_k: string, v: string | null, agg: string) => agg + v
			grouped.count()
			grouped.reduce(concat)
			grouped.aggregate(() => '', append, { value: str })
			const hopping = grouped.windowedBy(TimeWindows.of(10).advanceBy(5))
			hopping.count()
			hopping.reduce(concat)
			hopping.aggregate(() => '', append, { value: str })
			const sessions = grouped.windowedBy(SessionWindows.withInactivityGap(10))
			sessions.count()
			sessions.reduce(concat)
			sessions.aggregate(
				() => '',
				append,
				(_k, a, b) => a + b,
				{ value: str }
			)

			// Store names are persisted state: they name the changelog topics and the local stores.
			const stores = topologyOf(driver).subtopologies.flatMap(s => s.stores)
			expect(stores.map(s => [s.name, s.logging.enabled && s.logging.topic])).toEqual(
				[
					'count-store-0',
					'reduce-store-1',
					'aggregate-store-2',
					'window-count-store-3',
					'window-reduce-store-4',
					'window-aggregate-store-5',
					'window-count-store-6',
					'window-reduce-store-7',
					'window-aggregate-store-8',
				].map(name => [name, `test-app-${name}-changelog`])
			)
			// A hopping window store is told the window size, not the advance.
			expect(stores.filter(s => s.kind === 'window').map(s => s.windowSizeMs)).toEqual([10, 10, 10])
		})

		it('the changelog option turns off or configures the changelog of every aggregation store', () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			grouped.count({ storeName: 'counts', changelog: false })
			grouped.windowedBy(TimeWindows.of(10)).count({ storeName: 'windows', changelog: false })
			grouped.windowedBy(SessionWindows.withInactivityGap(10)).count({ storeName: 'sessions', changelog: false })
			const changelog = { topicName: 'custom-changelog', topicConfigs: { 'min.insync.replicas': '2' } }
			grouped.count({ storeName: 'configured', changelog })

			const topology = topologyOf(driver)
			for (const name of ['counts', 'windows', 'sessions']) {
				const { store, subtopology } = storeOf(topology, name)
				expect(store.logging.enabled).toBe(false)
				expect(subtopology.changelogTopics.has(name)).toBe(false)
			}
			const { store } = storeOf(topology, 'configured')
			if (!store.logging.enabled) throw new Error('not logged')
			expect(store.logging.topic).toBe('custom-changelog')
			expect(store.logging.configs['min.insync.replicas']).toBe('2')
		})
	})

	describe('retention and emit options', () => {
		it('windowed stores keep size + grace or gap + grace by default; Materialized.retention keeps them longer', () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			grouped.windowedBy(TimeWindows.ofSizeAndGrace(10, 5)).count({ storeName: 'windows' })
			grouped.windowedBy(SessionWindows.ofInactivityGapAndGrace(10, 5)).count({ storeName: 'sessions' })
			grouped.windowedBy(TimeWindows.ofSizeAndGrace(10, 5)).count({ storeName: 'kept', retention: '1m' })
			grouped
				.windowedBy(SessionWindows.ofInactivityGapAndGrace(10, 5))
				.count({ storeName: 'kept-sessions', retention: 60_000 })

			const topology = topologyOf(driver)
			const retention = (name: string) => {
				const { store } = storeOf(topology, name)
				return [store.retentionMs, store.logging.enabled && store.logging.configs['retention.ms']]
			}
			expect(retention('windows')).toEqual([15, String(15 + 86_400_000)])
			expect(retention('sessions')).toEqual([15, String(15 + 86_400_000)])
			expect(retention('kept')).toEqual([60_000, String(60_000 + 86_400_000)])
			expect(retention('kept-sessions')).toEqual([60_000, String(60_000 + 86_400_000)])
		})

		it('a retention below the minimum fails the aggregation before it registers anything', () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupBy((_k, v) => v, { key: str })
			expect(() => grouped.windowedBy(TimeWindows.ofSizeAndGrace(10, 5)).count({ retention: 14 })).toThrow(
				new TopologyError(
					"The retention of state store 'window-count-store-0' must be no smaller than the window size (10 ms) " +
						'plus the grace period (5 ms), got 14 ms'
				)
			)
			expect(() =>
				grouped.windowedBy(SessionWindows.ofInactivityGapAndGrace(10, 5)).count({ retention: '10ms' })
			).toThrow(TopologyError)
			const topology = topologyOf(driver)
			expect(repartitionTopics(topology)).toEqual([])
			expect(topology.subtopologies.flatMap(s => s.stores)).toEqual([])
		})

		it("emit: 'on-window-close' makes the expiry of the store wait for the emission (6.8)", () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			const emit = 'on-window-close'
			grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count({ storeName: 'final-windows', emit })
			grouped
				.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10))
				.count({ storeName: 'final-sessions', emit })
			grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count({ storeName: 'windows', emit: 'on-update' })

			const topology = topologyOf(driver)
			const clock = (name: string) => storeOf(topology, name).store.clock
			expect(clock('final-windows')).toEqual({ emitFinal: { kind: 'time', sizeMs: 10 } })
			expect(clock('final-sessions')).toEqual({ emitFinal: { kind: 'session' } })
			expect(clock('windows')).toEqual({ emitFinal: undefined })
		})
	})

	describe('named groupings (KF6)', () => {
		it('B4: count() and reduce() of one named grouping share one repartition topic, each record counted once', async () => {
			const driver = new TestDriver()
			const grouped = driver
				.input('in', { key: str, value: str })
				.groupBy((_k, v) => v!.slice(0, 1), { key: str, value: str, name: 'by-initial' })
			const counts: unknown[] = []
			const reduced: unknown[] = []
			grouped
				.count()
				.toStream()
				.peek((k, v) => counts.push([k, v]))
			grouped
				.reduce((a, b) => `${a},${b}`)
				.toStream()
				.peek((k, v) => reduced.push([k, v]))
			grouped.windowedBy(TimeWindows.of(10)).count({ storeName: 'windowed' })

			const topology = topologyOf(driver)
			expect(repartitionTopics(topology)).toEqual(['test-app-by-initial-repartition'])
			const sinks = topology.subtopologies.flatMap(s => s.nodes).filter(n => n.kind === 'sink')
			expect(sinks).toHaveLength(1)

			await driver.run(async ({ send }) => {
				await send('in', 'apple', at('x', 1))
				await send('in', 'avocado', at('y', 2))
			})
			expect(counts).toEqual([
				['a', 1],
				['a', 2],
			])
			expect(reduced).toEqual([
				['a', 'apple'],
				['a', 'apple,avocado'],
			])
		})

		it('B4: two groupings with the same name -> TopologyError', () => {
			const driver = new TestDriver()
			const stream = driver.input('in', { key: str, value: str }).selectKey((_k, v) => v)
			stream.groupByKey({ key: str, name: 'same' }).count()
			expect(() => stream.groupByKey({ key: str, name: 'same' }).count()).toThrow(
				"The repartition topic name 'same' is already used by another operation"
			)
			// groupBy() gives its key-select node the grouping's name, as Kafka Streams does.
			stream.groupBy((_k, v) => v, { key: str, name: 'other' }).count()
			expect(() => stream.groupBy((_k, v) => v, { key: str, name: 'other' })).toThrow(TopologyError)
		})

		it('an unnamed grouping repartitions once per aggregation (Kafka Streams)', () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupBy((_k, v) => v, { key: str })
			grouped.count()
			grouped.reduce((a, b) => a + b)
			expect(repartitionTopics(topologyOf(driver))).toEqual([
				'test-app-repartition-0-repartition',
				'test-app-repartition-1-repartition',
			])
		})

		it('groupByKey of a stream whose keys did not change does not repartition', () => {
			const driver = new TestDriver()
			driver.input('in', { key: str, value: str }).groupByKey({ name: 'unused' }).count()
			expect(repartitionTopics(topologyOf(driver))).toEqual([])
		})
	})

	it('B7: one stream time per task: the windows a record of a lagging topic can still update are never expired', async () => {
		const driver = new TestDriver()
		const counts: Array<[string, number | null]> = []
		const a = driver.input('a', { key: str, value: str })
		const b = driver.input('b', { key: str, value: str })
		a.merge(b)
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeAndGrace(1000, 23_000))
			.count()
			.toStream()
			.peek((k, v) => counts.push([`${k!.key}@${k!.window.start}`, v]))
		await driver.run(async ({ send }) => {
			await send('b', 'x', at('k', 80_000))
			await send('a', 'x', at('k', 100_000)) // the task's clock: windows ending by 77 000 are closed
			await send('b', 'x', at('k', 80_500)) // [80 000, 81 000) is open: updated
			await send('a', 'x', at('k', 200_000)) // windows ending by 177 000 are closed
			await send('b', 'x', at('k', 80_700)) // closed: dropped, not re-created with a count of 1
		})
		expect(counts).toEqual([
			['k@80000', 1],
			['k@100000', 1],
			['k@80000', 2],
			['k@200000', 1],
		])
	})

	describe('old values and value getters', () => {
		it('a count sends its previous value once asked; its getter reads the latest value and timestamp', async () => {
			const driver = new TestDriver()
			const table = driver.input('in', { key: str, value: str }).groupByKey().count() as KTableImpl<
				string,
				number
			>
			expect(table.table.enableSendingOldValues(false)).toBe(true)
			const changes = changesOf(table)
			const looked = lookUp(driver, table, str)
			await driver.run(async ({ send }) => {
				await send('in', 'x', at('k', 1))
				await send('in', 'x', at('k', 2))
				await send('lookups', Buffer.from(''), key('k'))
				await send('lookups', Buffer.from(''), key('missing'))
			})
			expect(changes).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: 2, oldValue: 1 },
			])
			expect(looked).toEqual([{ value: 2, timestamp: 2 }, null])
		})

		it("B1: a count of a table's stream sends its own previous counts as old values, never the table's", async () => {
			const driver = new TestDriver()
			const table = driver.table('t', { key: str, value: str }).toStream().groupByKey().count() as KTableImpl<
				string,
				number
			>
			table.table.enableSendingOldValues(true)
			const changes = changesOf(table)
			await driver.run(async ({ send }) => {
				await send('t', 'a', at('k', 1))
				await send('t', 'b', at('k', 2))
			})
			expect(changes).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: 2, oldValue: 1 },
			])
		})

		it('without the request, changes carry no old value (session retractions neither)', async () => {
			const driver = new TestDriver()
			const grouped = driver.input('in', { key: str, value: str }).groupByKey()
			const changes = changesOf(grouped.count() as KTableImpl<string, number>)
			const windowChanges = changesOf(
				grouped.windowedBy(TimeWindows.of(10)).count() as KTableImpl<Windowed<string>, number>
			)
			const sessionChanges = changesOf(
				grouped.windowedBy(SessionWindows.withInactivityGap(10)).count() as KTableImpl<Windowed<string>, number>
			)
			await driver.run(async ({ send }) => {
				await send('in', 'x', at('k', 1))
				await send('in', 'x', at('k', 2)) // merges session [1,1] into [1,2]
			})
			const updates = [
				{ newValue: 1, oldValue: null },
				{ newValue: 2, oldValue: null },
			]
			expect(changes).toEqual(updates)
			expect(windowChanges).toEqual(updates)
			expect(sessionChanges).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: null, oldValue: null },
				{ newValue: 2, oldValue: null },
			])
		})

		it('a time-windowed count sends the previous value of the window; its getter reads one window', async () => {
			const driver = new TestDriver()
			const table = driver
				.input('in', { key: str, value: str })
				.groupByKey()
				.windowedBy(TimeWindows.of(10))
				.count() as KTableImpl<Windowed<string>, number>
			table.table.enableSendingOldValues(true)
			const changes = changesOf(table)
			const looked = lookUp(driver, table, json as Codec<Windowed<string>>)
			await driver.run(async ({ send }) => {
				await send('in', 'x', at('k', 11))
				await send('in', 'x', at('k', 12))
				await send('lookups', Buffer.from(''), {
					keyValue: { key: 'k', window: { start: 10, end: 20 } },
					keyCodec: json,
				})
				await send('lookups', Buffer.from(''), {
					keyValue: { key: 'k', window: { start: 0, end: 10 } },
					keyCodec: json,
				})
			})
			expect(changes).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: 2, oldValue: 1 },
			])
			// The window's timestamp is the max timestamp of its records (KS).
			expect(looked).toEqual([{ value: 2, timestamp: 12 }, null])
		})

		it('BC23: a session count retracts every merged session with its value, then sends the merged one without', async () => {
			const driver = new TestDriver()
			const table = driver
				.input('in', { key: str, value: str })
				.groupByKey()
				.windowedBy(SessionWindows.withInactivityGap(10))
				.count() as KTableImpl<Windowed<string>, number>
			table.table.enableSendingOldValues(true)
			const changes = changesOf(table)
			const looked = lookUp(driver, table, json as Codec<Windowed<string>>)
			await driver.run(async ({ send }) => {
				await send('in', 'x', at('k', 0))
				await send('in', 'x', at('k', 5)) // merges [0,0] into [0,5]
				await send('in', 'x', at('k', 3)) // inside [0,5]: merged with it, so retracted too (KS)
				await send('lookups', Buffer.from(''), {
					keyValue: { key: 'k', window: { start: 0, end: 5 } },
					keyCodec: json,
				})
			})
			expect(changes).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: null, oldValue: 1 },
				{ newValue: 2, oldValue: null },
				{ newValue: null, oldValue: 2 },
				{ newValue: 3, oldValue: null },
			])
			// A session's timestamp is its end (KS).
			expect(looked).toEqual([{ value: 3, timestamp: 5 }])
		})

		it("a record at a one-point session's timestamp updates it in place, its previous value the old value", async () => {
			const driver = new TestDriver()
			const table = driver
				.input('in', { key: str, value: str })
				.groupByKey()
				.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10))
				.count() as KTableImpl<Windowed<string>, number>
			table.table.enableSendingOldValues(true)
			const changes = changesOf(table)
			await driver.run(async ({ send }) => {
				await send('in', 'x', at('k', 100))
				// Not retracted: KS's record cache sends the stored aggregate as the old value of this update.
				await send('in', 'x', at('k', 100))
			})
			expect(changes).toEqual([
				{ newValue: 1, oldValue: null },
				{ newValue: 2, oldValue: 1 },
			])
		})
	})
})
