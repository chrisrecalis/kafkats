import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import { internals } from '@/app/internal.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import { codec, type Codec } from '@/codec.js'
import { DslContext } from '@/dsl/context.js'
import { COUNT_CODEC } from '@/dsl/ops/aggregate.js'
import { SlidingWindows } from '@/dsl/windows.js'
import { WindowedCodecs } from '@/operators/windowed.js'
import type { SessionStore, ValueAndTimestamp } from '@/state.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import { APP, TaskHarness } from '../support/task-harness.js'

const str = codec.string()
const list = codec.json<string[]>()
const S = { key: str, value: str }
const BASE = 1_000_000_000_000

/** A record: its value (also its header `n`), timestamp and key (default 'k'). */
type Input = readonly [value: string, timestamp: number, key?: string]
type Result<V> = [key: string, start: number, end: number, value: V | null, timestamp: number]
type Contents = Result<string[]>

/** The values of the records of each window of 'in' (sorted: whatever order they came in), written to 'out'. */
function contents(
	driver: TestDriver,
	windows: SlidingWindows,
	options: WindowedAggregateOptions<string, string[]> = {}
): void {
	driver
		.input('in', S)
		.groupByKey()
		.windowedBy(windows)
		.aggregate(
			() => [] as string[],
			(_key, value, records) => [...records, value].sort(),
			{
				value: list,
				...options,
			}
		)
		.toStream()
		.to('out', { value: list })
}

const send = (ctx: TestContextApi, [value, timestamp, key = 'k']: Input) =>
	ctx.send('in', value, { key: Buffer.from(key), codec: str, timestamp, headers: { n: Buffer.from(value) } })

/** What was written to 'out' by windows of time difference `d`: key, window, value and timestamp of each record. */
function results<V>(ctx: TestContextApi, d: number, values: Codec<V>): Result<V>[] {
	const keys = WindowedCodecs.timeWindowed(str, d)
	return ctx.producer.messagesFor('out').map(({ key, value, timestamp }) => {
		const { key: base, window } = keys.decode(key!)
		return [base, window.start, window.end, value === null ? null : values.decode(value), Number(timestamp)]
	})
}

/** Sends `records` to the topology `build` registers; returns what it wrote to 'out'. */
async function run<V>(
	config: TestDriverConfig,
	d: number,
	build: (driver: TestDriver) => void,
	records: readonly Input[],
	values: Codec<V>
): Promise<Result<V>[]> {
	const driver = new TestDriver(config)
	build(driver)
	let written: Result<V>[] = []
	await driver.run(async ctx => {
		for (const record of records) await send(ctx, record)
		written = results(ctx, d, values)
	})
	return written
}

/** The records of each window of `windows` after `records`, as each update reported them. */
const windowContents = (config: TestDriverConfig, windows: SlidingWindows, records: readonly Input[]) =>
	run(config, windows.timeDifferenceMs, driver => contents(driver, windows), records, list)

/**
 * The counts of the windows of each key, regrouped by key and summed (a KGroupedTable reduce, which subtracts each
 * window's old count), written to 'totals': the last one is the sum of the latest count of every window.
 */
async function regroupedTotals(config: TestDriverConfig, windows: SlidingWindows, records: readonly Input[]) {
	const driver = new TestDriver(config)
	driver
		.input('in', S)
		.groupByKey()
		.windowedBy(windows)
		.count()
		.groupBy((windowed, count) => [windowed.key, count], { key: str, value: COUNT_CODEC })
		.reduce(
			(total, count) => total + count,
			(total, count) => total - count
		)
		.toStream()
		.to('totals', { value: COUNT_CODEC })
	let totals: number[] = []
	await driver.run(async ctx => {
		for (const record of records) await send(ctx, record)
		totals = ctx.producer.messagesFor('totals').map(({ value }) => COUNT_CODEC.decode(value!))
	})
	return totals
}

/** The last result of each window, by start. */
function latest<V>(results: readonly Result<V>[]): Result<V>[] {
	const byWindow = new Map(results.map(result => [`${result[0]}/${result[1]}`, result]))
	return [...byWindow.values()].sort((a, b) => a[1] - b[1])
}

function permutations<T>(items: readonly T[]): T[][] {
	if (items.length <= 1) return [[...items]]
	return items.flatMap((item, i) =>
		permutations([...items.slice(0, i), ...items.slice(i + 1)]).map(rest => [item, ...rest])
	)
}

/** The Kafka Streams javadoc example of SlidingWindows (`SlidingWindows.java:41-60`). */
const JAVADOC: readonly Input[] = [
	['a', 8000],
	['b', 9200],
	['c', 12400],
]
const JAVADOC_WINDOWS: Contents[] = [
	['k', 3000, 8000, ['a'], 8000],
	['k', 4200, 9200, ['a', 'b'], 9200],
	['k', 7400, 12400, ['a', 'b', 'c'], 12400],
	['k', 8001, 13001, ['b', 'c'], 12400],
	['k', 9201, 14201, ['c'], 12400],
]

describe.each(GUARANTEES)('sliding-window aggregation (%s)', (_, config) => {
	it('builds the windows of the Kafka Streams javadoc example, each result at the max timestamp of its records', async () => {
		const driver = new TestDriver(config)
		contents(driver, SlidingWindows.ofTimeDifferenceWithNoGrace(5000))
		await driver.run(async ctx => {
			for (const record of JAVADOC) await send(ctx, record)
			expect(results(ctx, 5000, list)).toEqual([
				['k', 3000, 8000, ['a'], 8000], // a's left window
				['k', 8001, 13001, ['b'], 9200], // a's right window, created by b
				['k', 4200, 9200, ['a', 'b'], 9200], // b's left window, from a's
				['k', 8001, 13001, ['b', 'c'], 12400],
				['k', 9201, 14201, ['c'], 12400], // b's right window, created by c
				['k', 7400, 12400, ['a', 'b', 'c'], 12400], // c's left window, from b's
			])
			// Every result carries the headers of the record that produced it.
			expect(ctx.producer.messagesFor('out').map(m => m.headers?.n?.toString())).toEqual([
				'a',
				'b',
				'b',
				'c',
				'c',
				'c',
			])
		})
	})

	it('count() and reduce() over the javadoc example', async () => {
		const windows = SlidingWindows.ofTimeDifferenceWithNoGrace(5000)
		const counts = await run(
			config,
			5000,
			driver => driver.input('in', S).groupByKey().windowedBy(windows).count().toStream().to('out'),
			JAVADOC,
			COUNT_CODEC
		)
		expect(counts.map(([, start, , count, timestamp]) => [start, count, timestamp])).toEqual([
			[3000, 1, 8000],
			[8001, 1, 9200],
			[4200, 2, 9200],
			[8001, 2, 12400],
			[9201, 1, 12400],
			[7400, 3, 12400],
		])
		const reduced = await run(
			config,
			5000,
			driver =>
				driver
					.input('in', S)
					.groupByKey()
					.windowedBy(windows)
					.reduce((aggregate, value) => aggregate + value)
					.toStream()
					.to('out'),
			JAVADOC,
			str
		)
		expect(reduced.map(([, start, , value]) => [start, value])).toEqual([
			[3000, 'a'],
			[8001, 'b'],
			[4200, 'ab'],
			[8001, 'bc'],
			[9201, 'c'],
			[7400, 'abc'],
		])
	})

	it('a record before its predecessors updates the windows containing it and copies the next records into its right window', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(5000, 3200), [
				['a', 8000],
				['c', 12400],
				['b', 9200], // stream time 12400: windows ending at 9200 or later still take records
			])
		).toEqual([
			['k', 3000, 8000, ['a'], 8000],
			['k', 8001, 13001, ['c'], 12400],
			['k', 7400, 12400, ['a', 'c'], 12400],
			['k', 7400, 12400, ['a', 'b', 'c'], 12400],
			['k', 8001, 13001, ['b', 'c'], 12400],
			['k', 4200, 9200, ['a', 'b'], 9200], // b's left window, at the grace boundary
			['k', 9201, 14201, ['c'], 12400], // b's right window: a copy of the window holding c
		])
	})

	it('records in any order within the grace period give the same windows', async () => {
		const windows = SlidingWindows.ofTimeDifferenceAndGrace(5000, 10_000)
		for (const records of permutations(JAVADOC)) {
			expect(latest(await windowContents(config, windows, records))).toEqual(JAVADOC_WINDOWS)
		}
	})

	it('records before the time difference share the window [0, d] as their left window (Kafka Streams processEarly)', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(10), [
				['a', 2],
				['b', 5],
				['c', 12],
			])
		).toEqual([
			['k', 0, 10, ['a'], 2],
			['k', 3, 13, ['b'], 5], // a's right window
			['k', 0, 10, ['a', 'b'], 5],
			['k', 3, 13, ['b', 'c'], 12],
			['k', 6, 16, ['c'], 12], // b's right window
			['k', 2, 12, ['a', 'b', 'c'], 12], // c's left window, from [0, 10]
		])
	})

	it('early records in any order within the grace period give the same windows', async () => {
		const expected: Contents[] = [
			['k', 0, 10, ['a', 'b'], 5],
			['k', 2, 12, ['a', 'b', 'c'], 12],
			['k', 3, 13, ['b', 'c'], 12],
			['k', 6, 16, ['c'], 12],
		]
		const windows = SlidingWindows.ofTimeDifferenceAndGrace(10, 100)
		const early: Input[] = [
			['a', 2],
			['b', 5],
			['c', 12],
		]
		for (const records of permutations(early)) {
			expect(latest(await windowContents(config, windows, records))).toEqual(expected)
		}
	})

	it('an early record before a later early record copies [0, d] into its right window', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(10, 100), [
				['b', 8],
				['a', 3],
			])
		).toEqual([
			['k', 0, 10, ['b'], 8],
			['k', 4, 14, ['b'], 8], // a's right window: [0, 10] held only records after a
			['k', 0, 10, ['a', 'b'], 8],
		])
	})

	it('KF2: a record exactly d after the previous one reads its left window, though it starts before stream time minus the retention', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(5), [
				['a', 100],
				['b', 105], // reads [95, 100]: the store clock (window start 95), not stream time, bounds what it reads
			])
		).toEqual([
			['k', 95, 100, ['a'], 100],
			['k', 101, 106, ['b'], 105], // a's right window
			['k', 100, 105, ['a', 'b'], 105], // b's left window, from a's left window
		])
	})

	it('a record more than d after the previous one opens its own left window and no right window for it', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(10, 100), [
				['a', 100],
				['b', 102],
				['c', 115], // finds [101, 111] (b alone): b's right window [103, 113] would not hold c
			])
		).toEqual([
			['k', 90, 100, ['a'], 100],
			['k', 101, 111, ['b'], 102],
			['k', 92, 102, ['a', 'b'], 102],
			['k', 105, 115, ['c'], 115],
		])
	})

	it("a record d + 1 after the previous one creates the previous record's right window, its left window, once", async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(5), [
				['a', 30],
				['b', 35],
				['c', 41],
			])
		).toEqual([
			['k', 25, 30, ['a'], 30],
			['k', 31, 36, ['b'], 35], // a's right window
			['k', 30, 35, ['a', 'b'], 35],
			['k', 36, 41, ['c'], 41], // b's right window and c's left window: one update (KS: two, merged by its cache)
		])
	})

	it('counts regrouped by key sum the latest count of every window, each window counted once', async () => {
		// Windows [25, 30], [31, 36], [30, 35] (2), [36, 41]: c is d + 1 after b.
		const noGrace = SlidingWindows.ofTimeDifferenceWithNoGrace(5)
		expect(
			await regroupedTotals(config, noGrace, [
				['a', 30],
				['b', 35],
				['c', 41],
			])
		).toEqual([1, 2, 4, 5])
		// Windows [90, 100], [101, 111], [92, 102] (2), [103, 113]: c is d + 1 after b.
		const grace = SlidingWindows.ofTimeDifferenceAndGrace(10, 100)
		expect(
			await regroupedTotals(config, grace, [
				['a', 100],
				['b', 102],
				['c', 113],
			])
		).toEqual([1, 2, 4, 5])
	})

	it('a record at a timestamp seen before finds its right window and leaves it alone', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(10, 100), [
				['a', 100],
				['c', 105],
				['b', 100], // [101, 111], a's right window, holds c only
			])
		).toEqual([
			['k', 90, 100, ['a'], 100],
			['k', 101, 111, ['c'], 105],
			['k', 95, 105, ['a', 'c'], 105],
			['k', 90, 100, ['a', 'b'], 100],
			['k', 95, 105, ['a', 'b', 'c'], 105],
		])
	})

	it('records of one timestamp share their windows', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(10), [
				['a', 100],
				['b', 105],
				['c', 105], // no record after it: no right window
			])
		).toEqual([
			['k', 90, 100, ['a'], 100],
			['k', 101, 111, ['b'], 105],
			['k', 95, 105, ['a', 'b'], 105],
			['k', 95, 105, ['a', 'b', 'c'], 105],
			['k', 101, 111, ['b', 'c'], 105],
		])
	})

	it('a late record updates the windows still open and is dropped for the closed ones, or entirely once all closed', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(10), [
				['a', 100],
				['b', 200],
				['c', 189], // 189 + 10 < 200: every window of c closed
				['d', 190], // its left window [180, 190] closed; [190, 200] still open
			])
		).toEqual([
			['k', 90, 100, ['a'], 100],
			['k', 190, 200, ['b'], 200],
			['k', 190, 200, ['b', 'd'], 200],
			['k', 191, 201, ['b'], 200], // d's right window
		])
	})

	it('a window takes records while its end is at least stream time minus grace', async () => {
		const records: Input[] = [
			['a', 8000],
			['c', 12400],
			['b', 9200],
		]
		const updatedBy = (results: Contents[]) => results.slice(3).map(([, start, end]) => [start, end])
		const atBoundary = await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(5000, 3200), records)
		const past = await windowContents(config, SlidingWindows.ofTimeDifferenceAndGrace(5000, 3199), records)
		expect(updatedBy(atBoundary)).toEqual([
			[7400, 12400],
			[8001, 13001],
			[4200, 9200],
			[9201, 14201],
		])
		expect(updatedBy(past)).toEqual([
			[7400, 12400],
			[8001, 13001],
			[9201, 14201],
		])
	})

	it('drops records with a null key or a null value (Kafka Streams)', async () => {
		const driver = new TestDriver(config)
		contents(driver, SlidingWindows.ofTimeDifferenceWithNoGrace(10))
		await driver.run(async ctx => {
			await ctx.send('in', 'a', { key: null, codec: str, timestamp: 100 })
			await ctx.send('in', null, { key: Buffer.from('k'), timestamp: 101 })
			await send(ctx, ['b', 102])
			expect(results(ctx, 10, list)).toEqual([['k', 92, 102, ['b'], 102]])
		})
	})

	it('keeps the windows of each key apart', async () => {
		expect(
			await windowContents(config, SlidingWindows.ofTimeDifferenceWithNoGrace(10), [
				['a', 100, 'x'],
				['b', 105, 'y'],
				['c', 108, 'x'],
			])
		).toEqual([
			['x', 90, 100, ['a'], 100],
			['y', 95, 105, ['b'], 105],
			['x', 101, 111, ['c'], 108],
			['x', 98, 108, ['a', 'c'], 108],
		])
	})
})

describe.each(GUARANTEES)('sliding windows emitted on window close (%s)', (_, config) => {
	const FINAL = { emit: 'on-window-close' } as const

	it('emits each window once, once stream time passed its end plus grace, with its max timestamp', async () => {
		const windows = SlidingWindows.ofTimeDifferenceWithNoGrace(10)
		const driver = new TestDriver(config)
		contents(driver, windows, FINAL)
		await driver.run(async ctx => {
			await send(ctx, ['a', 100])
			await send(ctx, ['b', 105]) // closes [90, 100]
			expect(results(ctx, 10, list)).toEqual([['k', 90, 100, ['a'], 100]])
			ctx.clear()
			await send(ctx, ['c', 130]) // closes [95, 105] and [101, 111]
			await send(ctx, ['d', 131]) // closes [120, 130]
			expect(results(ctx, 10, list)).toEqual([
				['k', 95, 105, ['a', 'b'], 105],
				['k', 101, 111, ['b'], 105],
				['k', 120, 130, ['c'], 130],
			])
			// The headers are those of the record whose stream time closed the window (KS).
			expect(ctx.producer.messagesFor('out').map(m => m.headers?.n?.toString())).toEqual(['c', 'c', 'd'])
		})
	})

	it('KF1: the store expires no window before it was emitted', async () => {
		const driver = new TestDriver({ ...config, emitIntervalMs: 1000, initialWallClockTimeMs: 0 })
		contents(driver, SlidingWindows.ofTimeDifferenceWithNoGrace(10), { ...FINAL, storeName: 'w' })
		await driver.run(async ctx => {
			await send(ctx, ['a', BASE + 5]) // the store keeps 20 ms of windows; expiry runs every 60 s of stream time
			await send(ctx, ['b', BASE + 70_000]) // throttled; expiry runs: [BASE - 5, BASE + 5] is past retention
			await ctx.advanceWallClockTime(1000)
			await send(ctx, ['c', BASE + 70_001])
			expect(results(ctx, 10, list)).toEqual([
				['k', BASE - 5, BASE + 5, ['a'], BASE + 5],
				['k', BASE + 69_990, BASE + 70_000, ['b'], BASE + 70_000],
			])
			// Past retention, it reads as absent.
			const store = ctx.getStateStore<SessionStore<string, unknown>>('w')
			expect(await store.get({ key: 'k', windowStart: BASE - 5, windowEnd: BASE + 5 })).toBeUndefined()
		})
	})
})

describe('sliding windows emitted on window close across a restart', () => {
	const keys = WindowedCodecs.timeWindowed(str, 10)

	/** A task of final sliding-window counts with time difference 10, and the start and count of what it emitted. */
	function finalCounts(eos: boolean) {
		const h = new TaskHarness({
			eos,
			build: builder => {
				const ctx = new DslContext(builder, { applicationId: APP })
				ctx.stream('in', S)
					.groupByKey()
					.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
					.count({ emit: 'on-window-close' })
					.toStream()
					.to('out', { key: keys, value: COUNT_CODEC })
			},
		})
		const emitted = () =>
			h.producer.committedRecords('out').map(({ message }) => {
				const { window }: Windowed<string> = keys.decode(message.key!)
				return [window.start, COUNT_CODEC.decode(message.value!)]
			})
		return { h, emitted }
	}

	for (const eos of [false, true]) {
		it(`${eos ? 'exactly-once' : 'at-least-once'}: a window emitted before the commit is not emitted again`, async () => {
			const { h, emitted } = finalCounts(eos)
			const first = await h.start(h.task())
			h.enqueue(first, 'in', [
				{ key: 'k', value: 'v', timestamp: 100 },
				{ key: 'k', value: 'v', timestamp: 130 }, // emits [90, 100]
			])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			// The re-created task continues from the emit progress committed with the offsets.
			const restarted = await h.restart()
			h.enqueue(restarted, 'in', [{ key: 'k', value: 'v', timestamp: 150 }]) // emits [120, 130]
			await h.process(restarted)
			expect(await h.commit()).toBe('committed')
			expect(emitted()).toEqual([
				[90, 1],
				[120, 1],
			])
		})
	}

	it('KF1, at-least-once: a window emitted after the last commit is emitted again by the revived task', async () => {
		const { h, emitted } = finalCounts(false)
		const first = await h.start(h.task())
		h.enqueue(first, 'in', [{ key: 'k', value: 'v', timestamp: 100 }])
		await h.process(first)
		expect(await h.commit()).toBe('committed')
		// Closes [90, 100], and its expiry pass runs with the store clock past the retention of [90, 100].
		const closing = { key: 'k', value: 'v', timestamp: 70_100 }
		h.enqueue(first, 'in', [closing])
		await h.process(first)
		// Revived as after a failed send: the replay emits [90, 100] again.
		const revived = await h.revive(first)
		h.enqueue(revived, 'in', [closing])
		await h.process(revived)
		expect(emitted()).toEqual([
			[90, 1],
			[90, 1],
		])
	})
})

describe('sliding-window aggregation store and results', () => {
	it('keeps its windows [s, s + d] in a timestamped session-layout store logged with 2d + grace plus a day', async () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace('10s', '5s'))
			.count({ storeName: 'w' })
		const store = internals(driver.flow)
			.build()
			.subtopologies.flatMap(s => s.stores)
			.find(s => s.name === 'w')!
		expect([store.kind, store.timestamped, store.retentionMs]).toEqual(['session', true, 25_000])
		if (!store.logging.enabled) throw new Error('not logged')
		expect(store.logging.configs['cleanup.policy']).toBe('delete,compact')
		expect(store.logging.configs['retention.ms']).toBe(String(25_000 + 86_400_000))

		await driver.run(async ctx => {
			await send(ctx, ['a', 20_000])
			const windows = ctx.getStateStore<SessionStore<string, ValueAndTimestamp<number>>>('w')
			expect(await windows.get({ key: 'k', windowStart: 10_000, windowEnd: 20_000 })).toEqual({
				value: 1,
				timestamp: 20_000,
			})
			const [logged] = ctx.producer.messagesFor('test-app-w-changelog')
			expect(windowedKeyCodec(str).decode(logged!.key!)).toEqual({
				key: 'k',
				windowStart: 10_000,
				windowEnd: 20_000,
			})
			expect(logged!.value).toEqual(Buffer.concat([COUNT_CODEC.encode(20_000), COUNT_CODEC.encode(1)]))
		})
	})

	it('results are keyed in the time-windowed format with size d, so their end is start + d', async () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
			.count()
			.toStream()
			.to('out')
		await driver.run(async ctx => {
			await send(ctx, ['a', 100])
			const [result] = ctx.producer.messagesFor('out')
			expect(result!.key).toEqual(Buffer.concat([Buffer.from('k'), COUNT_CODEC.encode(90)]))
		})
	})

	it('BC10: key-based operations on the results do not repartition them', () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(10))
			.count({ storeName: 'w' })
			.toStream()
			.groupByKey()
			.count({ storeName: 'updates' })
		const topology = internals(driver.flow).build()
		expect([...topology.internalTopics.values()].filter(t => t.kind === 'repartition')).toEqual([])
		const owner = (name: string) => topology.subtopologies.find(s => s.stores.some(store => store.name === name))
		expect(owner('updates')).toBe(owner('w'))
	})

	it('validates its options before registering anything', () => {
		const driver = new TestDriver()
		const windowed = driver.input('in', S).groupByKey().windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(10, 5))
		expect(() => windowed.count({ storeName: 'w', retention: 24 })).toThrow(
			new TopologyError(
				"The retention of state store 'w' must be no smaller than twice the time difference (10 ms) plus the " +
					'grace period (5 ms), got 24 ms'
			)
		)
		expect(() =>
			windowed.aggregate(
				() => 0,
				(_key, _value, n) => n + 1
			)
		).toThrow(
			new TopologyError('windowed aggregate() needs a codec for its aggregates: pass one as the value option')
		)
		expect(
			internals(driver.flow)
				.build()
				.subtopologies.flatMap(s => s.stores)
		).toEqual([])
	})
})
