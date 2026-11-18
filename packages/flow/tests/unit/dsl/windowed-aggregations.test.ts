import { describe, expect, it } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import type { WindowedAggregateOptions } from '@/api/options.js'
import { codec } from '@/codec.js'
import { DslContext } from '@/dsl/context.js'
import { SessionWindows, TimeWindows } from '@/dsl/windows.js'
import { WindowedCodecs, type WindowedCodec } from '@/operators/windowed.js'
import type { RecordHeaders } from '@/processor/record.js'
import type { WindowStore } from '@/state.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'
import type { TestRecord } from '../support/fake-io.js'
import { APP, TaskHarness } from '../support/task-harness.js'

const str = codec.string()
const num = codec.json<number>()
const S = { key: str, value: str }
const BASE = 1_000_000_000_000
const FINAL = { emit: 'on-window-close', value: num } satisfies WindowedAggregateOptions<string, number>

type Result = [key: string, start: number, end: number, value: number | null, timestamp: number]

/** Counts of the records of topic 'in' in `windows`, written to 'out' with the windowed codec of the windows. */
function counts(
	driver: TestDriver,
	windows: TimeWindows | SessionWindows,
	options: WindowedAggregateOptions<string, number> = { value: num }
): WindowedCodec<string> {
	const grouped = driver.input('in', S).groupByKey()
	if (windows instanceof SessionWindows) {
		grouped.windowedBy(windows).count(options).toStream().to('out', { value: num })
		return WindowedCodecs.sessionWindowed(str)
	}
	grouped.windowedBy(windows).count(options).toStream().to('out', { value: num })
	return WindowedCodecs.timeWindowed(str, windows.sizeMs)
}

/** What was written to 'out': key, window, value and timestamp of each record. */
function results(ctx: TestContextApi, keys: WindowedCodec<string>): Result[] {
	return ctx.producer.messagesFor('out').map(({ key, value, timestamp }) => {
		const { key: base, window } = keys.decode(key!)
		return [base, window.start, window.end, value === null ? null : num.decode(value), Number(timestamp)]
	})
}

const send = (ctx: TestContextApi, key: string, timestamp: number, headers?: RecordHeaders) =>
	ctx.send('in', 'v', { key: Buffer.from(key), codec: str, timestamp, headers })

/** Sends `records` (key, timestamp) to the counts of `windows`; returns what they wrote. */
async function run(
	config: TestDriverConfig,
	windows: TimeWindows | SessionWindows,
	records: readonly (readonly [key: string, timestamp: number])[],
	options?: WindowedAggregateOptions<string, number>
): Promise<Result[]> {
	const driver = new TestDriver(config)
	const keys = counts(driver, windows, options)
	let written: Result[] = []
	await driver.run(async ctx => {
		for (const [key, timestamp] of records) await send(ctx, key, timestamp)
		written = results(ctx, keys)
	})
	return written
}

describe.each(GUARANTEES)('windowed aggregation results (%s)', (_, config) => {
	it('BC9: a time window result carries the max timestamp of the records in the window', async () => {
		expect(
			await run(config, TimeWindows.ofSizeAndGrace(10, 100), [
				['k', 15],
				['k', 12],
				['k', 18],
			])
		).toEqual([
			['k', 10, 20, 1, 15],
			['k', 10, 20, 2, 15],
			['k', 10, 20, 3, 18],
		])
	})

	it('BC9: each hopping window carries the max timestamp of its own records', async () => {
		expect(
			await run(config, TimeWindows.ofSizeAndGrace(10, 100).advanceBy(5), [
				['k', 7],
				['k', 12],
				['k', 3],
			])
		).toEqual([
			['k', 0, 10, 1, 7],
			['k', 5, 15, 1, 7],
			['k', 5, 15, 2, 12],
			['k', 10, 20, 1, 12],
			['k', 0, 10, 2, 7],
		])
	})
})

describe.each(GUARANTEES)('windowed counts regrouped by key (%s)', (_, config) => {
	it('summed session counts end at the sum of the sessions when records share a timestamp', async () => {
		const driver = new TestDriver(config)
		driver
			.input('in', S)
			.groupByKey()
			.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(10))
			.count({ value: num })
			.groupBy((windowed, count) => [windowed.key, count], { key: str, value: num })
			.reduce(
				(total, count) => total + count,
				(total, count) => total - count
			)
			.toStream()
			.to('totals', { value: num })
		await driver.run(async ctx => {
			for (const timestamp of [100, 100, 105, 100, 130, 130]) await send(ctx, 'a', timestamp)
			expect(ctx.producer.messagesFor('totals').map(({ value }) => num.decode(value!))).toEqual([
				1, // [100,100] = 1
				2, // [100,100] = 2, updated in place
				0, // [100,100] retracted
				3, // [100,105] = 3
				0, // [100,105] retracted
				4, // [100,105] = 4
				5, // [130,130] = 1
				6, // [130,130] = 2, updated in place
			])
		})
	})
})

describe.each(GUARANTEES)('emit on window close (%s)', (_, config) => {
	it('KF1: time windows are emitted once each, once closed, with their max timestamp; updates are not', async () => {
		const driver = new TestDriver(config)
		const keys = counts(driver, TimeWindows.ofSizeWithNoGrace(10), FINAL)
		const closing = { trace: Buffer.from('closing') }
		await driver.run(async ctx => {
			await send(ctx, 'k', 5)
			await send(ctx, 'k', 7)
			await send(ctx, 'j', 9)
			await send(ctx, 'k', 25, closing) // closes [0,10)
			expect(results(ctx, keys)).toEqual([
				['k', 0, 10, 2, 7],
				['j', 0, 10, 1, 9],
			])
			// The headers are those of the record whose stream time closed the window (KS).
			expect(ctx.producer.messagesFor('out').map(m => m.headers)).toEqual([closing, closing])
			ctx.clear()
			await send(ctx, 'k', 45) // closes [20,30); [10,20) never had a record
			await send(ctx, 'k', 46) // nothing new closed
			expect(results(ctx, keys)).toEqual([['k', 20, 30, 1, 25]])
		})
	})

	it('a time window is emitted once stream time passed its end plus grace; later records for it are dropped', async () => {
		expect(
			await run(
				config,
				TimeWindows.ofSizeAndGrace(10, 5),
				[
					['k', 5],
					['k', 14], // stream time 14: [0,10) is open until 15
					['k', 8],
					['k', 15], // closes [0,10)
					['k', 9], // late: dropped
				],
				FINAL
			)
		).toEqual([['k', 0, 10, 2, 8]])
	})

	it('a time window ending exactly where the last emission stopped is not emitted again', async () => {
		expect(
			await run(
				config,
				TimeWindows.ofSizeWithNoGrace(10),
				[
					['k', 5],
					['k', 15], // closes [0,10)
					['k', 20], // closes [10,20): the next scan starts after it
					['k', 35], // closes [20,30)
				],
				FINAL
			)
		).toEqual([
			['k', 0, 10, 1, 5],
			['k', 10, 20, 1, 15],
			['k', 20, 30, 1, 20],
		])
	})

	it('a session ending at the close time is still open, and emitted once it closed', async () => {
		const written = await run(
			config,
			SessionWindows.ofInactivityGapWithNoGrace(10),
			[
				['k', 0],
				['j', 10], // close time 0: [k,0,0] may still take a record at 10
				['k', 10], // a new session: [k,0,0] lies below the store's read floor (1)
				['j', 40], // close time 30
			],
			FINAL
		)
		// Sessions ending at the same time come out in store order, which differs between the stores.
		expect(written.sort((a, b) => a[2] - b[2] || a[0].localeCompare(b[0]))).toEqual([
			['k', 0, 0, 1, 0],
			['j', 10, 10, 1, 10],
			['k', 10, 10, 1, 10],
		])
	})

	it('a session ending at the close time of the last emission is emitted by the next one', async () => {
		expect(
			await run(
				config,
				SessionWindows.ofInactivityGapWithNoGrace(10),
				[
					['a', 20],
					['b', 30], // close time 20: [a,20,20] is still open
					['b', 31], // close time 21
				],
				FINAL
			)
		).toEqual([['a', 20, 20, 1, 20]])
	})

	it('sessions are emitted once closed, at their end; merged-away sessions never are', async () => {
		expect(
			await run(
				config,
				SessionWindows.ofInactivityGapWithNoGrace(10),
				[
					['a', 0],
					['a', 5], // merges [0,0] into [0,5]
					['b', 30], // stream time 30: sessions ending before 20 are closed
					['a', 50], // closes [30,30]
				],
				FINAL
			)
		).toEqual([
			['a', 0, 5, 2, 5],
			['b', 30, 30, 1, 30],
		])
	})

	it('emits at most once per emitIntervalMs of wall clock, then every window closed meanwhile', async () => {
		const driver = new TestDriver({ ...config, emitIntervalMs: 1000, initialWallClockTimeMs: 0 })
		const keys = counts(driver, TimeWindows.ofSizeWithNoGrace(10), FINAL)
		await driver.run(async ctx => {
			await send(ctx, 'k', 5) // emits what closed (nothing); the next emission is due at 1000
			await send(ctx, 'k', 25)
			await send(ctx, 'k', 45)
			expect(results(ctx, keys)).toEqual([])
			await ctx.advanceWallClockTime(999)
			await send(ctx, 'k', 46)
			expect(results(ctx, keys)).toEqual([])
			await ctx.advanceWallClockTime(1)
			await send(ctx, 'k', 47)
			expect(results(ctx, keys)).toEqual([
				['k', 0, 10, 1, 5],
				['k', 20, 30, 1, 25],
			])
		})
	})

	it('KF1: the store expires no window before it was emitted, whatever its retention', async () => {
		const driver = new TestDriver({ ...config, emitIntervalMs: 1000, initialWallClockTimeMs: 0 })
		const keys = counts(driver, TimeWindows.ofSizeWithNoGrace(10), { ...FINAL, storeName: 'w' })
		await driver.run(async ctx => {
			await send(ctx, 'k', BASE + 5) // the store keeps 10 ms of windows; expiry runs every 60 s of stream time
			await send(ctx, 'k', BASE + 25) // throttled: nothing emitted until the wall clock moves
			await send(ctx, 'k', BASE + 70_000) // expiry runs: both windows are past retention, not emitted yet
			await ctx.advanceWallClockTime(1000)
			await send(ctx, 'k', BASE + 70_001)
			expect(results(ctx, keys)).toEqual([
				['k', BASE, BASE + 10, 1, BASE + 5],
				['k', BASE + 20, BASE + 30, 1, BASE + 25],
			])
			// Past retention, they read as absent.
			const store = ctx.getStateStore<WindowStore<string, unknown>>('w')
			expect(await store.get({ key: 'k', windowStart: BASE, windowEnd: BASE + 10 })).toBeUndefined()
		})
	})
})

describe('emit on window close across a restart', () => {
	const keys = WindowedCodecs.timeWindowed(str, 10)

	for (const eos of [false, true]) {
		it(`${eos ? 'exactly-once' : 'at-least-once'}: a window emitted before the commit is not emitted again`, async () => {
			const h = new TaskHarness({
				eos,
				build: builder => {
					const ctx = new DslContext(builder, { applicationId: APP })
					ctx.stream('in', S)
						.groupByKey()
						.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
						.count(FINAL)
						.toStream()
						.to('out', { key: keys, value: num })
				},
			})
			const emitted = () =>
				h.producer.committedRecords('out').map(({ message }) => {
					const { window }: Windowed<string> = keys.decode(message.key!)
					return [window.start, num.decode(message.value!)]
				})
			const first = await h.start(h.task())
			h.enqueue(first, 'in', [
				{ key: 'k', value: 'v', timestamp: 5 },
				{ key: 'k', value: 'v', timestamp: 25 }, // emits [0,10)
			])
			await h.process(first)
			expect(await h.commit('revocation')).toBe('committed')
			await first.suspend()
			expect(await first.close('clean')).toBe('clean')

			// The re-created task continues from the emit progress committed with the offsets.
			const restarted = await h.restart()
			h.enqueue(restarted, 'in', [{ key: 'k', value: 'v', timestamp: 45 }]) // emits [20,30)
			await h.process(restarted)
			expect(await h.commit()).toBe('committed')
			expect(emitted()).toEqual([
				[0, 1],
				[20, 1],
			])
		})
	}
})

describe('at-least-once revive after an expiry pass (4.7.5)', () => {
	/** A task counting the records of 'in' per key in `windows` into 'out', and what it sent there from `from` on. */
	function counting(windows: TimeWindows | SessionWindows, options: WindowedAggregateOptions<string, number>) {
		const keys: WindowedCodec<string> =
			windows instanceof SessionWindows
				? WindowedCodecs.sessionWindowed(str)
				: WindowedCodecs.timeWindowed(str, windows.sizeMs)
		const h = new TaskHarness({
			build: builder => {
				const grouped = new DslContext(builder, { applicationId: APP }).stream('in', S).groupByKey()
				const table =
					windows instanceof SessionWindows
						? grouped.windowedBy(windows).count(options)
						: grouped.windowedBy(windows).count(options)
				table.toStream().to('out', { key: keys, value: num })
			},
		})
		const sent = (from: number) =>
			h.producer.sent
				.slice(from)
				.filter(({ topic }) => topic === 'out')
				.map(({ message }) => {
					const { key, window }: Windowed<string> = keys.decode(message.key!)
					return [key, window.start, window.end, message.value === null ? null : num.decode(message.value)]
				})
		return { h, sent }
	}

	/**
	 * Processes `committed` and commits, then `uncommitted`, whose expiry pass runs past the retention of the records
	 * before; revives the task as a failed send would and processes `uncommitted` again. Returns what each run of
	 * `uncommitted` sent.
	 */
	async function replay(
		{ h, sent }: ReturnType<typeof counting>,
		committed: readonly TestRecord[],
		uncommitted: readonly TestRecord[]
	) {
		const first = await h.start(h.task())
		h.enqueue(first, 'in', committed)
		await h.process(first)
		expect(await h.commit()).toBe('committed')
		const before = h.producer.sent.length
		h.enqueue(first, 'in', uncommitted)
		await h.process(first)
		const firstRun = sent(before)
		const revived = await h.revive(first)
		const replayed = h.producer.sent.length
		h.enqueue(revived, 'in', uncommitted)
		await h.process(revived)
		return [firstRun, sent(replayed)]
	}

	const at = (key: string, timestamp: number): TestRecord => ({ key, value: 'v', timestamp })

	it('KF1: a final time window forwarded after the last commit is forwarded again', async () => {
		const task = counting(TimeWindows.ofSizeWithNoGrace(10), FINAL)
		// k@70005 closes [10,20) and moves the store clock past its retention.
		const [first, again] = await replay(task, [at('k', 5), at('k', 15)], [at('k', 70_005)])
		expect(first).toEqual([['k', 10, 20, 1]])
		expect(again).toEqual(first)
	})

	it('KF1: a final session forwarded after the last commit is forwarded again', async () => {
		const task = counting(SessionWindows.ofInactivityGapWithNoGrace(10), FINAL)
		const [first, again] = await replay(task, [at('k', 5)], [at('j', 70_000)])
		expect(first).toEqual([['k', 5, 5, 1]])
		expect(again).toEqual(first)
	})

	it('a session merged after the last commit is merged again', async () => {
		const task = counting(SessionWindows.ofInactivityGapWithNoGrace(10), { value: num })
		const [first, again] = await replay(task, [at('k', 100)], [at('k', 105), at('j', 70_000)])
		expect(first).toEqual([
			['k', 100, 100, null],
			['k', 100, 105, 2],
			['j', 70_000, 70_000, 1],
		])
		// The local store keeps the merged session: at-least-once counts the replayed records twice (Kafka Streams too).
		expect(again).toEqual([
			['k', 100, 105, null],
			['k', 100, 105, 3],
			['j', 70_000, 70_000, 2],
		])
	})

	it('a time window updated after the last commit is updated again from its committed count, not restarted', async () => {
		// The restore applied j's write at window end 110 after the commit: a store clock seeded from it would put the
		// read floor at 101 and hide [0,10) from the replay of k@7, losing the committed k@5.
		const task = counting(TimeWindows.ofSizeWithNoGrace(10), { value: num })
		const [first, again] = await replay(task, [at('k', 5)], [at('k', 7), at('j', 100)])
		expect(first).toEqual([
			['k', 0, 10, 2],
			['j', 100, 110, 1],
		])
		// At-least-once counts the replayed records twice (Kafka Streams too).
		expect(again).toEqual([
			['k', 0, 10, 3],
			['j', 100, 110, 2],
		])
	})
})
