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
