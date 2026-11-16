import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { SessionWindows, TimeWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'

const str = codec.string()
const num = codec.json<number>()
const WINDOW_MS = 1000
const BASE = 1_000_000_000_000
// Stores keep 24 windows (24 s): 30 s late is past retention, 10 s late is not.

type Update = { key: string; start: number; end: number; value: number | null }

const TUMBLING = TimeWindows.of(WINDOW_MS)
const SESSIONS = SessionWindows.withInactivityGap(WINDOW_MS)

/** A sum per window (or session) of the values of topic 'in'; returns its updates. */
function windowedSums(driver: TestDriver, windows: TimeWindows | SessionWindows): Update[] {
	const updates: Update[] = []
	const grouped = driver.input('in', { key: str, value: num }).groupByKey()
	const table =
		windows instanceof SessionWindows
			? grouped.windowedBy(windows).reduce((a, b) => a + b)
			: grouped.windowedBy(windows).reduce((a, b) => a + b)
	table
		.toStream()
		.peek((k, value) => updates.push({ key: k!.key, start: k!.window.start, end: k!.window.end, value }))
	return updates
}

const send = (ctx: TestContextApi, key: string, value: number, timestamp: number) =>
	ctx.send('in', value, { key: Buffer.from(key), codec: num, timestamp })

describe('time-window aggregation: late records', () => {
	it('drops a record whose window is already past retention instead of resurrecting it', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, TUMBLING)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			// 30 s late (> 24 s retention): its window is gone, so it must be dropped, not re-created from scratch.
			await send(ctx, 'a', 99, BASE - 30_000)
		})
		expect(updates).toEqual([{ key: 'a', start: BASE, end: BASE + WINDOW_MS, value: 5 }])
	})

	it('still aggregates a moderately late record that is within retention', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, TUMBLING)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			await send(ctx, 'a', 7, BASE - 10_000)
		})
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE, 5],
			[BASE - 10_000, 7],
		])
	})

	it('keeps a window that ends exactly 24 windows before the clock and drops the one before it', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, TUMBLING)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE) // the clock: window start BASE
			await send(ctx, 'a', 1, BASE - 24_001) // [BASE - 25 s, BASE - 24 s): ends at the bound, kept
			await send(ctx, 'a', 2, BASE - 25_001) // [BASE - 26 s, BASE - 25 s): ends before it, dropped
		})
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE, 5],
			[BASE - 25_000, 1],
		])
	})

	it('drops only the expired windows of a hopping window record', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, TUMBLING.advanceBy(WINDOW_MS / 2))
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE) // the clock: window start BASE
			// In [BASE - 25.5 s, BASE - 24.5 s), expired, and [BASE - 25 s, BASE - 24 s), still kept.
			await send(ctx, 'a', 1, BASE - 24_700)
		})
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE - 500, 5],
			[BASE, 5],
			[BASE - 25_000, 1],
		])
	})
})

describe('session aggregation: late records', () => {
	it('keeps a record that starts a session exactly 24 gaps before the clock and drops an older one', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, SESSIONS)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE) // the clock: BASE
			await send(ctx, 'b', 1, BASE - 24_000) // no session of 'b', at the bound: a new session
			await send(ctx, 'c', 2, BASE - 24_001) // no session of 'c', older than the bound: dropped
		})
		expect(updates).toEqual([
			{ key: 'a', start: BASE, end: BASE, value: 5 },
			{ key: 'b', start: BASE - 24_000, end: BASE - 24_000, value: 1 },
		])
	})

	it('drops a late record that has no live session to merge into', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, SESSIONS)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			await send(ctx, 'a', 99, BASE - 30_000)
		})
		expect(updates).toEqual([{ key: 'a', start: BASE, end: BASE, value: 5 }])
	})

	it('still merges a late record into a live session, however old', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, SESSIONS)
		await driver.run(async ctx => {
			await send(ctx, 'a', 42, BASE - 30_500) // a session of 'a', still stored
			await send(ctx, 'a', 5, BASE)
			await send(ctx, 'a', 99, BASE - 30_000) // 30 s late, but within the gap of the live session
			await send(ctx, 'b', 1, BASE - 30_000) // 30 s late and no session of 'b': dropped
		})
		expect(updates.slice(2)).toEqual([
			{ key: 'a', start: BASE - 30_500, end: BASE - 30_500, value: null },
			{ key: 'a', start: BASE - 30_500, end: BASE - 30_000, value: 141 },
		])
	})
})
