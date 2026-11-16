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

/** A sum per window (or session) of the values of topic 'in'; returns its updates. */
function windowedSums(driver: TestDriver, sessions: boolean): Update[] {
	const updates: Update[] = []
	const grouped = driver.input('in', { key: str, value: num }).groupByKey()
	const table = sessions
		? grouped.windowedBy(SessionWindows.withInactivityGap(WINDOW_MS)).reduce((a, b) => a + b)
		: grouped.windowedBy(TimeWindows.of(WINDOW_MS)).reduce((a, b) => a + b)
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
		const updates = windowedSums(driver, false)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			// 30 s late (> 24 s retention): its window is gone, so it must be dropped, not re-created from scratch.
			await send(ctx, 'a', 99, BASE - 30_000)
		})
		expect(updates).toEqual([{ key: 'a', start: BASE, end: BASE + WINDOW_MS, value: 5 }])
	})

	it('still aggregates a moderately late record that is within retention', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, false)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			await send(ctx, 'a', 7, BASE - 10_000)
		})
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE, 5],
			[BASE - 10_000, 7],
		])
	})
})

describe('session aggregation: late records', () => {
	it('drops a late record that has no live session to merge into', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, true)
		await driver.run(async ctx => {
			await send(ctx, 'a', 5, BASE)
			await send(ctx, 'a', 99, BASE - 30_000)
		})
		expect(updates).toEqual([{ key: 'a', start: BASE, end: BASE, value: 5 }])
	})

	it('still merges a late record into a live session, however old', async () => {
		const driver = new TestDriver()
		const updates = windowedSums(driver, true)
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
