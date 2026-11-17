import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { SessionWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'

const str = codec.string()
const num = codec.json<number>()
const GAP_MS = 10

type Update = { start: number; end: number; value: number | null }

/** A session sum (reduce or aggregate) of topic 'in' with a gap of 10 ms; returns its updates. */
function sessionSums(driver: TestDriver, kind: 'reduce' | 'aggregate'): Update[] {
	const updates: Update[] = []
	const windowed = driver
		.input('in', { key: str, value: num })
		.groupByKey()
		.windowedBy(SessionWindows.withInactivityGap(GAP_MS))
	const table =
		kind === 'reduce'
			? windowed.reduce((a, b) => a + b)
			: windowed.aggregate(
					() => 0,
					(_k, v, agg) => agg + v,
					(_k, a, b) => a + b,
					{ value: num }
				)
	table.toStream().peek((k, value) => updates.push({ start: k!.window.start, end: k!.window.end, value }))
	return updates
}

const send = (ctx: TestContextApi, timestamp: number) =>
	ctx.send('in', 1, { key: Buffer.from('a'), codec: num, timestamp })

describe.each(['reduce', 'aggregate'] as const)('session merge retraction (%s)', kind => {
	it('retracts the merged-away session before emitting the merged result', async () => {
		const driver = new TestDriver()
		const updates = sessionSums(driver, kind)
		await driver.run(async ctx => {
			await send(ctx, 0) // session [0,0] = 1
			await send(ctx, 5) // merges [0,0] into [0,5] = 2
		})
		// The old [0,0] window must be retracted (tombstone) before the merged [0,5] result, so a downstream
		// table or consumer keyed on the windowed key does not keep a stale [0,0] entry.
		expect(updates).toEqual([
			{ start: 0, end: 0, value: 1 },
			{ start: 0, end: 0, value: null },
			{ start: 0, end: 5, value: 2 },
		])
	})

	it('does not retract when the merged window equals the existing session (in-place update)', async () => {
		const driver = new TestDriver()
		const updates = sessionSums(driver, kind)
		await driver.run(async ctx => {
			await send(ctx, 0)
			await send(ctx, 5) // [0,5] = 2
			updates.length = 0
			// A record inside [0,5] keeps the window's bounds: no tombstone, just an updated value.
			await send(ctx, 3)
		})
		expect(updates).toEqual([{ start: 0, end: 5, value: 3 }])
	})

	it('retracts both sessions a middle record bridges', async () => {
		const driver = new TestDriver()
		const updates = sessionSums(driver, kind)
		await driver.run(async ctx => {
			await send(ctx, 0) // session [0,0]
			await send(ctx, 20) // separate session [20,20] (gap 10)
			updates.length = 0
			// A record at 10 is within the gap of both sessions and bridges them into [0,20] = 3. Both absorbed
			// windows differ from the merged window, so both are retracted, in start order.
			await send(ctx, 10)
		})
		expect(updates).toEqual([
			{ start: 0, end: 0, value: null },
			{ start: 20, end: 20, value: null },
			{ start: 0, end: 20, value: 3 },
		])
	})
})
