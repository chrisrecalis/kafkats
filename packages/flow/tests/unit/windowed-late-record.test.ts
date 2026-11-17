import { describe, expect, it } from 'vitest'
import type { WindowDuration } from '@/api/options.js'
import { codec } from '@/codec.js'
import { SessionWindows, TimeWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import { GUARANTEES } from './support/guarantees.js'

const str = codec.string()
const num = codec.json<number>()
const BASE = 1_000_000_000_000

type Update = { key: string; start: number; end: number; value: number | null }

/** A sum per window (or session) of the values of topic 'in'; returns its updates. */
function windowedSums(driver: TestDriver, windows: TimeWindows | SessionWindows, retention?: WindowDuration): Update[] {
	const updates: Update[] = []
	const grouped = driver.input('in', { key: str, value: num }).groupByKey()
	const sum = (a: number, b: number) => a + b
	const table =
		windows instanceof SessionWindows
			? grouped.windowedBy(windows).reduce(sum, { retention })
			: grouped.windowedBy(windows).reduce(sum, { retention })
	table
		.toStream()
		.peek((k, value) => updates.push({ key: k!.key, start: k!.window.start, end: k!.window.end, value }))
	return updates
}

const send = (ctx: TestContextApi, key: string, value: number, timestamp: number) =>
	ctx.send('in', value, { key: Buffer.from(key), codec: num, timestamp })

/** Runs `records` (key, value, timestamp) through the sums of `windows`; returns the updates. */
async function run(
	config: TestDriverConfig,
	windows: TimeWindows | SessionWindows,
	records: readonly (readonly [key: string, value: number, timestamp: number])[],
	retention?: WindowDuration
): Promise<Update[]> {
	const driver = new TestDriver(config)
	const updates = windowedSums(driver, windows, retention)
	await driver.run(async ctx => {
		for (const [key, value, timestamp] of records) await send(ctx, key, value, timestamp)
	})
	return updates
}

describe.each(GUARANTEES)('time-window aggregation: late records (%s)', (_, config) => {
	const windows = TimeWindows.ofSizeAndGrace(1000, 5000)

	it('a window takes records while its end is after stream time minus grace (Kafka Streams)', async () => {
		const updates = await run(config, windows, [
			['a', 5, BASE], // stream time BASE: windows ending by BASE - 5000 are closed
			['a', 1, BASE - 5000], // [BASE - 5000, BASE - 4000): open
			['a', 2, BASE - 5001], // [BASE - 6000, BASE - 5000): closed, dropped
		])
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE, 5],
			[BASE - 5000, 1],
		])
	})

	it('does not re-create a closed window, however long the store keeps windows', async () => {
		const updates = await run(
			config,
			windows,
			[
				['a', 5, BASE],
				['a', 99, BASE - 30_000],
			],
			'1h'
		)
		expect(updates).toEqual([{ key: 'a', start: BASE, end: BASE + 1000, value: 5 }])
	})

	it('a record is judged by stream time including itself: the record that advances it is never late', async () => {
		const updates = await run(config, TimeWindows.ofSizeWithNoGrace(1000), [
			['a', 1, BASE + 999],
			['b', 2, BASE + 2000], // stream time BASE + 2000: [BASE + 1000, BASE + 2000) is closed
			['a', 3, BASE + 1999], // dropped
			['a', 4, BASE + 2999], // [BASE + 2000, BASE + 3000): open
		])
		expect(updates.map(u => [u.key, u.start, u.value])).toEqual([
			['a', BASE, 1],
			['b', BASE + 2000, 2],
			['a', BASE + 2000, 4],
		])
	})

	it('a hopping record updates its open windows and is dropped for its closed ones', async () => {
		const updates = await run(config, TimeWindows.ofSizeWithNoGrace(1000).advanceBy(500), [
			['a', 5, BASE], // stream time BASE: windows ending by BASE are closed
			['a', 1, BASE - 300], // [BASE - 1000, BASE) closed, [BASE - 500, BASE + 500) open
		])
		expect(updates.map(u => [u.start, u.value])).toEqual([
			[BASE - 500, 5],
			[BASE, 5],
			[BASE - 500, 6],
		])
	})
})

describe.each(GUARANTEES)('session aggregation: late records (%s)', (_, config) => {
	const sessions = SessionWindows.ofInactivityGapAndGrace(1000, 5000)

	it('a record is dropped when its session ends before stream time minus grace minus gap (Kafka Streams)', async () => {
		const updates = await run(config, sessions, [
			['a', 5, BASE], // stream time BASE: sessions ending before BASE - 6000 are closed
			['b', 1, BASE - 6000], // ends at the bound: a new session
			['c', 2, BASE - 6001], // ends before it: dropped
		])
		expect(updates).toEqual([
			{ key: 'a', start: BASE, end: BASE, value: 5 },
			{ key: 'b', start: BASE - 6000, end: BASE - 6000, value: 1 },
		])
	})

	it('a late record falling into a session still open is merged: the merged session decides', async () => {
		const updates = await run(config, SessionWindows.ofInactivityGapWithNoGrace(1000), [
			['a', 1, BASE - 2000],
			['a', 1, BASE - 1000],
			['a', 1, BASE], // the session [BASE - 2000, BASE]; stream time BASE: sessions ending before BASE - 1000 are closed
			['a', 10, BASE - 1500], // older than that, but in the open session: merged
			['b', 1, BASE - 1500], // no session of 'b': dropped
		])
		expect(updates.slice(-2)).toEqual([
			{ key: 'a', start: BASE - 2000, end: BASE, value: null },
			{ key: 'a', start: BASE - 2000, end: BASE, value: 13 },
		])
	})

	it('KF2: with gap 10 and no grace, records at 100 and 110 merge (the store clock sets the read floor)', async () => {
		const updates = await run(config, SessionWindows.ofInactivityGapWithNoGrace(10), [
			['a', 1, 100],
			['a', 2, 110],
		])
		expect(updates).toEqual([
			{ key: 'a', start: 100, end: 100, value: 1 },
			{ key: 'a', start: 100, end: 100, value: null },
			{ key: 'a', start: 100, end: 110, value: 3 },
		])
	})

	it('a session past the retention of its store is no longer merged with', async () => {
		const updates = await run(config, SessionWindows.ofInactivityGapWithNoGrace(10), [
			['a', 1, 0],
			['a', 2, 20], // the store keeps sessions ending after 20 - 10: [0, 0] is gone for reads
			['a', 3, 10], // within the gap of both, but merges [20, 20] only
		])
		expect(updates.slice(-2)).toEqual([
			{ key: 'a', start: 20, end: 20, value: null },
			{ key: 'a', start: 10, end: 20, value: 5 },
		])
	})
})
