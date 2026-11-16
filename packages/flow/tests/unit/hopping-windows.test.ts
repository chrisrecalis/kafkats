import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { KTable } from '@/api/ktable.js'
import { codec } from '@/codec.js'
import { TimeWindows } from '@/dsl/windows.js'
import { TestDriver } from '@/testing/driver.js'

const str = codec.string()
const num = codec.json<number>()

const grouped = (driver: TestDriver) => driver.input('in', { key: str, value: num }).groupByKey()

/** The updates of `build`'s table for records of key 'a' with the given values and timestamps. */
async function updates(
	build: (driver: TestDriver) => KTable<Windowed<string>, number>,
	records: readonly (readonly [value: number, timestamp: number])[]
): Promise<{ start: number; end: number; value: number | null }[]> {
	const driver = new TestDriver()
	const out: { start: number; end: number; value: number | null }[] = []
	build(driver)
		.toStream()
		.peek((k, value) => out.push({ start: k!.window.start, end: k!.window.end, value }))
	await driver.run(async ({ send }) => {
		for (const [value, timestamp] of records) {
			await send('in', value, { key: Buffer.from('a'), codec: num, timestamp })
		}
	})
	return out
}

describe('hopping windows', () => {
	it('aggregate() aggregates a record into every overlapping window', async () => {
		// size=1000, advance=500: a record at 750 belongs to [0, 1000) and [500, 1500).
		const out = await updates(
			driver =>
				grouped(driver)
					.windowedBy(TimeWindows.of(1000).advanceBy(500))
					.aggregate(
						() => 0,
						(_k, v, agg) => agg + v,
						{ value: num }
					),
			[[5, 750]]
		)
		expect(out).toEqual([
			{ start: 0, end: 1000, value: 5 },
			{ start: 500, end: 1500, value: 5 },
		])
	})

	it('reduce() reduces a record into every overlapping window', async () => {
		const out = await updates(
			driver =>
				grouped(driver)
					.windowedBy(TimeWindows.of(1000).advanceBy(500))
					.reduce((a, b) => a + b),
			[
				[7, 750],
				[1, 900],
			]
		)
		expect(out).toEqual([
			{ start: 0, end: 1000, value: 7 },
			{ start: 500, end: 1500, value: 7 },
			{ start: 0, end: 1000, value: 8 },
			{ start: 500, end: 1500, value: 8 },
		])
	})

	it('throws a TopologyError at the aggregation when advanceBy is greater than the window size', () => {
		// Larger advances would silently drop the records that fall between windows.
		const driver = new TestDriver()
		const windowed = grouped(driver).windowedBy(TimeWindows.of(1000).advanceBy(2000))
		expect(() => windowed.count()).toThrow(TopologyError)
		expect(() => windowed.reduce((a, b) => a + b)).toThrow(/advanceBy must be <= window size/)
	})

	it('tumbling windows (advance == size) produce exactly one update per record', async () => {
		const out = await updates(driver => grouped(driver).windowedBy(TimeWindows.of(1000)).count(), [[1, 750]])
		expect(out).toEqual([{ start: 0, end: 1000, value: 1 }])
	})
})
