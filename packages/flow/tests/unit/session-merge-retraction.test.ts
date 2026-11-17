import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { SessionWindows } from '@/dsl/windows.js'
import { WindowedCodecs } from '@/operators/windowed.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from './support/guarantees.js'

const str = codec.string()
const num = codec.json<number>()
const keys = WindowedCodecs.sessionWindowed(str)

type Update = { start: number; end: number; value: number | null; timestamp: number }

/** A session sum (reduce or aggregate) of topic 'in' with a gap of 10 ms, written to 'out'. */
function sessionSums(driver: TestDriver, kind: 'reduce' | 'aggregate'): void {
	const windowed = driver
		.input('in', { key: str, value: num })
		.groupByKey()
		.windowedBy(SessionWindows.ofInactivityGapAndGrace(10, 100))
	const table =
		kind === 'reduce'
			? windowed.reduce((a, b) => a + (b ?? 0))
			: windowed.aggregate(
					() => 0,
					(_k, v, agg) => agg + (v ?? 0),
					(_k, a, b) => a + b,
					{ value: num }
				)
	table.toStream().to('out', { key: keys, value: num })
}

/** The updates written to 'out', with their timestamps. */
function updates(ctx: TestContextApi): Update[] {
	return ctx.producer.messagesFor('out').map(({ key, value, timestamp }) => {
		const { window } = keys.decode(key!)
		return { ...window, value: value === null ? null : num.decode(value), timestamp: Number(timestamp) }
	})
}

const send = (ctx: TestContextApi, timestamp: number) =>
	ctx.send('in', 1, { key: Buffer.from('a'), codec: num, timestamp })

describe.each(['reduce', 'aggregate'] as const)('session merge retraction (%s)', kind => {
	describe.each(GUARANTEES)('%s', (_, config) => {
		/** Sends records at `timestamps` to a session sum; returns the updates of the last `last` records. */
		async function run(timestamps: readonly number[], last = timestamps.length): Promise<Update[]> {
			const driver = new TestDriver(config)
			sessionSums(driver, kind)
			let result: Update[] = []
			await driver.run(async ctx => {
				for (const [i, timestamp] of timestamps.entries()) {
					if (i === timestamps.length - last) ctx.clear()
					await send(ctx, timestamp)
				}
				result = updates(ctx)
			})
			return result
		}

		it('retracts the merged-away session before emitting the merged result, each at its session end', async () => {
			// [0,0] = 1, then 5 merges it into [0,5] = 2: a table keyed by the window must not keep [0,0].
			expect(await run([0, 5])).toEqual([
				{ start: 0, end: 0, value: 1, timestamp: 0 },
				{ start: 0, end: 0, value: null, timestamp: 0 },
				{ start: 0, end: 5, value: 2, timestamp: 5 },
			])
		})

		it('BC23: retracts a merged session even when the merged session has its bounds (Kafka Streams)', async () => {
			// 3 falls into [0,5]: the session is retracted, then sent again with the new aggregate.
			expect(await run([0, 5, 3], 1)).toEqual([
				{ start: 0, end: 5, value: null, timestamp: 5 },
				{ start: 0, end: 5, value: 3, timestamp: 5 },
			])
		})

		it('updates a one-point session in place: a record at its timestamp merges nothing else', async () => {
			expect(await run([7, 7], 1)).toEqual([{ start: 7, end: 7, value: 2, timestamp: 7 }])
		})

		it('retracts both sessions a middle record bridges, in start order', async () => {
			// 10 is within the gap of [0,0] and [20,20] and bridges them into [0,20] = 3.
			expect(await run([0, 20, 10], 1)).toEqual([
				{ start: 0, end: 0, value: null, timestamp: 0 },
				{ start: 20, end: 20, value: null, timestamp: 20 },
				{ start: 0, end: 20, value: 3, timestamp: 20 },
			])
		})
	})
})
