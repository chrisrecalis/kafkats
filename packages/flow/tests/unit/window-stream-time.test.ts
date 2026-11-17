import { describe, expect, it, vi } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { aggregateWith } from '@/operators/kstream-aggregate.js'
import { TimeWindowAggregate } from '@/operators/window-aggregate.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import { clockedWindowStore } from './support/clocked-stores.js'
import { capture, ProcessorHarness, processorDef, sourceDef } from './support/processor-harness.js'

const WINDOW_MS = 1000
const GRACE_MS = 4500
const BASE = 1_000_000_000_000

/** A tumbling sum of 1 s windows with a grace of 4.5 s over store 'w', as the DSL builds it, run in the processor harness. */
async function windowedSum(metadata?: Record<string, number>) {
	const { inner, store } = clockedWindowStore('w', WINDOW_MS + GRACE_MS, WINDOW_MS)
	const expire = vi.spyOn(store, 'expire')
	const scans = [vi.spyOn(inner, 'all'), vi.spyOn(inner, 'fetchAll'), vi.spyOn(inner, 'fetch')]
	const node = new TimeWindowAggregate<string, number, number>(
		'w',
		{ sizeMs: WINDOW_MS, advanceMs: WINDOW_MS, graceMs: GRACE_MS },
		aggregateWith(
			() => 0,
			(_k, v, agg) => agg + v
		),
		false
	)
	const out = capture<Windowed<string>, Change<number>>('out', ['agg'])
	const supplier = node.supplier as ProcessorSupplier<never, never, unknown, unknown>
	const harness = await ProcessorHarness.start(
		[sourceDef('source', ['in']), processorDef('agg', ['source'], supplier, ['w']), out.def],
		{ stores: [store], metadata }
	)
	const process = (timestamp: number) => harness.process('in', { key: 'a', value: 1, timestamp })
	return { harness, process, expire, scans, records: out.records }
}

describe('time-window aggregation: stream time', () => {
	it("is the processor's clock: the max record timestamp, persisted in the task's metadata under the store's name", async () => {
		const { harness, process } = await windowedSum()
		expect(harness.metadata.get('w')).toBeUndefined()
		await process(BASE + 750)
		expect(harness.metadata.get('w')).toBe(BASE + 750)
		await process(BASE - 4000) // an older record never moves it back
		expect(harness.metadata.get('w')).toBe(BASE + 750)
	})

	it('counts record timestamps, not window starts: a window closes at its end plus grace', async () => {
		const { process, records } = await windowedSum()
		await process(BASE + 999) // stream time BASE + 999: windows ending by BASE - 3501 are closed
		await process(BASE - 3002) // [BASE - 4000, BASE - 3000): open
		await process(BASE - 4001) // [BASE - 5000, BASE - 4000): closed (a clock of window starts, BASE, would keep it)
		expect(records.map(r => r.key.window.start)).toEqual([BASE, BASE - 4000])
	})

	it('expires through the store clock once the processor clock moved more than 60 s past the last expiry', async () => {
		const { process, expire } = await windowedSum()
		await process(BASE)
		expect(expire).toHaveBeenCalledTimes(1)
		await process(BASE + 100)
		await process(BASE + 60_000) // exactly 60 s
		expect(expire).toHaveBeenCalledTimes(1)
		await process(BASE + 60_001)
		expect(expire).toHaveBeenCalledTimes(2)
	})

	it('B14: a clock seeded from the commit metadata drops the same late records, without scanning the store', async () => {
		const { process, scans, records } = await windowedSum({ w: BASE })
		await process(BASE - 6000) // [BASE - 6000, BASE - 5000) ended before BASE - grace: closed
		await process(BASE - 5000) // [BASE - 5000, BASE - 4000): open
		expect(records.map(r => r.key.window.start)).toEqual([BASE - 5000])
		for (const scan of scans) expect(scan).not.toHaveBeenCalled()
	})
})
