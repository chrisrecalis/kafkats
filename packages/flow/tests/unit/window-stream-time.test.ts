import { describe, expect, it, vi } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import { codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import { aggregateWith } from '@/operators/kstream-aggregate.js'
import { TimeWindowAggregate } from '@/operators/window-aggregate.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import { InMemoryWindowStore } from '@/state/memory.js'
import { capture, ProcessorHarness, processorDef, sourceDef } from './support/processor-harness.js'

const WINDOW_MS = 1000
const BASE = 1_000_000_000_000

/** A tumbling sum of 1 s windows over store 'w', as the DSL builds it, run in the processor harness. */
async function windowedSum(metadata?: Record<string, number>) {
	const store = new InMemoryWindowStore<string, number>('w', {
		keyCodec: codec.string(),
		valueCodec: codec.json<number>(),
		retentionMs: WINDOW_MS * 24,
		windowSizeMs: WINDOW_MS,
	})
	const expire = vi.spyOn(store, 'expireOldWindows')
	const scan = vi.spyOn(store, 'all')
	const node = new TimeWindowAggregate<string, number, number>(
		'w',
		{ sizeMs: WINDOW_MS, advanceMs: WINDOW_MS },
		aggregateWith(
			() => 0,
			(_k, v, agg) => agg + v
		)
	)
	const out = capture<Windowed<string>, Change<number>>('out', ['agg'])
	const supplier = node.supplier as ProcessorSupplier<never, never, unknown, unknown>
	const harness = await ProcessorHarness.start(
		[sourceDef('source', ['in']), processorDef('agg', ['source'], supplier, ['w']), out.def],
		{ stores: [store], metadata }
	)
	const process = (timestamp: number) => harness.process('in', { key: 'a', value: 1, timestamp })
	return { harness, process, expire, scan, records: out.records }
}

describe('time-window aggregation: stream time', () => {
	it('expires by stream time (the window start of the record), not by wall clock', async () => {
		const { process, expire } = await windowedSum()
		await process(BASE + 750)
		expect(expire).toHaveBeenCalledTimes(1)
		expect(expire).toHaveBeenCalledWith(BASE)
	})

	it('does not expire again until the clock moved more than 60 s past the last expiry', async () => {
		const { process, expire } = await windowedSum()
		await process(BASE)
		await process(BASE + 100)
		await process(BASE + 60_999) // window start BASE + 60 000: exactly 60 s
		expect(expire).toHaveBeenCalledTimes(1)
		await process(BASE + 61_000)
		expect(expire).toHaveBeenCalledTimes(2)
		expect(expire).toHaveBeenLastCalledWith(BASE + 61_000)
	})

	it("is the processor's clock, persisted in the task's metadata under the store's name, in window starts", async () => {
		const { harness, process } = await windowedSum()
		expect(harness.metadata.get('w')).toBeUndefined()
		await process(BASE + 750)
		expect(harness.metadata.get('w')).toBe(BASE)
		await process(BASE - 5_000) // an older record never moves it back
		expect(harness.metadata.get('w')).toBe(BASE)
	})

	it('B14: a clock seeded from the commit metadata drops the same late records, without scanning the store', async () => {
		const { process, scan, records } = await windowedSum({ w: BASE })
		await process(BASE - 30_000) // window end BASE - 29 000 < BASE - 24 s retention: expired
		await process(BASE - 10_000) // within retention
		expect(records.map(r => r.key.window.start)).toEqual([BASE - 10_000])
		expect(scan).not.toHaveBeenCalled()
	})
})
