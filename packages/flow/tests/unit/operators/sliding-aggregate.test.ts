import { describe, expect, it, vi } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import { counting } from '@/dsl/ops/aggregate.js'
import type { Change } from '@/operators/change.js'
import { aggregateWith } from '@/operators/kstream-aggregate.js'
import { SlidingWindowAggregate } from '@/operators/sliding-aggregate.js'
import { clockedSlidingStore } from '../support/clocked-stores.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from '../support/processor-harness.js'

/** A sliding-window count with time difference 10 over store 'w', and what it forwards. */
async function slidingCount({ graceMs = 0, sendOldValues = false } = {}) {
	const { inner, store } = clockedSlidingStore('w', 20 + graceMs)
	const count = aggregateWith<string, string, number>(counting.initializer, counting.aggregator)
	const node = new SlidingWindowAggregate('w', { timeDifferenceMs: 10, graceMs }, count, false)
	if (sendOldValues) node.enableSendingOldValues()
	const out = capture<Windowed<string>, Change<number>>('out', ['count'])
	const harness = await ProcessorHarness.start(
		[sourceDef('source', ['in']), processorDef('count', ['source'], node.supplier, ['w']), out.def],
		{ stores: [store] }
	)
	const process = (key: string | null, value: string | null, timestamp: number) =>
		harness.process('in', { key, value, timestamp })
	const forwarded = () =>
		out.records.map(({ key, value, timestamp }) => [
			key.window.start,
			key.window.end,
			value.newValue,
			value.oldValue,
			timestamp,
		])
	return { harness, inner, process, forwarded }
}

describe('SlidingWindowAggregate', () => {
	it('sends the old aggregate of an updated window; a new window has none, also one seeded from or copied from another', async () => {
		const { process, forwarded } = await slidingCount({ sendOldValues: true })
		await process('k', 'a', 100)
		await process('k', 'b', 105)
		await process('k', 'c', 103)
		expect(forwarded()).toEqual([
			[90, 100, 1, null, 100],
			[101, 111, 1, null, 105], // a's right window
			[95, 105, 2, null, 105], // b's left window, seeded from [90, 100] (Kafka Streams sends the seed as old)
			[95, 105, 3, 2, 105],
			[101, 111, 2, 1, 105],
			[104, 114, 1, null, 105], // c's right window, a copy of [101, 111] before c
		])
	})

	it('drops records with a null key or value, records whose windows all closed, and updates of closed windows', async () => {
		const { harness, process, forwarded } = await slidingCount({ graceMs: 5 })
		await process(null, 'a', 100)
		await process('k', null, 100)
		await process('k', 'b', 110)
		await process('k', 'c', 125) // windows ending before 120 closed
		await process('k', 'd', 109) // all its windows closed: 109 + 10 < 120
		await process('k', 'e', 116) // its left window [106, 116] closed
		const reasons = ['null-key', 'null-value', 'expired-window'] as const
		const dropped = reasons.map(reason => harness.task.droppedCount(reason))
		expect(dropped).toEqual([1, 1, 2])
		expect(forwarded()).toEqual([
			[100, 110, 1, null, 110],
			[115, 125, 1, null, 125],
			[115, 125, 2, null, 125],
			[111, 121, 1, null, 116], // b's right window
			[117, 127, 1, null, 125], // e's right window
		])
	})

	it('B15: a record reads the windows of its own key only', async () => {
		const { inner, process } = await slidingCount()
		const findSessions = vi.spyOn(inner, 'findSessions')
		const scans = [vi.spyOn(inner, 'all'), vi.spyOn(inner, 'range')]
		for (const [key, timestamp] of [
			['x', 100],
			['y', 101],
			['x', 105],
			['z', 104],
		] as const) {
			await process(key, 'v', timestamp)
		}
		expect(findSessions.mock.calls.map(([key]) => key)).toEqual(['x', 'y', 'x', 'z'])
		for (const scan of scans) expect(scan).not.toHaveBeenCalled()
	})
})
