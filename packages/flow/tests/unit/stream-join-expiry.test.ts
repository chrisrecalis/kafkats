import { describe, expect, it, vi } from 'vitest'
import { codec } from '@/codec.js'
import { joinBufferWindow } from '@/operators/join-buffer.js'
import { streamStreamJoinProcessor } from '@/operators/stream-stream-join.js'
import { CLEANUP_INTERVAL_MS } from '@/processor/time-tracker.js'
import { InMemoryWindowStore } from '@/state/memory.js'
import { processorDef, ProcessorHarness, sourceDef } from './support/processor-harness.js'

const WINDOW_MS = 1000
const BASE = 1_000_000_000_000

/** A join of topics 'left' and 'right' over two in-memory buffers, wired as the DSL wires it. */
async function joinHarness(outer: { left: boolean; right: boolean }, metadata?: Record<string, number>) {
	const buffer = (name: string) =>
		new InMemoryWindowStore<string, string>(name, {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
			...joinBufferWindow(WINDOW_MS),
		})
	const left = buffer('left-store')
	const right = buffer('right-store')
	const side = { clock: 'left-store', windowMs: WINDOW_MS, beforeMs: WINDOW_MS, afterMs: WINDOW_MS }
	const harness = await ProcessorHarness.start(
		[
			sourceDef('left-source', ['left']),
			sourceDef('right-source', ['right']),
			processorDef(
				'this',
				['left-source'],
				streamStreamJoinProcessor<string, string, string, string>({
					...side,
					thisStore: 'left-store',
					otherStore: 'right-store',
					joiner: (l, r) => `${l}+${r}`,
					outer: outer.left,
				}),
				['left-store', 'right-store']
			),
			processorDef(
				'other',
				['right-source'],
				streamStreamJoinProcessor<string, string, string, string>({
					...side,
					thisStore: 'right-store',
					otherStore: 'left-store',
					joiner: (r, l) => `${l}+${r}`,
					outer: outer.right,
				}),
				['right-store', 'left-store']
			),
		],
		{ stores: [left, right], metadata }
	)
	const expiries = [vi.spyOn(left, 'expireOldWindows'), vi.spyOn(right, 'expireOldWindows')] as const
	return { harness, expiries, left }
}

const kinds = {
	inner: { left: false, right: false },
	left: { left: true, right: false },
	outer: { left: true, right: true },
} as const

describe('stream-stream join buffer expiry', () => {
	for (const [kind, outer] of Object.entries(kinds)) {
		it(`${kind} join expires BOTH buffers by stream time`, async () => {
			const { harness, expiries } = await joinHarness(outer)
			await harness.process('left', { key: 'a', value: '1', timestamp: BASE })
			// This side's buffer and the other side's, so neither grows while the other stream is idle.
			for (const expire of expiries) {
				expect(expire).toHaveBeenCalledTimes(1)
				// By stream time (the record timestamp), not the wall clock.
				expect(expire).toHaveBeenCalledWith(BASE)
			}
		})
	}

	it('does not expire again until the clock moved past the cleanup interval', async () => {
		const { harness, expiries } = await joinHarness(kinds.inner)
		const [left] = expiries
		await harness.process('left', { key: 'a', value: '1', timestamp: BASE })
		await harness.process('left', { key: 'a', value: '2', timestamp: BASE + 100 })
		expect(left).toHaveBeenCalledTimes(1)
		await harness.process('left', { key: 'a', value: '3', timestamp: BASE + CLEANUP_INTERVAL_MS + 1 })
		expect(left).toHaveBeenCalledTimes(2)
		expect(left).toHaveBeenLastCalledWith(BASE + CLEANUP_INTERVAL_MS + 1)
	})

	it('both sides advance one clock, kept in the task metadata under the left buffer name', async () => {
		const { harness, expiries } = await joinHarness(kinds.inner)
		await harness.process('right', { key: 'a', value: 'r', timestamp: BASE })
		await harness.process('left', { key: 'a', value: 'l', timestamp: BASE - 5 })
		expect(harness.metadata.get('left-store')).toBe(BASE)
		expect(harness.metadata.needsCommit).toBe(true)
		// The left side expires by the shared clock, not by its own record's timestamp.
		expect(expiries[0]).toHaveBeenLastCalledWith(BASE)
	})

	it('a clock seeded from the commit metadata decides expiry after a restart', async () => {
		const { harness, expiries, left } = await joinHarness(kinds.inner, { 'left-store': BASE })
		await harness.process('left', { key: 'a', value: 'old', timestamp: BASE - 10 * WINDOW_MS })
		expect(expiries[0]).toHaveBeenCalledWith(BASE)
		expect(await left.approximateNumEntries()).toBe(0)
	})

	it('bounds a buffer as stream time advances far beyond its retention', async () => {
		const { harness, left } = await joinHarness(kinds.inner)
		// 20 records 70 s apart: past the cleanup interval each time, so every record expires the older ones.
		for (let i = 0; i < 20; i++) {
			await harness.process('left', { key: 'a', value: String(i), timestamp: BASE + i * 70_000 })
		}
		// Without expiry the buffer would hold all 20 records.
		expect(await left.approximateNumEntries()).toBeLessThanOrEqual(2)
	})
})
