import { describe, expect, it, vi } from 'vitest'
import { codec } from '@/codec.js'
import { listCodec } from '@/operators/list-value.js'
import { streamStreamJoinProcessor } from '@/operators/stream-stream-join.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { CLEANUP_INTERVAL_MS } from '@/processor/time-tracker.js'
import { InMemorySessionStore } from '@/state/memory.js'
import { ClockedSessionStore, TaskStoreClock } from '@/state/time-clipping.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from './support/processor-harness.js'

const WINDOW_MS = 1000
/** before + after + grace */
const RETENTION_MS = 2 * WINDOW_MS
const BASE = 1_000_000_000_000

/** A join buffer as a task stacks it: an in-memory session store under the clipping layer of its store clock. */
function buffer(name: string) {
	const raw = new InMemorySessionStore<string, string[]>(name, {
		keyCodec: codec.string(),
		valueCodec: listCodec(codec.string()),
		retentionMs: RETENTION_MS,
	})
	const metadata = new ProcessorMetadataImpl()
	const clock = new TaskStoreClock(metadata, name, RETENTION_MS)
	const store = new ClockedSessionStore(raw, clock, RETENTION_MS)
	return { raw, metadata, clock, store, expire: vi.spyOn(store, 'expire') }
}

type Buffer = ReturnType<typeof buffer>

/**
 * A join of topics 'left' and 'right' over two buffers, wired as the DSL wires a join without an outer-join store.
 * `restore` fills the left buffer as a restore would, and the processor clock is then seeded as from a commit.
 * `process` processes a record, then commits the buffers' clocks, as a task committing after each record.
 */
async function joinHarness(outer: { left: boolean; right: boolean }, restore?: (left: Buffer) => Promise<void>) {
	const left = buffer('left-store')
	const right = buffer('right-store')
	await restore?.(left)
	const side = { clock: 'left-store', beforeMs: WINDOW_MS, afterMs: WINDOW_MS, graceMs: 0 }
	const results = capture('out', ['this', 'other'])
	const harness = await ProcessorHarness.start(
		[
			sourceDef('left-source', ['left']),
			sourceDef('right-source', ['right']),
			processorDef(
				'this',
				['left-source'],
				streamStreamJoinProcessor<string, string, string, string>({
					...side,
					side: 'left',
					thisStore: 'left-store',
					otherStore: 'right-store',
					joiner: (_key, l, r) => `${l}+${r}`,
					outer: outer.left,
					otherOuter: outer.right,
				}),
				['left-store', 'right-store']
			),
			processorDef(
				'other',
				['right-source'],
				streamStreamJoinProcessor<string, string, string, string>({
					...side,
					side: 'right',
					thisStore: 'right-store',
					otherStore: 'left-store',
					joiner: (_key, r, l) => `${l}+${r}`,
					outer: outer.right,
					otherOuter: outer.left,
				}),
				['right-store', 'left-store']
			),
			results.def,
		],
		{ stores: [left.store, right.store], metadata: restore && { 'left-store': BASE } }
	)
	const process = async (topic: string, record: { key: string; value: string; timestamp: number }) => {
		await harness.process(topic, record)
		left.metadata.markCommitted()
		right.metadata.markCommitted()
	}
	return { harness, process, left, right, results: results.records }
}

const kinds = {
	inner: { left: false, right: false },
	left: { left: true, right: false },
	outer: { left: true, right: true },
} as const

describe('stream-stream join buffer expiry', () => {
	for (const [kind, outer] of Object.entries(kinds)) {
		it(`${kind} join expires both buffers on the processor clock's cadence, each by its own store clock`, async () => {
			const { process, left, right } = await joinHarness(outer)
			await process('right', { key: 'a', value: 'r', timestamp: BASE })
			await process('left', { key: 'a', value: 'old', timestamp: BASE + 1000 })
			await process('left', { key: 'a', value: 'new', timestamp: BASE + 70_000 })
			left.expire.mockClear()
			right.expire.mockClear()
			await process('left', { key: 'a', value: 'newest', timestamp: BASE + 140_000 })
			// Both buffers, so neither grows while the other stream is idle.
			expect(left.expire).toHaveBeenCalledTimes(1)
			expect(right.expire).toHaveBeenCalledTimes(1)
			// The left buffer's committed clock is at 'new', past the retention of 'old'; the right one's is at 'r'.
			expect(await left.raw.approximateNumEntries()).toBe(2)
			expect(await right.raw.approximateNumEntries()).toBe(1)
		})
	}

	it('does not expire again until the clock moved past the cleanup interval', async () => {
		const { harness, left } = await joinHarness(kinds.inner)
		await harness.process('left', { key: 'a', value: '1', timestamp: BASE })
		await harness.process('left', { key: 'a', value: '2', timestamp: BASE + 100 })
		expect(left.expire).toHaveBeenCalledTimes(1)
		await harness.process('left', { key: 'a', value: '3', timestamp: BASE + CLEANUP_INTERVAL_MS + 1 })
		expect(left.expire).toHaveBeenCalledTimes(2)
	})

	it('both sides advance one processor clock, kept in the task metadata under the left buffer name', async () => {
		const { harness, left } = await joinHarness(kinds.inner)
		await harness.process('left', { key: 'a', value: 'l', timestamp: BASE })
		await harness.process('right', { key: 'a', value: 'r', timestamp: BASE + CLEANUP_INTERVAL_MS + 1 })
		expect(left.expire).toHaveBeenCalledTimes(2)
		// The left side's next expiry is due by the shared clock, not by its own record's timestamp.
		await harness.process('left', { key: 'a', value: 'l', timestamp: BASE - 5 })
		expect(left.expire).toHaveBeenCalledTimes(3)
		expect(harness.metadata.get('left-store')).toBe(BASE + CLEANUP_INTERVAL_MS + 1)
		expect(harness.metadata.needsCommit).toBe(true)
	})

	it('after a restart the seeded store clock clips probes, and the committed one decides expiry', async () => {
		const old = BASE - 10 * WINDOW_MS
		const { harness, left, results } = await joinHarness(kinds.inner, async restored => {
			await restored.raw.put({ key: 'a', windowStart: old, windowEnd: old }, ['old'])
			const committed = new Map([['left-store.observed', BASE]])
			restored.metadata.replaceAll([committed], new Set(committed.keys()))
			restored.clock.seed(BASE)
		})
		await harness.process('right', { key: 'a', value: 'r', timestamp: old + 500 })
		// 'old' was restored, but is past the retention of the left buffer's seeded clock: no match, gone at the expiry.
		expect(results).toEqual([])
		expect(left.expire).toHaveBeenCalledTimes(1)
		expect(await left.raw.approximateNumEntries()).toBe(0)
	})

	it('bounds a buffer as stream time advances far beyond its retention', async () => {
		const { process, left } = await joinHarness(kinds.inner)
		// 20 records 70 s apart: past the cleanup interval each time, so every record expires the ones before the
		// last commit.
		for (let i = 0; i < 20; i++) {
			await process('left', { key: 'a', value: String(i), timestamp: BASE + i * 70_000 })
		}
		expect(await left.raw.approximateNumEntries()).toBe(2)
	})
})
