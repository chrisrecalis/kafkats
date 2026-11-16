import { describe, expect, it } from 'vitest'
import { codec } from '../../src/index.js'
import { WindowedAggregateNode } from '../../src/legacy/processors/aggregation.js'
import type { StreamRecord, WorkerContext } from '../../src/legacy/processors/base.js'
import { InMemoryWindowStore } from '../../src/state/memory.js'

const WINDOW_MS = 1_000
const RETENTION_MS = WINDOW_MS * 24
const BASE = 1_000_000_000_000

const record = (timestamp: number): StreamRecord<string, number> => ({
	key: 'k',
	value: 1,
	timestamp: BigInt(timestamp),
	topic: 't',
	partition: 0,
	offset: 0n,
	headers: {},
})

describe('windowed aggregation: stream time after a recovery', () => {
	for (const [name, advanceMs] of [
		['tumbling', 0],
		['hopping', 250],
	] as const) {
		it(`${name} windows: a record gets the same late-or-not answer before and after the clock is rebuilt`, async () => {
			const store = new InMemoryWindowStore<string, number>('w', {
				keyCodec: codec.string(),
				valueCodec: codec.json<number>(),
				retentionMs: RETENTION_MS,
				windowSizeMs: WINDOW_MS,
			})
			const node = new WindowedAggregateNode<string, number, number>(
				'w',
				{ store },
				() => 0,
				(_key, value, sum) => sum + value,
				WINDOW_MS,
				advanceMs
			)
			const forwarded = (worker: WorkerContext) => {
				const clone = node.clone(worker)
				let count = 0
				clone.connect({ process: async () => void count++ } as never)
				return { clone, count: () => count }
			}
			const worker = { streamClocks: new Map() } as unknown as WorkerContext

			// Stream time ends up in the middle of a window.
			const before = forwarded(worker)
			await before.clone.process(record(BASE + 30_530))
			const windowsBefore = await store.approximateNumEntries()

			// Every timestamp around the edge of retention: decide it now ...
			const candidates = Array.from({ length: 80 }, (_, i) => BASE + 30_530 - RETENTION_MS - 2_000 + i * 50)
			const answer = async (timestamp: number): Promise<boolean> => {
				const probe = forwarded({ streamClocks: new Map(worker.streamClocks) } as unknown as WorkerContext)
				const existing = new Set<string>()
				for await (const [key] of store.all()) existing.add(JSON.stringify(key))
				await probe.clone.process(record(timestamp))
				// Undo what the probe wrote, so that every candidate is decided on the same state.
				for await (const [key] of store.all()) {
					if (!existing.has(JSON.stringify(key))) await store.delete(key)
				}
				return probe.count() > 0
			}
			const decidedBefore: boolean[] = []
			for (const timestamp of candidates) decidedBefore.push(await answer(timestamp))
			expect(await store.approximateNumEntries()).toBe(windowsBefore)

			// ... and again after a restart, when the clock is rebuilt from the restored store.
			worker.streamClocks.clear()
			const decidedAfter: boolean[] = []
			for (const timestamp of candidates) decidedAfter.push(await answer(timestamp))

			expect(decidedAfter).toEqual(decidedBefore)
			// The candidates straddle the edge: some are late, some are not.
			expect(new Set(decidedBefore).size).toBe(2)
		})
	}
})
