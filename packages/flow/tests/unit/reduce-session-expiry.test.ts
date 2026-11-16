import { describe, expect, it, vi } from 'vitest'
import type { ChangelogWriter } from '@/changelog/writer.js'
import { codec } from '@/codec.js'
import { aggregateWith, reduceWith } from '@/operators/kstream-aggregate.js'
import type { KTableNode } from '@/operators/ktable-node.js'
import { sessionAggregateWith, sessionReduceWith, SessionWindowAggregate } from '@/operators/session-aggregate.js'
import { TimeWindowAggregate } from '@/operators/window-aggregate.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { StateStore, WindowedKey } from '@/state.js'
import { ChangelogBackedSessionStore } from '@/state/logging.js'
import { InMemorySessionStore, InMemoryWindowStore } from '@/state/memory.js'
import { ProcessorHarness, processorDef, sourceDef } from './support/processor-harness.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()
const BASE = 1_000_000_000_000
const GAP_MS = 1000

const windowStore = () =>
	new InMemoryWindowStore<string, number>('s', {
		keyCodec: stringCodec,
		valueCodec: numberCodec,
		retentionMs: 24_000,
		windowSizeMs: 1000,
	})
const sessionStore = () =>
	new InMemorySessionStore<string, number>('s', {
		keyCodec: stringCodec,
		valueCodec: numberCodec,
		retentionMs: 24 * GAP_MS,
	})

/**
 * Runs the processor of `node` over store `s` in the processor harness, its task seeded with `metadata`; returns a
 * function processing a record of key 'a', and the task's metadata.
 */
async function run(node: KTableNode<unknown, unknown>, store: StateStore, metadata?: Record<string, number>) {
	const supplier = node.supplier as ProcessorSupplier<never, never, unknown, unknown>
	const harness = await ProcessorHarness.start(
		[sourceDef('source', ['in']), processorDef('agg', ['source'], supplier, ['s'])],
		{ stores: [store], metadata }
	)
	const process = (timestamp: number) => harness.process('in', { key: 'a', value: 1, timestamp })
	return { process, metadata: harness.metadata }
}

const add = (a: number, b: number) => a + b

describe('windowed/session reduce + session aggregate retention', () => {
	it('a time-window reduce expires its store once its clock moved past the cleanup interval', async () => {
		const store = windowStore()
		const expire = vi.spyOn(store, 'expireOldWindows')
		const { process } = await run(
			new TimeWindowAggregate('s', { sizeMs: 1000, advanceMs: 1000 }, reduceWith(add)),
			store
		)

		// The first record moves stream time past the 60 s cleanup threshold from 0.
		await process(BASE)
		expect(expire).toHaveBeenCalledTimes(1)
		expect(expire).toHaveBeenCalledWith(BASE)
	})

	it('a session reduce expires its store once its clock moved past the cleanup interval', async () => {
		const store = sessionStore()
		const expire = vi.spyOn(store, 'expireOldSessions')
		const { process } = await run(new SessionWindowAggregate('s', GAP_MS, sessionReduceWith(add)), store)

		await process(BASE)
		expect(expire).toHaveBeenCalledTimes(1)
		expect(expire).toHaveBeenCalledWith(BASE)
	})

	it('a session aggregate expires its store once its clock moved past the cleanup interval', async () => {
		const store = sessionStore()
		const expire = vi.spyOn(store, 'expireOldSessions')
		const merge = sessionAggregateWith<string, number, number>(
			() => 0,
			(_k, v, agg) => agg + v,
			(_k, a, b) => a + b
		)
		const { process } = await run(new SessionWindowAggregate('s', GAP_MS, merge), store)

		await process(BASE)
		expect(expire).toHaveBeenCalledTimes(1)
		expect(expire).toHaveBeenCalledWith(BASE)
	})

	it('does NOT trigger expiry when stream time has not advanced past the threshold', async () => {
		const store = windowStore()
		const expire = vi.spyOn(store, 'expireOldWindows')
		const aggregation = aggregateWith<string, number, number>(
			() => 0,
			(_k, v, agg) => agg + v
		)
		const { process } = await run(
			new TimeWindowAggregate('s', { sizeMs: 1000, advanceMs: 1000 }, aggregation),
			store
		)

		await process(BASE)
		expect(expire).toHaveBeenCalledTimes(1)
		// 100 ms later: well under the 60 s threshold.
		await process(BASE + 100)
		expect(expire).toHaveBeenCalledTimes(1)
	})

	it('B14: a session clock (record timestamps, under the store name) is seeded from the commit metadata, without a store scan', async () => {
		const store = sessionStore()
		const scan = vi.spyOn(store, 'all')
		const node = new SessionWindowAggregate('s', GAP_MS, sessionReduceWith(add))
		const { process, metadata } = await run(node, store, { s: BASE })

		await process(BASE - 30_000) // joins no session and is older than the seeded clock minus 24 gaps: dropped
		await process(BASE - 10_000) // within retention: a new session
		expect(metadata.get('s')).toBe(BASE)
		await process(BASE + 500)
		expect(metadata.get('s')).toBe(BASE + 500)

		const sessions: unknown[] = []
		for await (const [session] of store.findSessions('a', 0, Number.MAX_SAFE_INTEGER)) sessions.push(session)
		expect(sessions).toEqual([
			{ key: 'a', windowStart: BASE - 10_000, windowEnd: BASE - 10_000 },
			{ key: 'a', windowStart: BASE + 500, windowEnd: BASE + 500 },
		])
		expect(scan).not.toHaveBeenCalled()
	})

	it('InMemorySessionStore.expireOldSessions actually removes data and returns the keys', async () => {
		const store = new InMemorySessionStore('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			retentionMs: 60_000,
		})
		await store.init()

		await store.put({ key: 'a', windowStart: 0, windowEnd: 1_000 }, 1)
		await store.put({ key: 'a', windowStart: 50_000, windowEnd: 60_000 }, 2)
		await store.put({ key: 'b', windowStart: 70_000, windowEnd: 80_000 }, 3)

		// currentTime - retentionMs = 100_000 - 60_000 = 40_000.
		// Sessions with windowEnd < 40_000 should be removed: only the (0, 1_000) one.
		const expired = await store.expireOldSessions(100_000)

		expect(expired.length).toBe(1)
		expect(expired[0]).toEqual({ key: 'a', windowStart: 0, windowEnd: 1_000 })
		expect(await store.approximateNumEntries()).toBe(2)
	})

	it('ChangelogBackedSessionStore.expireOldSessions emits a tombstone for each expired key', async () => {
		const inner = new InMemorySessionStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			retentionMs: 60_000,
		})
		await inner.init()
		await inner.put({ key: 'a', windowStart: 0, windowEnd: 1_000 }, 1)
		await inner.put({ key: 'a', windowStart: 50_000, windowEnd: 60_000 }, 2)

		const writer = { write: vi.fn(), writeTombstone: vi.fn() }
		const wrapped = new ChangelogBackedSessionStore(
			inner,
			writer as unknown as ChangelogWriter<WindowedKey<string>, number>
		)
		const expired = await wrapped.expireOldSessions(100_000)

		expect(expired.length).toBe(1)
		expect(writer.write).not.toHaveBeenCalled()
		expect(writer.writeTombstone.mock.calls).toEqual([[{ key: 'a', windowStart: 0, windowEnd: 1_000 }]])
	})
})
