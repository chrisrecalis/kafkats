import { describe, it, expect, vi } from 'vitest'

import { WindowedAggregateNode, SessionAggregateNode } from '@/processors/aggregation.js'
import type { StreamRecord } from '@/processors/base.js'
import type { SessionStore, WindowStore, WindowedKey } from '@/state.js'

function rec(key: string, value: number, ts: number): StreamRecord<string, number> {
	return { key, value, timestamp: BigInt(ts), topic: 't', partition: 0, offset: 0n, headers: {} }
}

function makeWindowStore() {
	const putSpy = vi.fn(async () => {})
	async function* empty(): AsyncIterable<[WindowedKey<string>, number]> {
		// no entries
	}
	const store = {
		name: 's',
		get: vi.fn(async () => undefined),
		put: putSpy,
		delete: vi.fn(async () => {}),
		all: vi.fn(() => empty()),
		range: vi.fn(() => empty()),
		fetch: vi.fn(() => empty()),
		fetchAll: vi.fn(() => empty()),
		fetchRange: vi.fn(() => empty()),
		expireOldWindows: vi.fn(async () => 0),
		approximateNumEntries: vi.fn(async () => 0),
		init: vi.fn(),
		flush: vi.fn(),
		close: vi.fn(),
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any as WindowStore<string, number>
	return { store, putSpy }
}

function makeSessionStore(overlapping: Array<[WindowedKey<string>, number]> = []) {
	const putSpy = vi.fn(async () => {})
	async function* sessions(): AsyncIterable<[WindowedKey<string>, number]> {
		for (const entry of overlapping) yield entry
	}
	const store = {
		name: 's',
		get: vi.fn(async () => undefined),
		put: putSpy,
		delete: vi.fn(async () => {}),
		all: vi.fn(),
		range: vi.fn(),
		findSessions: vi.fn(() => sessions()),
		remove: vi.fn(async () => {}),
		expireOldSessions: vi.fn(async () => []),
		approximateNumEntries: vi.fn(async () => 0),
		init: vi.fn(),
		flush: vi.fn(),
		close: vi.fn(),
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any as SessionStore<string, number>
	return { store, putSpy }
}

const WINDOW_MS = 1000
const BASE = 1_000_000_000_000
// 30s late comfortably exceeds the 24s retention (WINDOW_MS * 24).

describe('WindowedAggregateNode late-record handling', () => {
	it('drops a record whose window is already past retention instead of resurrecting it', async () => {
		const { store, putSpy } = makeWindowStore()
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			WINDOW_MS
		)

		// On-time record advances stream time to BASE (one window written).
		await node.process(rec('a', 5, BASE))
		expect(putSpy).toHaveBeenCalledTimes(1)

		// Record arriving well past retention (30s late > 24s retention): its window is gone,
		// so it must be dropped, not re-created from the initializer.
		await node.process(rec('a', 99, BASE - 30_000))
		expect(putSpy).toHaveBeenCalledTimes(1)
	})

	it('still processes a moderately-late record that is within retention', async () => {
		const { store, putSpy } = makeWindowStore()
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			WINDOW_MS
		)

		await node.process(rec('a', 5, BASE))
		expect(putSpy).toHaveBeenCalledTimes(1)

		// 10s late is within the 24s retention → still aggregated.
		await node.process(rec('a', 7, BASE - 10_000))
		expect(putSpy).toHaveBeenCalledTimes(2)
	})
})

describe('SessionAggregateNode late-record handling', () => {
	it('drops a late record that has no live session to merge into', async () => {
		const { store, putSpy } = makeSessionStore([]) // no overlapping sessions
		const node = new SessionAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			WINDOW_MS
		)

		await node.process(rec('a', 5, BASE))
		expect(putSpy).toHaveBeenCalledTimes(1)

		await node.process(rec('a', 99, BASE - 30_000))
		expect(putSpy).toHaveBeenCalledTimes(1)
	})

	it('still processes a late record that merges into a live session', async () => {
		// A live (within-retention) session exists for the key, so the late record must merge
		// into it rather than being dropped.
		const liveSession: [WindowedKey<string>, number] = [
			{ key: 'a', windowStart: BASE - 30_500, windowEnd: BASE - 30_000 },
			42,
		]
		const { store, putSpy } = makeSessionStore([liveSession])
		const node = new SessionAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			WINDOW_MS
		)

		await node.process(rec('a', 5, BASE))
		expect(putSpy).toHaveBeenCalledTimes(1)

		await node.process(rec('a', 99, BASE - 30_000))
		expect(putSpy).toHaveBeenCalledTimes(2)
	})
})
