import { describe, it, expect, vi } from 'vitest'

import {
	StreamStreamJoinNode,
	StreamStreamLeftJoinNode,
	StreamStreamOuterJoinNode,
} from '@/processors/joins/stream-stream.js'
import type { StreamRecord } from '@/processors/base.js'
import type { WindowStore, WindowedKey } from '@/state.js'
import { InMemoryWindowStore, codec } from '../../src/index.js'

const numberCodec = {
	encode: (n: number) => Buffer.from(String(n)),
	decode: (b: Buffer) => Number(b.toString()),
}

function makeStubStore<K, V>() {
	const expireSpy = vi.fn<(cutoff: number) => Promise<number>>().mockResolvedValue(0)
	async function* empty(): AsyncIterable<[WindowedKey<K>, V]> {
		// no entries
	}
	const store = {
		name: 's',
		get: vi.fn(async () => undefined),
		put: vi.fn(async () => {}),
		delete: vi.fn(async () => {}),
		all: vi.fn(() => empty()),
		range: vi.fn(() => empty()),
		fetch: vi.fn(() => empty()),
		fetchAll: vi.fn(() => empty()),
		fetchRange: vi.fn(() => empty()),
		expireOldWindows: expireSpy,
		approximateNumEntries: vi.fn().mockResolvedValue(0),
		init: vi.fn(),
		flush: vi.fn(),
		close: vi.fn(),
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any as WindowStore<K, V>
	return { store, expireSpy }
}

function record(ts: number, offset: number): StreamRecord<string, number> {
	return {
		key: 'a',
		value: 1,
		timestamp: BigInt(ts),
		topic: 't',
		partition: 0,
		offset: BigInt(offset),
		headers: {},
	}
}

type Side = WindowStore<string, number>

const nodeFactories = {
	inner: (l: Side, r: Side) =>
		new StreamStreamJoinNode<string, number, number, string>(
			{ store: l },
			{ store: r },
			(a, b) => `${a}:${b}`,
			1000
		),
	left: (l: Side, r: Side) =>
		new StreamStreamLeftJoinNode<string, number, number, string>(
			{ store: l },
			{ store: r },
			(a, b) => `${a}:${b}`,
			1000
		),
	outer: (l: Side, r: Side) =>
		new StreamStreamOuterJoinNode<string, number, number, string>(
			{ store: l },
			{ store: r },
			(a, b) => `${a}:${b}`,
			1000
		),
} as const

describe('Stream-stream join store expiry', () => {
	for (const kind of ['inner', 'left', 'outer'] as const) {
		it(`${kind} join expires BOTH window stores as stream time advances`, async () => {
			const left = makeStubStore<string, number>()
			const right = makeStubStore<string, number>()
			const node = nodeFactories[kind](left.store, right.store)

			await node.process(record(1_000_000_000_000, 0))

			// Both this stream's store and the other stream's store must be expired, so a
			// join store never grows unbounded — even if one input stream goes idle.
			expect(left.expireSpy).toHaveBeenCalledTimes(1)
			expect(right.expireSpy).toHaveBeenCalledTimes(1)
			// Expiry is driven by stream time (the record timestamp), not wall clock.
			expect(left.expireSpy.mock.calls[0]![0]).toBe(1_000_000_000_000)
		})
	}

	it('does not re-expire until stream time advances past the cleanup interval', async () => {
		const left = makeStubStore<string, number>()
		const right = makeStubStore<string, number>()
		const node = nodeFactories.inner(left.store, right.store)

		await node.process(record(1_000_000_000_000, 0))
		expect(left.expireSpy).toHaveBeenCalledTimes(1)

		// +100ms is below the 60s cleanup interval → no additional expiry.
		await node.process(record(1_000_000_000_100, 1))
		expect(left.expireSpy).toHaveBeenCalledTimes(1)
	})

	it('bounds a REAL window store as stream time advances far beyond retention', async () => {
		const joinWindowMs = 1000
		const makeStore = () =>
			new InMemoryWindowStore<string, number>('s', {
				keyCodec: codec.string(),
				valueCodec: numberCodec,
				retentionMs: joinWindowMs * 2,
				windowSizeMs: joinWindowMs,
			})
		const left = makeStore()
		const right = makeStore()
		const node = new StreamStreamJoinNode<string, number, number, string>(
			{ store: left },
			{ store: right },
			(a, b) => `${a}:${b}`,
			joinWindowMs
		)

		const base = 1_000_000_000_000
		// 20 records, each 70s apart — well past the 60s cleanup interval, so expiry fires
		// on every record and retention (2s) keeps only the most recent window(s).
		for (let i = 0; i < 20; i++) {
			await node.process(record(base + i * 70_000, i))
		}

		// Without expiry the store would hold all 20 windows (the unbounded-growth bug).
		expect(await left.approximateNumEntries()).toBeLessThanOrEqual(2)
	})
})
