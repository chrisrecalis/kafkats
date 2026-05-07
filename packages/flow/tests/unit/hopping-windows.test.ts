import { describe, it, expect, vi } from 'vitest'

import { WindowedAggregateNode, WindowedReduceNode } from '@/processors/aggregation.js'
import type { WindowStore, WindowedKey } from '@/state.js'

function makeStubStore<K, A>() {
	const data = new Map<string, A>()
	const store: WindowStore<K, A> = {
		name: 's',
		get: vi.fn(async (key: WindowedKey<K>) => data.get(JSON.stringify(key)) ?? undefined),
		put: vi.fn(async (key: WindowedKey<K>, value: A) => {
			data.set(JSON.stringify(key), value)
		}),
		delete: vi.fn(async () => {}),
		all: vi.fn(),
		range: vi.fn(),
		fetch: vi.fn(),
		fetchAll: vi.fn(),
		fetchRange: vi.fn(),
		expireOldWindows: vi.fn().mockResolvedValue(0),
		approximateNumEntries: vi.fn().mockResolvedValue(0),
		init: vi.fn(),
		flush: vi.fn(),
		close: vi.fn(),
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
	} as any
	return store
}

describe('hopping windows', () => {
	it('WindowedAggregateNode aggregates a record into every overlapping window', async () => {
		// size=1000, advance=500 → 2 overlapping windows per record.
		// Record at t=750 belongs to:
		//  - window [0, 1000)   (started at 0, covers 0..999)
		//  - window [500, 1500) (started at 500, covers 500..1499)
		const store = makeStubStore<string, number>()
		const ref = { store }
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			ref,
			() => 0,
			(_k, v, agg) => agg + v,
			1000,
			500
		)
		const forwards: Array<{ start: number; end: number; value: number }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (record: any) => {
			forwards.push({ start: record.key.window.start, end: record.key.window.end, value: record.value })
		}

		await node.process({
			key: 'a',
			value: 5,
			timestamp: 750n,
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})

		// Expect TWO forwards, one per overlapping window.
		expect(forwards).toHaveLength(2)
		const sorted = [...forwards].sort((a, b) => a.start - b.start)
		expect(sorted[0]).toEqual({ start: 0, end: 1000, value: 5 })
		expect(sorted[1]).toEqual({ start: 500, end: 1500, value: 5 })
	})

	it('WindowedReduceNode reduces a record into every overlapping window', async () => {
		const store = makeStubStore<string, number>()
		const ref = { store }
		const node = new WindowedReduceNode<string, number>('s', ref, (a, b) => a + b, 1000, 500)
		const forwards: Array<{ start: number; end: number; value: number }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (record: any) => {
			forwards.push({ start: record.key.window.start, end: record.key.window.end, value: record.value })
		}

		await node.process({
			key: 'a',
			value: 7,
			timestamp: 750n,
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})

		expect(forwards).toHaveLength(2)
	})

	it('throws when advanceBy is greater than window size (would silently drop records otherwise)', () => {
		const store = makeStubStore<string, number>()
		expect(
			() =>
				new WindowedAggregateNode<string, number, number>(
					's',
					{ store },
					() => 0,
					(_k, v, agg) => agg + v,
					1000,
					2000 // advanceMs > windowSizeMs — illegal
				)
		).toThrow(/advanceBy must be <= window size/)

		expect(
			() => new WindowedReduceNode<string, number>('s', { store }, (a, b) => a + b, 1000, 2000)
		).toThrow(/advanceBy must be <= window size/)
	})

	it('tumbling windows (advance == size) still produce exactly one update per record', async () => {
		const store = makeStubStore<string, number>()
		const ref = { store }
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			ref,
			() => 0,
			(_k, v, agg) => agg + v,
			1000
			// advance defaults to size
		)
		const forwards: Array<{ start: number; end: number }> = []
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async (record: any) => {
			forwards.push({ start: record.key.window.start, end: record.key.window.end })
		}

		await node.process({
			key: 'a',
			value: 1,
			timestamp: 750n,
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})

		expect(forwards).toEqual([{ start: 0, end: 1000 }])
	})
})
