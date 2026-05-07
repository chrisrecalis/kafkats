import { describe, it, expect, vi } from 'vitest'

import { WindowedAggregateNode } from '@/processors/aggregation.js'
import type { WindowStore, WindowedKey } from '@/state.js'

describe('WindowedAggregateNode stream-time cleanup', () => {
	function makeStubStore<K, A>() {
		const data = new Map<string, A>()
		const expireSpy = vi.fn().mockResolvedValue(0)
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
			expireOldWindows: expireSpy,
			approximateNumEntries: vi.fn().mockResolvedValue(0),
			init: vi.fn(),
			flush: vi.fn(),
			close: vi.fn(),
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
		} as any
		return { store, expireSpy }
	}

	it('uses stream time (record timestamp), not wall clock, for expiry checks', async () => {
		const { store, expireSpy } = makeStubStore<string, number>()
		const ref = { store }
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			ref,
			() => 0,
			(_k, v, agg) => agg + v,
			1000 // window size 1s
		)

		// Force forward to no-op
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async () => {}

		const baseTime = 1_000_000_000_000 // arbitrary stream time origin

		// Single record at stream time = baseTime; cleanup interval = 60_000ms.
		// First record advances streamTime to baseTime; lastCleanupStreamTime
		// starts at 0, so the diff is huge → expire fires with streamTime, not now.
		await node.process({
			key: 'a',
			value: 1,
			timestamp: BigInt(baseTime),
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})

		expect(expireSpy).toHaveBeenCalledTimes(1)
		// The expiry argument must be the stream time (baseTime), not Date.now().
		expect(expireSpy.mock.calls[0]![0]).toBe(baseTime)
	})

	it('does NOT trigger expiry when stream time has not advanced past the cleanup threshold', async () => {
		const { store, expireSpy } = makeStubStore<string, number>()
		const ref = { store }
		const node = new WindowedAggregateNode<string, number, number>(
			's',
			ref,
			() => 0,
			(_k, v, agg) => agg + v,
			1000
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(node as any).forward = async () => {}

		// Initialize cleanupState's lastCleanupStreamTimeMs by feeding an early record
		await node.process({
			key: 'a',
			value: 1,
			timestamp: 1_000_000_000_000n,
			topic: 't',
			partition: 0,
			offset: 0n,
			headers: {},
		})
		expect(expireSpy).toHaveBeenCalledTimes(1)

		// Second record only 100ms later in stream time — well under the 60s threshold.
		await node.process({
			key: 'a',
			value: 1,
			timestamp: 1_000_000_000_100n,
			topic: 't',
			partition: 0,
			offset: 1n,
			headers: {},
		})

		// Should still be 1 — expiry should NOT have run again.
		expect(expireSpy).toHaveBeenCalledTimes(1)
	})
})
