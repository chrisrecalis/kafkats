import { describe, expect, it, vi } from 'vitest'

import { WindowedReduceNode, SessionReduceNode, SessionAggregateNode } from '../../src/processors/aggregation.js'
import type { SessionStore, WindowStore, WindowedKey } from '../../src/state.js'

describe('windowed/session reduce + session aggregate retention', () => {
	function makeStubWindowStore<K, V>() {
		const expireSpy = vi.fn().mockResolvedValue(0)
		const data = new Map<string, V>()
		const store = {
			name: 's',
			get: vi.fn(async (k: WindowedKey<K>) => data.get(JSON.stringify(k)) ?? undefined),
			put: vi.fn(async (k: WindowedKey<K>, v: V) => {
				data.set(JSON.stringify(k), v)
			}),
			delete: vi.fn(),
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
		} as unknown as WindowStore<K, V>
		return { store, expireSpy }
	}

	function makeStubSessionStore<K, V>() {
		const expireSpy = vi.fn().mockResolvedValue(0)
		const store = {
			name: 's',
			get: vi.fn().mockResolvedValue(undefined),
			put: vi.fn(),
			delete: vi.fn(),
			all: vi.fn(),
			range: vi.fn(),
			findSessions: async function* () {
				/* no overlapping sessions */
			},
			remove: vi.fn(),
			expireOldSessions: expireSpy,
			approximateNumEntries: vi.fn().mockResolvedValue(0),
			init: vi.fn(),
			flush: vi.fn(),
			close: vi.fn(),
		} as unknown as SessionStore<K, V>
		return { store, expireSpy }
	}

	it('WindowedReduceNode triggers expireOldWindows after the cleanup interval of stream time', async () => {
		const { store, expireSpy } = makeStubWindowStore<string, number>()
		const node = new WindowedReduceNode<string, number>('s', { store }, (a, b) => a + b, 1000)

		// First record at t=baseTime advances streamTime past the 60s cleanup threshold from initial 0.
		const baseTime = 1_000_000_000_000
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
		expect(expireSpy.mock.calls[0]![0]).toBe(baseTime)
	})

	it('SessionReduceNode triggers expireOldSessions after the cleanup interval of stream time', async () => {
		const { store, expireSpy } = makeStubSessionStore<string, number>()
		const node = new SessionReduceNode<string, number>('s', { store }, (a, b) => a + b, 1000)

		const baseTime = 1_000_000_000_000
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
		expect(expireSpy.mock.calls[0]![0]).toBe(baseTime)
	})

	it('SessionAggregateNode triggers expireOldSessions after the cleanup interval of stream time', async () => {
		const { store, expireSpy } = makeStubSessionStore<string, number>()
		const node = new SessionAggregateNode<string, number, number>(
			's',
			{ store },
			() => 0,
			(_k, v, agg) => agg + v,
			(a, b) => a + b,
			1000
		)

		const baseTime = 1_000_000_000_000
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
		expect(expireSpy.mock.calls[0]![0]).toBe(baseTime)
	})

	it('does NOT trigger expiry when stream time has not advanced past the threshold', async () => {
		const { store, expireSpy } = makeStubWindowStore<string, number>()
		const node = new WindowedReduceNode<string, number>('s', { store }, (a, b) => a + b, 1000)

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

		// 100ms later — well under the 60s threshold.
		await node.process({
			key: 'a',
			value: 1,
			timestamp: 1_000_000_000_100n,
			topic: 't',
			partition: 0,
			offset: 1n,
			headers: {},
		})

		expect(expireSpy).toHaveBeenCalledTimes(1)
	})
})
