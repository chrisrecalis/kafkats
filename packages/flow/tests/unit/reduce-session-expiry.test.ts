import { describe, expect, it, vi } from 'vitest'

import { WindowedReduceNode, SessionReduceNode, SessionAggregateNode } from '../../src/processors/aggregation.js'
import type { SessionStore, WindowStore, WindowedKey } from '../../src/state.js'
import { InMemorySessionStore } from '../../src/state/memory.js'
import { ChangelogBackedSessionStore } from '../../src/state/changelog.js'
import type { ChangelogWriter } from '../../src/changelog.js'

const stringCodec = {
	encode: (s: string) => Buffer.from(s, 'utf-8'),
	decode: (b: Buffer) => b.toString('utf-8'),
}
const numberCodec = {
	encode: (n: number) => {
		const b = Buffer.alloc(8)
		b.writeDoubleLE(n, 0)
		return b
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

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

		const writes: Array<{ tombstone: boolean; key: WindowedKey<string> }> = []
		const writer = {
			write: vi.fn(async (key: WindowedKey<string>) => {
				writes.push({ tombstone: false, key })
			}),
			writeTombstone: vi.fn(async (key: WindowedKey<string>) => {
				writes.push({ tombstone: true, key })
			}),
		}

		const wrapped = new ChangelogBackedSessionStore(
			inner,
			writer as unknown as ChangelogWriter<WindowedKey<string>, number>
		)
		const expired = await wrapped.expireOldSessions(100_000)

		expect(expired.length).toBe(1)
		expect(writer.writeTombstone).toHaveBeenCalledTimes(1)
		expect(writes.filter(w => w.tombstone).map(w => w.key)).toEqual([
			{ key: 'a', windowStart: 0, windowEnd: 1_000 },
		])
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
