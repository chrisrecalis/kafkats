import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { InMemorySessionStore, InMemoryWindowStore } from '@/state/memory.js'
import type { WindowedKey } from '@/state.js'

const options = { keyCodec: codec.string(), valueCodec: codec.json<number>(), retentionMs: 1000, windowSizeMs: 100 }

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
	const out: T[] = []
	for await (const item of iterable) out.push(item)
	return out
}

const windows = (entries: Array<[WindowedKey<string>, number]>) =>
	entries.map(([k, v]) => `${k.key}@${k.windowStart}-${k.windowEnd}=${v}`)

describe('in-memory windowed stores (entries grouped by user key)', () => {
	it('fetch returns only the windows of that exact key, in time order', async () => {
		const store = new InMemoryWindowStore<string, number>('w', options)
		// Inserted out of time order, with keys sharing a prefix.
		await store.put({ key: 'k', windowStart: 300, windowEnd: 400 }, 3)
		await store.put({ key: 'ka', windowStart: 100, windowEnd: 200 }, 9)
		await store.put({ key: 'k', windowStart: 100, windowEnd: 300 }, 2)
		await store.put({ key: 'k', windowStart: 100, windowEnd: 200 }, 1)
		await store.put({ key: 'k', windowStart: -50, windowEnd: 50 }, 0)

		expect(windows(await collect(store.fetch('k', -100, 300)))).toEqual([
			'k@-50-50=0',
			'k@100-200=1',
			'k@100-300=2',
			'k@300-400=3',
		])
		expect(windows(await collect(store.fetch('k', 0, 200)))).toEqual(['k@100-200=1', 'k@100-300=2'])
		expect(await collect(store.fetch('missing', 0, 1000))).toEqual([])
	})

	it('put overwrites, delete removes, and the entry count follows', async () => {
		const store = new InMemoryWindowStore<string, number>('w', options)
		const key = { key: 'a', windowStart: 0, windowEnd: 100 }
		await store.put(key, 1)
		await store.put(key, 2)
		await store.put({ key: 'b', windowStart: 0, windowEnd: 100 }, 5)
		expect(await store.get(key)).toBe(2)
		expect(await store.get({ key: 'a', windowStart: 0, windowEnd: 101 })).toBeUndefined()
		expect(await store.approximateNumEntries()).toBe(2)

		await store.delete(key)
		await store.delete({ key: 'nope', windowStart: 0, windowEnd: 100 })
		expect(await store.get(key)).toBeUndefined()
		expect(await store.approximateNumEntries()).toBe(1)
		expect(windows(await collect(store.all()))).toEqual(['b@0-100=5'])
	})

	it('expireOldWindows removes windows ending before the retention cutoff, across keys', async () => {
		const store = new InMemoryWindowStore<string, number>('w', options)
		await store.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 1)
		await store.put({ key: 'a', windowStart: 900, windowEnd: 1000 }, 2)
		await store.put({ key: 'b', windowStart: 50, windowEnd: 150 }, 3)

		// cutoff = 1200 - 1000 = 200: windows ending before 200 expire.
		expect(await store.expireOldWindows(1200)).toBe(2)
		expect(windows(await collect(store.all()))).toEqual(['a@900-1000=2'])
		expect(await store.approximateNumEntries()).toBe(1)
	})

	it('fetchAll and fetchRange scan every key', async () => {
		const store = new InMemoryWindowStore<string, number>('w', options)
		await store.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 1)
		await store.put({ key: 'b', windowStart: 100, windowEnd: 200 }, 2)
		await store.put({ key: 'c', windowStart: 200, windowEnd: 300 }, 3)

		expect(windows(await collect(store.fetchAll(0, 100))).sort()).toEqual(['a@0-100=1', 'b@100-200=2'])
		expect(windows(await collect(store.fetchRange('b', 'c', 0, 1000))).sort()).toEqual([
			'b@100-200=2',
			'c@200-300=3',
		])
	})

	it('findSessions returns the overlapping sessions of that key in time order; remove and expiry drop them', async () => {
		const store = new InMemorySessionStore<string, number>('s', options)
		await store.put({ key: 'u', windowStart: 600, windowEnd: 700 }, 3)
		await store.put({ key: 'u', windowStart: 100, windowEnd: 200 }, 1)
		await store.put({ key: 'u', windowStart: 300, windowEnd: 400 }, 2)
		await store.put({ key: 'u2', windowStart: 100, windowEnd: 200 }, 9)

		expect(windows(await collect(store.findSessions('u', 150, 350)))).toEqual(['u@100-200=1', 'u@300-400=2'])

		const expired = await store.expireOldSessions(1250) // cutoff 250
		expect(expired.map(k => `${k.key}@${k.windowStart}`).sort()).toEqual(['u2@100', 'u@100'])
		expect(windows(await collect(store.findSessions('u', 0, 1000)))).toEqual(['u@300-400=2', 'u@600-700=3'])

		await store.remove('u')
		expect(await collect(store.findSessions('u', 0, 1000))).toEqual([])
		expect(await store.approximateNumEntries()).toBe(0)
	})

	it('a key deleted while all() is being iterated is not yielded afterwards', async () => {
		const store = new InMemorySessionStore<string, number>('s', options)
		for (const key of ['a', 'b', 'c']) await store.put({ key, windowStart: 0, windowEnd: 10 }, 1)
		const seen: string[] = []
		for await (const [k] of store.all()) {
			seen.push(k.key)
			if (k.key === 'a') await store.remove('b')
		}
		expect(seen).toEqual(['a', 'c'])
	})
})
