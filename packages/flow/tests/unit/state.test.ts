import { describe, expect, it, beforeEach, afterEach } from 'vitest'
import {
	inMemory,
	InMemoryKeyValueStore,
	InMemoryWindowStore,
	InMemorySessionStore,
	type StateStoreProvider,
	type KeyValueStore,
	type WindowStore,
	type SessionStore,
	type WindowedKey,
	codec,
} from '../../src/index.js'

describe('InMemoryKeyValueStore', () => {
	let store: KeyValueStore<string, number>

	beforeEach(async () => {
		store = new InMemoryKeyValueStore('test-store', {
			keyCodec: codec.string(),
			valueCodec: {
				encode: (v: number) => Buffer.from(v.toString()),
				decode: (b: Buffer) => parseInt(b.toString(), 10),
			},
		})
		await store.init()
	})

	afterEach(async () => {
		await store.close()
	})

	it('stores and retrieves values', async () => {
		await store.put('key1', 100)
		await store.put('key2', 200)

		expect(await store.get('key1')).toBe(100)
		expect(await store.get('key2')).toBe(200)
		expect(await store.get('key3')).toBeUndefined()
	})

	it('deletes values', async () => {
		await store.put('key1', 100)
		expect(await store.get('key1')).toBe(100)

		await store.delete('key1')
		expect(await store.get('key1')).toBeUndefined()
	})

	it('iterates over all entries', async () => {
		await store.put('a', 1)
		await store.put('b', 2)
		await store.put('c', 3)

		const entries: [string, number][] = []
		for await (const entry of store.all()) {
			entries.push(entry)
		}

		expect(entries).toHaveLength(3)
		expect(entries.map(([k]) => k).sort()).toEqual(['a', 'b', 'c'])
	})

	it('returns approximate entry count', async () => {
		await store.put('a', 1)
		await store.put('b', 2)

		expect(await store.approximateNumEntries()).toBe(2)
	})
})

describe('InMemoryWindowStore', () => {
	let store: WindowStore<string, number>

	beforeEach(async () => {
		store = new InMemoryWindowStore('test-window-store', {
			keyCodec: codec.string(),
			valueCodec: {
				encode: (v: number) => Buffer.from(v.toString()),
				decode: (b: Buffer) => parseInt(b.toString(), 10),
			},
			retentionMs: 60000,
			windowSizeMs: 10000,
		})
		await store.init()
	})

	afterEach(async () => {
		await store.close()
	})

	it('stores and retrieves windowed values', async () => {
		const key: WindowedKey<string> = { key: 'user1', windowStart: 1000, windowEnd: 2000 }
		await store.put(key, 42)

		expect(await store.get(key)).toBe(42)
	})

	it('fetches values for a key within time range', async () => {
		await store.put({ key: 'user1', windowStart: 1000, windowEnd: 2000 }, 10)
		await store.put({ key: 'user1', windowStart: 2000, windowEnd: 3000 }, 20)
		await store.put({ key: 'user1', windowStart: 5000, windowEnd: 6000 }, 30)
		await store.put({ key: 'user2', windowStart: 1000, windowEnd: 2000 }, 100)

		const results: [WindowedKey<string>, number][] = []
		for await (const entry of store.fetch('user1', 0, 3500)) {
			results.push(entry)
		}

		expect(results).toHaveLength(2)
		expect(results.map(([, v]) => v).sort((a, b) => a - b)).toEqual([10, 20])
	})

	it('fetches all values within time range', async () => {
		await store.put({ key: 'user1', windowStart: 1000, windowEnd: 2000 }, 10)
		await store.put({ key: 'user2', windowStart: 1500, windowEnd: 2500 }, 20)
		await store.put({ key: 'user3', windowStart: 5000, windowEnd: 6000 }, 30)

		const results: [WindowedKey<string>, number][] = []
		for await (const entry of store.fetchAll(0, 3000)) {
			results.push(entry)
		}

		expect(results).toHaveLength(2)
	})
})

describe('InMemorySessionStore', () => {
	let store: SessionStore<string, number>

	beforeEach(async () => {
		store = new InMemorySessionStore('test-session-store', {
			keyCodec: codec.string(),
			valueCodec: {
				encode: (v: number) => Buffer.from(v.toString()),
				decode: (b: Buffer) => parseInt(b.toString(), 10),
			},
			retentionMs: 60000,
		})
		await store.init()
	})

	afterEach(async () => {
		await store.close()
	})

	it('finds overlapping sessions', async () => {
		await store.put({ key: 'user1', windowStart: 1000, windowEnd: 2000 }, 10)
		await store.put({ key: 'user1', windowStart: 3000, windowEnd: 4000 }, 20)
		await store.put({ key: 'user1', windowStart: 6000, windowEnd: 7000 }, 30)

		// Find sessions that overlap with [1500, 3500]
		const results: [WindowedKey<string>, number][] = []
		for await (const entry of store.findSessions('user1', 1500, 3500)) {
			results.push(entry)
		}

		expect(results).toHaveLength(2)
		expect(results.map(([, v]) => v).sort((a, b) => a - b)).toEqual([10, 20])
	})

	it('removes all sessions for a key', async () => {
		await store.put({ key: 'user1', windowStart: 1000, windowEnd: 2000 }, 10)
		await store.put({ key: 'user1', windowStart: 3000, windowEnd: 4000 }, 20)
		await store.put({ key: 'user2', windowStart: 1000, windowEnd: 2000 }, 100)

		await store.remove('user1')

		expect(await store.approximateNumEntries()).toBe(1)
	})
})

describe('InMemoryStateStoreProvider', () => {
	let provider: StateStoreProvider

	beforeEach(() => {
		provider = inMemory()
	})

	afterEach(async () => {
		await provider.close()
	})

	it('creates key-value stores', async () => {
		const store = provider.createKeyValueStore<string, string>('kv-store', {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
		})

		await store.init()
		await store.put('hello', 'world')
		expect(await store.get('hello')).toBe('world')
	})

	it('creates window stores', async () => {
		const store = provider.createWindowStore<string, number>('window-store', {
			keyCodec: codec.string(),
			valueCodec: {
				encode: (v: number) => Buffer.from(v.toString()),
				decode: (b: Buffer) => parseInt(b.toString(), 10),
			},
			retentionMs: 60000,
			windowSizeMs: 10000,
		})

		await store.init()
		await store.put({ key: 'k', windowStart: 0, windowEnd: 1000 }, 42)
		expect(await store.get({ key: 'k', windowStart: 0, windowEnd: 1000 })).toBe(42)
	})

	it('creates session stores', async () => {
		const store = provider.createSessionStore<string, number>('session-store', {
			keyCodec: codec.string(),
			valueCodec: {
				encode: (v: number) => Buffer.from(v.toString()),
				decode: (b: Buffer) => parseInt(b.toString(), 10),
			},
			retentionMs: 60000,
		})

		await store.init()
		await store.put({ key: 'k', windowStart: 0, windowEnd: 1000 }, 42)
		expect(await store.get({ key: 'k', windowStart: 0, windowEnd: 1000 })).toBe(42)
	})

	it('closes all stores', async () => {
		const store1 = provider.createKeyValueStore<string, string>('store1', {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
		})
		const store2 = provider.createKeyValueStore<string, string>('store2', {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
		})

		await store1.init()
		await store2.init()
		await store1.put('a', 'b')
		await store2.put('c', 'd')

		await provider.close()

		// After close, stores should be cleared
		expect(await store1.approximateNumEntries()).toBe(0)
		expect(await store2.approximateNumEntries()).toBe(0)
	})
})
