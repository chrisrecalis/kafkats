import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { inMemory, type SessionStore, type WindowStore, type WindowedKey } from '@kafkats/flow'

import { rocksdb } from '@/rocksdb.js'
import { numberCodec, stringCodec } from './codecs.js'

/** Deterministic pseudo-random numbers (mulberry32). */
function random(seed: number): () => number {
	return () => {
		seed = (seed + 0x6d2b79f5) | 0
		let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296
	}
}

async function collect(iterable: AsyncIterable<[WindowedKey<string>, number]>): Promise<string[]> {
	const out: string[] = []
	for await (const [k, v] of iterable) out.push(`${k.key}@${k.windowStart}-${k.windowEnd}=${v}`)
	return out
}

const KEYS = ['a', 'ab', 'b', 'k', 'ka', 'kb']

/**
 * The per-key lookups made while processing records (fetch, findSessions) must return the same
 * entries in the same (time) order from both providers.
 */
describe('RocksDB and in-memory windowed stores agree', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'rocksdb-parity-'))
	})
	afterEach(() => {
		rmSync(stateDir, { recursive: true, force: true })
	})

	it('window stores: fetch after random puts, deletes and expiry', async () => {
		const options = { keyCodec: stringCodec, valueCodec: numberCodec, windowSizeMs: 100, retentionMs: 2_000 }
		const provider = rocksdb({ stateDir })
		const stores: WindowStore<string, number>[] = [
			provider.createWindowStore('w', options),
			inMemory().createWindowStore('w', options),
		]
		for (const store of stores) await store.init()

		const next = random(42)
		for (let i = 0; i < 2_000; i++) {
			const key = KEYS[Math.floor(next() * KEYS.length)]!
			const windowStart = Math.floor(next() * 40) * 50 - 500
			const windowed = { key, windowStart, windowEnd: windowStart + 50 + Math.floor(next() * 3) * 50 }
			const op = next()
			for (const store of stores) {
				if (op < 0.8) await store.put(windowed, i)
				else await store.delete(windowed)
			}
			if (i % 500 === 499) {
				const expired = await Promise.all(stores.map(s => s.expireOldWindows(1_200)))
				expect(expired[1]).toBe(expired[0])
			}
			if (i % 50 === 0) {
				const from = Math.floor(next() * 40) * 50 - 600
				const to = from + Math.floor(next() * 1_000)
				const [fromRocks, fromMemory] = await Promise.all(stores.map(s => collect(s.fetch(key, from, to))))
				expect(fromMemory).toEqual(fromRocks)
			}
		}
		for (const key of KEYS) {
			const [fromRocks, fromMemory] = await Promise.all(stores.map(s => collect(s.fetch(key, -10_000, 10_000))))
			expect(fromMemory).toEqual(fromRocks)
		}
		const [allRocks, allMemory] = await Promise.all(stores.map(s => collect(s.all())))
		expect([...allMemory!].sort()).toEqual([...allRocks!].sort())
		// RocksDB estimates the count from table statistics; only "empty or not" is exact.
		expect((await stores[0]!.approximateNumEntries()) > 0).toBe((await stores[1]!.approximateNumEntries()) > 0)
		await provider.close()
	})

	it('session stores: findSessions after random puts, deletes, removes and expiry', async () => {
		const options = { keyCodec: stringCodec, valueCodec: numberCodec, retentionMs: 2_000 }
		const provider = rocksdb({ stateDir })
		const stores: SessionStore<string, number>[] = [
			provider.createSessionStore('s', options),
			inMemory().createSessionStore('s', options),
		]
		for (const store of stores) await store.init()

		const next = random(7)
		for (let i = 0; i < 2_000; i++) {
			const key = KEYS[Math.floor(next() * KEYS.length)]!
			const windowStart = Math.floor(next() * 400) - 100
			const windowed = { key, windowStart, windowEnd: windowStart + Math.floor(next() * 200) }
			const op = next()
			for (const store of stores) {
				if (op < 0.85) await store.put(windowed, i)
				else if (op < 0.98) await store.delete(windowed)
				else await store.remove(key)
			}
			if (i % 500 === 499) {
				const [expiredLmdb, expiredMemory] = await Promise.all(stores.map(s => s.expireOldSessions(2_100)))
				const show = (keys: WindowedKey<string>[]) =>
					keys.map(k => `${k.key}@${k.windowStart}-${k.windowEnd}`).sort()
				expect(show(expiredMemory!)).toEqual(show(expiredLmdb!))
			}
			if (i % 50 === 0) {
				const earliestStart = Math.floor(next() * 400) - 150
				const latestEnd = earliestStart + Math.floor(next() * 300)
				const [fromRocks, fromMemory] = await Promise.all(
					stores.map(s => collect(s.findSessions(key, earliestStart, latestEnd)))
				)
				expect(fromMemory).toEqual(fromRocks)
			}
		}
		for (const key of KEYS) {
			const [fromRocks, fromMemory] = await Promise.all(
				stores.map(s => collect(s.findSessions(key, -10_000, 10_000)))
			)
			expect(fromMemory).toEqual(fromRocks)
		}
		// RocksDB estimates the count from table statistics; only "empty or not" is exact.
		expect((await stores[0]!.approximateNumEntries()) > 0).toBe((await stores[1]!.approximateNumEntries()) > 0)
		await provider.close()
	})
})
