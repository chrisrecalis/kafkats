import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { lmdb } from '@/lmdb.js'

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

describe('LMDBWindowStore time bounds', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'lmdb-win-'))
	})
	afterEach(() => {
		try {
			rmSync(stateDir, { recursive: true, force: true })
		} catch {
			// best effort
		}
	})

	it('fetchAll includes windows with windowStart == timeTo regardless of windowEnd', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('w', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1000,
			retentionMs: 60_000,
		})
		await store.init()

		// Window starting at timeTo with windowEnd > timeTo+1: previous code missed this.
		await store.put({ key: 'a', windowStart: 100, windowEnd: 200 }, 1)
		await store.put({ key: 'b', windowStart: 100, windowEnd: 1100 }, 2)
		await store.put({ key: 'c', windowStart: 50, windowEnd: 150 }, 3)
		await store.put({ key: 'd', windowStart: 200, windowEnd: 300 }, 4) // outside

		const results: Array<{ key: string; start: number; end: number; value: number }> = []
		for await (const [wk, v] of store.fetchAll(50, 100)) {
			results.push({ key: wk.key, start: wk.windowStart, end: wk.windowEnd, value: v })
		}

		const keys = results.map(r => r.key).sort()
		// Must include a (start=100,end=200), b (start=100,end=1100), c (start=50,end=150).
		// d (start=200) is outside [50,100].
		expect(keys).toEqual(['a', 'b', 'c'])

		await store.close()
		await provider.close()
	})

	it('expireOldWindows actually removes old windows', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('w', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1000,
			retentionMs: 1000,
		})
		await store.init()

		// Put windows: one old (should expire), one current (should remain).
		await store.put({ key: 'old', windowStart: 0, windowEnd: 100 }, 1)
		await store.put({ key: 'current', windowStart: 9000, windowEnd: 10_000 }, 2)

		// At streamTime=10000, retentionMs=1000 → cutoff=9000.
		// Old window (end=100) should be removed; current window (end=10000) should stay.
		const removed = await store.expireOldWindows(10_000)
		expect(removed).toBeGreaterThan(0)

		const remaining: string[] = []
		for await (const [wk] of store.fetchAll(0, 20_000)) {
			remaining.push(wk.key)
		}
		expect(remaining).toContain('current')
		expect(remaining).not.toContain('old')

		await store.close()
		await provider.close()
	})
})
