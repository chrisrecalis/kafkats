import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { lmdb, writeSignedTime, readSignedTime } from '@/lmdb.js'

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

// F5b: LMDB time-key encoding must preserve signed-integer order. Pre-fix `writeBigInt64BE`
// stored negatives as 0xFF…FF which sort AFTER positives in unsigned-byte order, so range
// scans crossing zero (e.g. fetchAll(-1000, 1000)) would silently return nothing.

describe('writeSignedTime/readSignedTime encoding contract', () => {
	it('round-trips signed integers exactly', () => {
		const buf = Buffer.alloc(8)
		for (const t of [-1_000_000, -1, 0, 1, 1_700_000_000_000]) {
			writeSignedTime(buf, t, 0)
			expect(readSignedTime(buf, 0)).toBe(t)
		}
	})

	it('produces lex-byte order matching signed numeric order', () => {
		const sortedTimes = [-1_000_000, -1, 0, 1, 1_000_000]
		const encoded = sortedTimes.map(t => {
			const b = Buffer.alloc(8)
			writeSignedTime(b, t, 0)
			return b
		})
		for (let i = 1; i < encoded.length; i++) {
			expect(encoded[i - 1]!.compare(encoded[i]!)).toBeLessThan(0)
		}
	})
})

describe('LMDBWindowStore signed-time ordering', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'lmdb-signed-'))
	})
	afterEach(() => {
		try {
			rmSync(stateDir, { recursive: true, force: true })
		} catch {
			// best effort
		}
	})

	it('fetchAll returns windows with negative windowStart', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1000,
			retentionMs: 60_000,
		})
		await store.init()

		// Negative-start window — reachable via hopping windows for records near unix epoch.
		await store.put({ key: 'k', windowStart: -500, windowEnd: 500 }, 1)
		await store.put({ key: 'k', windowStart: 0, windowEnd: 1000 }, 2)
		await store.put({ key: 'k', windowStart: 500, windowEnd: 1500 }, 3)

		const results: Array<{ start: number; value: number }> = []
		for await (const [windowedKey, value] of store.fetchAll(-1000, 1000)) {
			results.push({ start: windowedKey.windowStart, value })
		}
		results.sort((a, b) => a.start - b.start)

		// Pre-fix: negative-start window would sort 0xFFFFFFFFFFFFFE0C in unsigned bytes,
		// AFTER the positive starts (0x0000…) — fetchAll would return only positives or
		// nothing depending on bound interpretation. Post-fix biased encoding orders
		// -500 < 0 < 500 correctly.
		expect(results.map(r => r.start)).toEqual([-500, 0, 500])
		expect(results.map(r => r.value)).toEqual([1, 2, 3])

		await store.close()
		await provider.close()
	})

	it('fetch (key-filtered) returns negative-start windows for the right key', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1000,
			retentionMs: 60_000,
		})
		await store.init()

		await store.put({ key: 'a', windowStart: -200, windowEnd: 800 }, 10)
		await store.put({ key: 'a', windowStart: 100, windowEnd: 1100 }, 20)
		await store.put({ key: 'b', windowStart: -200, windowEnd: 800 }, 99)

		const results: number[] = []
		for await (const [, value] of store.fetch('a', -500, 500)) {
			results.push(value)
		}
		expect(results.sort((a, b) => a - b)).toEqual([10, 20])

		await store.close()
		await provider.close()
	})

	it('expireOldWindows correctly removes negative-end windows when cutoff is positive', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1000,
			retentionMs: 1_000,
		})
		await store.init()

		// Old (should expire): windowEnd = -100 — clearly < cutoff.
		await store.put({ key: 'k', windowStart: -1100, windowEnd: -100 }, 1)
		// Crossing zero (should expire): windowEnd = 50 — also < cutoff = 100.
		await store.put({ key: 'k', windowStart: -950, windowEnd: 50 }, 2)
		// Recent (should NOT expire): windowEnd = 5000 > cutoff = 100.
		await store.put({ key: 'k', windowStart: 4000, windowEnd: 5000 }, 3)

		// currentTime = 1100 → cutoff = currentTime - retentionMs = 100.
		await store.expireOldWindows(1100)

		const remaining: number[] = []
		for await (const [, value] of store.fetchAll(-2000, 10000)) {
			remaining.push(value)
		}

		// Pre-fix: scanning by `getRange({ end: timeBound(100) })` against raw-signed keys
		// would miss the negative-start records entirely (they sort after positive starts).
		// Post-fix: biased encoding means the scan correctly enumerates them.
		expect(remaining).toEqual([3])

		await store.close()
		await provider.close()
	})

	it('encodes signed times in monotonically-ordered bytes (pure encoding contract)', async () => {
		// Direct check of the encoding invariant: a sequence of strictly-increasing signed
		// times must produce strictly-increasing byte sequences. Without this, LMDB range
		// scans across signed values are broken.
		const provider = lmdb({ stateDir })
		const store = provider.createWindowStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			windowSizeMs: 1,
			retentionMs: 60_000,
		})
		await store.init()

		const sortedTimes = [-1_000_000, -1, 0, 1, 1_000_000]
		for (const t of sortedTimes) {
			await store.put({ key: 'k', windowStart: t, windowEnd: t + 1 }, t)
		}

		const seen: number[] = []
		// fetchAll iterates in stored byte-order (lex order of biased keys = numeric order of times).
		for await (const [windowedKey] of store.fetchAll(-2_000_000, 2_000_000)) {
			seen.push(windowedKey.windowStart)
		}
		expect(seen).toEqual(sortedTimes)

		await store.close()
		await provider.close()
	})
})

describe('LMDBSessionStore signed-time ordering', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'lmdb-signed-sess-'))
	})
	afterEach(() => {
		try {
			rmSync(stateDir, { recursive: true, force: true })
		} catch {
			// best effort
		}
	})

	it('findSessions returns sessions with negative windowStart', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createSessionStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			retentionMs: 60_000,
		})
		await store.init()

		await store.put({ key: 'k', windowStart: -200, windowEnd: 100 }, 1)
		await store.put({ key: 'k', windowStart: 50, windowEnd: 200 }, 2)

		const found: number[] = []
		for await (const [, value] of store.findSessions('k', -500, 500)) {
			found.push(value)
		}
		expect(found.sort((a, b) => a - b)).toEqual([1, 2])

		await store.close()
		await provider.close()
	})

	it('findSessions and remove do NOT bleed into longer keys sharing the prefix', async () => {
		// Session keys are stored as [keyBytes][biasedStart][biasedEnd] with no delimiter.
		// A naive "[keyBytes, lex-min, lex-min] to [keyBytes, lex-max, lex-max]" range scan
		// includes longer keys like "ka" when querying "k" — Codex caught this as a follow-on
		// of the F5b encoding switch. The fix: post-filter by exact key-bytes + length match.
		const provider = lmdb({ stateDir })
		const store = provider.createSessionStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			retentionMs: 60_000,
		})
		await store.init()

		await store.put({ key: 'k', windowStart: 100, windowEnd: 200 }, 1)
		await store.put({ key: 'ka', windowStart: 100, windowEnd: 200 }, 99)
		await store.put({ key: 'k2', windowStart: 100, windowEnd: 200 }, 88)

		// findSessions('k') must NOT see "ka" or "k2" sessions.
		const found: number[] = []
		for await (const [, value] of store.findSessions('k', 0, 1000)) {
			found.push(value)
		}
		expect(found).toEqual([1])

		// remove('k') must NOT delete "ka" or "k2" sessions.
		await store.remove('k')
		const remaining: number[] = []
		for await (const [, value] of store.findSessions('ka', 0, 1000)) {
			remaining.push(value)
		}
		for await (const [, value] of store.findSessions('k2', 0, 1000)) {
			remaining.push(value)
		}
		expect(remaining.sort((a, b) => a - b)).toEqual([88, 99])

		await store.close()
		await provider.close()
	})

	it('expireOldSessions correctly removes sessions ending before a positive cutoff (incl. negatives)', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createSessionStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
			retentionMs: 1_000,
		})
		await store.init()

		// Two old sessions (one with negative windowEnd) and one current.
		await store.put({ key: 'k', windowStart: -500, windowEnd: -100 }, 1)
		await store.put({ key: 'k', windowStart: -50, windowEnd: 50 }, 2)
		await store.put({ key: 'k', windowStart: 5000, windowEnd: 6000 }, 3)

		// currentTime = 1100 → cutoff = 100. Sessions 1 and 2 should expire.
		const expired = await store.expireOldSessions(1100)
		expect(expired.length).toBe(2)

		const remaining: number[] = []
		for await (const [, value] of store.findSessions('k', -10_000, 10_000)) {
			remaining.push(value)
		}
		expect(remaining).toEqual([3])

		await store.close()
		await provider.close()
	})
})
