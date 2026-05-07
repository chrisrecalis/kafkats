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

describe('LMDBKeyValueStore.range', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'lmdb-test-'))
	})
	afterEach(() => {
		try {
			rmSync(stateDir, { recursive: true, force: true })
		} catch {
			// best effort
		}
	})

	it('range(from, to) is inclusive on the upper bound (matching contract)', async () => {
		const provider = lmdb({ stateDir })
		const store = provider.createKeyValueStore<string, number>('s', {
			keyCodec: stringCodec,
			valueCodec: numberCodec,
		})
		await store.init()

		await store.put('a', 1)
		await store.put('b', 2)
		await store.put('c', 3)
		await store.put('d', 4)

		const collected: Array<[string, number]> = []
		for await (const entry of store.range('b', 'c')) {
			collected.push(entry)
		}

		// Inclusive on both ends: must contain both 'b' and 'c'.
		expect(collected.map(([k]) => k).sort()).toEqual(['b', 'c'])

		await store.close()
		await provider.close()
	})
})
