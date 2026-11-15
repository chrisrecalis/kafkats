import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { numberCodec, stringCodec } from './codecs.js'
import type { EngineUnderTest } from './engine.js'

export function rangeSuite(engine: EngineUnderTest): void {
	describe(`${engine.name}: KeyValueStore.range`, () => {
		let stateDir: string
		beforeEach(() => {
			stateDir = mkdtempSync(join(tmpdir(), 'state-test-'))
		})
		afterEach(() => {
			try {
				rmSync(stateDir, { recursive: true, force: true })
			} catch {
				// best effort
			}
		})

		it('range(from, to) is inclusive on the upper bound (matching contract)', async () => {
			const provider = engine.create(stateDir)
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
}
