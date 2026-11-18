import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { EngineUnderTest } from './engine.js'

const TOPIC = 'app-store-changelog'

export function checkpointSentinelSuite(engine: EngineUnderTest): void {
	// Flow invalidates a checkpoint by writing -1n (below every offset, so never trusted): the store is then wiped and
	// rebuilt instead of replayed on top of contents a cut-short rebuild or a revival left. A provider that stored it
	// as anything else (0n, say) would have such a store trusted.

	describe(`${engine.name}: changelog checkpoint sentinel`, () => {
		let stateDir: string
		beforeEach(() => {
			stateDir = mkdtempSync(join(tmpdir(), 'state-sentinel-'))
		})
		afterEach(() => {
			try {
				rmSync(stateDir, { recursive: true, force: true })
			} catch {
				// best effort
			}
		})

		it('keeps -1n apart from 0n across flush, close and reopen; a later offset replaces it', async () => {
			const provider = engine.create(stateDir)
			const checkpoints = provider.getChangelogCheckpointStore?.()
			expect(checkpoints, 'a persistent provider keeps changelog checkpoints').toBeDefined()
			await checkpoints!.set(TOPIC, 0, 7n)
			await checkpoints!.set(TOPIC, 0, -1n)
			await checkpoints!.set(TOPIC, 1, 0n)
			await checkpoints!.set(TOPIC, 2, 2n ** 62n)
			await checkpoints!.flush?.()
			expect(await checkpoints!.get(TOPIC, 0)).toBe(-1n)
			expect(await checkpoints!.get(TOPIC, 1)).toBe(0n)
			await provider.close()

			const reopened = engine.create(stateDir)
			const again = reopened.getChangelogCheckpointStore!()!
			expect(await again.get(TOPIC, 0)).toBe(-1n)
			expect(await again.get(TOPIC, 1)).toBe(0n)
			expect(await again.get(TOPIC, 2)).toBe(2n ** 62n)
			expect(await again.get(TOPIC, 3)).toBeUndefined()
			await again.set(TOPIC, 0, 5n)
			await again.flush?.()
			expect(await again.get(TOPIC, 0)).toBe(5n)
			await reopened.close()
		})
	})
}
