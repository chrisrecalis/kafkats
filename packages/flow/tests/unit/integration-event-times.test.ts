/**
 * The integration and chaos tests stamp records near the wall clock (`eventTimeBase()`): the broker's time-based
 * retention (7 days by default, checked every 5 minutes, the active segment included) deletes records stamped far in
 * the past while a test still reads them back, a failure that depends on when the check runs.
 */
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

const RETENTION_MS = 7 * 24 * 3_600_000
/** A 13-digit number of milliseconds since the epoch (2001 to 2286), digit separators allowed. */
const EPOCH_MS = /\b1(?:_?\d){12}\b/g

function sources(dir: string): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap(entry =>
		entry.isDirectory() ? sources(join(dir, entry.name)) : entry.name.endsWith('.ts') ? [join(dir, entry.name)] : []
	)
}

describe('integration and chaos test sources', () => {
	it('stamp no record before the broker retention: no fixed epoch base', () => {
		const stale: string[] = []
		for (const dir of ['tests/integration', 'tests/chaos']) {
			for (const file of sources(join(import.meta.dirname, '..', '..', dir))) {
				for (const [literal] of readFileSync(file, 'utf8').matchAll(EPOCH_MS)) {
					if (Number(literal.replaceAll('_', '')) < Date.now() - RETENTION_MS)
						stale.push(`${file}: ${literal}`)
				}
			}
		}
		expect(stale).toEqual([])
	})
})
