/**
 * The integration and chaos tests stamp records near the wall clock (`eventTimeBase()`): the broker's time-based
 * retention (7 days by default, checked every 5 minutes, the active segment included) deletes records stamped far in
 * the past while a test still reads them back, a failure that depends on when the check runs.
 *
 * A fixed stamp is a literal point in time: an epoch number of milliseconds, or a `Date` of a literal (records are
 * stamped with a `Date`: `new Date(10_000)` is in 1970). A number that only becomes a stamp further on (a field later
 * passed to `new Date()`) cannot be told apart from other numbers here; stamp such fields from a base too.
 */
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

const RETENTION_MS = 7 * 24 * 3_600_000
/** A 13-digit number of milliseconds since the epoch (2001 to 2286), digit separators and a bigint suffix allowed. */
const EPOCH_MS = /\b1(?:_?\d){12}n?\b/g
/** A Date of a literal: milliseconds, a date string, or `Date.UTC()` of numbers. */
const LITERAL_DATE = /new Date\(\s*(\d[\d_]*|'[^']*'|"[^"]*")\s*\)|Date\.UTC\(([\d_,\s]*)\)/g

const millis = (literal: string): number => Number(literal.replaceAll('_', '').replace(/n$/, ''))

/** The fixed stamps of `source` before the broker retention, as written. */
function staleStamps(source: string, now = Date.now()): string[] {
	const stale = new Set<string>()
	for (const [literal] of source.matchAll(EPOCH_MS)) {
		if (millis(literal) < now - RETENTION_MS) stale.add(literal)
	}
	for (const [literal, argument, utc] of source.matchAll(LITERAL_DATE)) {
		const time =
			utc !== undefined
				? Date.UTC(...(utc.split(',').map(millis) as [number, number]))
				: /^\d/.test(argument!)
					? millis(argument!)
					: Date.parse(argument!.slice(1, -1))
		if (!(time >= now - RETENTION_MS)) stale.add(literal)
	}
	return [...stale]
}

function sources(dir: string): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap(entry =>
		entry.isDirectory() ? sources(join(dir, entry.name)) : entry.name.endsWith('.ts') ? [join(dir, entry.name)] : []
	)
}

describe('integration and chaos test sources', () => {
	it('stamp no record before the broker retention: no fixed epoch base, no Date of a literal', () => {
		const stale: string[] = []
		for (const dir of ['tests/integration', 'tests/chaos']) {
			for (const file of sources(join(import.meta.dirname, '..', '..', dir))) {
				for (const literal of staleStamps(readFileSync(file, 'utf8'))) stale.push(`${file}: ${literal}`)
			}
		}
		expect(stale).toEqual([])
	})

	it('a fixed stamp is an old epoch number (also a bigint) or a Date of a literal, 1970 included', () => {
		const now = Date.UTC(2026, 10, 19)
		expect(
			staleStamps(
				[
					"producer.send(t, { key: 'a', value, timestamp: new Date(10_000) })",
					'const BASE = 1_700_000_000_000n',
					"timestamp: new Date('2023-11-14T22:13:20Z')",
					'timestamp: new Date(Date.UTC(2023, 0, 1))',
				].join('\n'),
				now
			)
		).toEqual([
			'1_700_000_000_000n',
			'new Date(10_000)',
			"new Date('2023-11-14T22:13:20Z')",
			'Date.UTC(2023, 0, 1)',
		])
		// Stamps from a base near the wall clock, and recent literals, are fine; so is a stored timestamp of 0.
		expect(
			staleStamps(
				[
					'timestamp: new Date(base + 30_000)',
					'new Date(Date.now() - 5_000)',
					'const RECENT = 1_795_000_000_000',
					"new Date('2026-11-18')",
					'expect(stored).toEqual({ value: 1, timestamp: 0 })',
				].join('\n'),
				now
			)
		).toEqual([])
	})
})
