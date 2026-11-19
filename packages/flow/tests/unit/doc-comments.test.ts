/**
 * A doc comment documents the declaration right below it: one directly above another doc comment documents nothing,
 * and the declaration it was written for has none (what moving code between them leaves behind).
 */
import { readdirSync, readFileSync } from 'node:fs'
import { join, relative } from 'node:path'
import { describe, expect, it } from 'vitest'

const ROOT = join(import.meta.dirname, '..', '..')

function sources(dir: string): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap(entry =>
		entry.isDirectory() ? sources(join(dir, entry.name)) : entry.name.endsWith('.ts') ? [join(dir, entry.name)] : []
	)
}

/** Line numbers of the doc comments that end right above another one. */
function strandedDocComments(source: string): number[] {
	const lines = source.split('\n')
	return lines.flatMap((line, i) => (i > 0 && /^\s*\/\*\*/.test(line) && /\*\/\s*$/.test(lines[i - 1]!) ? [i] : []))
}

describe('doc comments of the sources and tests', () => {
	it('each documents a declaration: none ends right above another one', () => {
		const stranded = ['src', 'tests', 'bench'].flatMap(dir =>
			sources(join(ROOT, dir)).flatMap(file =>
				strandedDocComments(readFileSync(file, 'utf8')).map(line => `${relative(ROOT, file)}:${line}`)
			)
		)
		expect(stranded).toEqual([])
	})

	it('finds a doc comment left above the doc comment of another declaration', () => {
		const moved = [
			'/** An unlogged in-memory key-value store of strings. */',
			'/** The start of a partition committed at `offset`. */',
			'export function committed() {}',
			'',
			'/**',
			' * A store.',
			' */',
			'export function kvStore() {}',
		].join('\n')
		expect(strandedDocComments(moved)).toEqual([1])
	})
})
