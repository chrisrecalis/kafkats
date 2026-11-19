/**
 * A doc comment documents the declaration right below it: one directly above another doc comment documents nothing,
 * and the declaration it was written for has none (what moving code between them leaves behind). Checked in the
 * sources, tests and benchmarks of every package of the repository.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join, relative } from 'node:path'
import { describe, expect, it } from 'vitest'

const PACKAGES = join(import.meta.dirname, '..', '..', '..')
const REPOSITORY = join(PACKAGES, '..')

/** The TypeScript files under `dir`, none of an installed or built package. */
function sources(dir: string): string[] {
	if (!existsSync(dir)) return []
	return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
		if (entry.name === 'node_modules' || entry.name === 'dist') return []
		const path = join(dir, entry.name)
		return entry.isDirectory() ? sources(path) : entry.name.endsWith('.ts') ? [path] : []
	})
}

/** Line numbers of the doc comments that end right above another one. */
function strandedDocComments(source: string): number[] {
	const lines = source.split('\n')
	return lines.flatMap((line, i) => (i > 0 && /^\s*\/\*\*/.test(line) && /\*\/\s*$/.test(lines[i - 1]!) ? [i] : []))
}

describe('doc comments of the sources and tests of every package', () => {
	it('each documents a declaration: none ends right above another one', () => {
		const files = readdirSync(PACKAGES).flatMap(pkg =>
			['src', 'tests', 'bench'].flatMap(dir => sources(join(PACKAGES, pkg, dir)))
		)
		// Read from the repository's packages: a wrong root would find nothing and pass.
		expect(files.some(file => file.startsWith(join(PACKAGES, 'client', 'src')))).toBe(true)
		const stranded = files.flatMap(file =>
			strandedDocComments(readFileSync(file, 'utf8')).map(line => `${relative(REPOSITORY, file)}:${line}`)
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
