import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { flowVersion } from '@/index.js'

describe('flowVersion', () => {
	it('is the version of the package', () => {
		const packageJson = JSON.parse(readFileSync(new URL('../../../package.json', import.meta.url), 'utf8')) as {
			version: string
		}
		expect(flowVersion).toBe(packageJson.version)
	})
})
