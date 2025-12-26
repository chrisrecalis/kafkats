import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'

export default defineConfig({
	resolve: {
		alias: {
			'@': fileURLToPath(new URL('./src', import.meta.url)),
		},
	},
	test: {
		globalSetup: ['./tests/integration/helpers/global-setup.ts'],
		testTimeout: 60_000,
		hookTimeout: 120_000,
		// Stop on first failure and set max 5 minute total runtime
		bail: 1,
		globalTimeout: 300_000, // 5 minutes max for entire test suite
	},
})
