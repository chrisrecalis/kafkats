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
	},
})
