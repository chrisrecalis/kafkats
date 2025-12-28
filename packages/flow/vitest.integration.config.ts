import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'

export default defineConfig({
	resolve: {
		alias: {
			'@': fileURLToPath(new URL('./src', import.meta.url)),
		},
	},
	test: {
		include: ['tests/integration/**/*.test.ts'],
		globalSetup: ['tests/integration/kafka.global-setup.ts'],
		pool: 'threads',
		maxWorkers: 1,
		fileParallelism: false,
		testTimeout: 15_000,
		hookTimeout: 60_000,
		teardownTimeout: 60_000,
		deps: {
			optimizer: {
				ssr: {
					enabled: false,
				},
			},
		},
	},
})
