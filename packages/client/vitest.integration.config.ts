import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'

export default defineConfig({
	resolve: {
		alias: {
			'@': fileURLToPath(new URL('./src', import.meta.url)),
		},
	},
	test: {
		// Integration tests share Kafka containers across files to avoid slow repeated startups.
		maxWorkers: 1,
		isolate: false,
		testTimeout: 60_000,
		hookTimeout: 120_000,
		deps: {
			optimizer: {
				ssr: {
					enabled: false,
				},
			},
		},
		server: {
			deps: {
				external: ['testcontainers', '@testcontainers/kafka'],
			},
		},
	},
})
