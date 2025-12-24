import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'
import os from 'node:os'

const maxWorkersFromEnv = Number(process.env.VITEST_MAX_WORKERS)
const cpuCount = os.cpus().length
const defaultWorkers = Math.min(8, Math.max(2, cpuCount))
const maxWorkers = Number.isFinite(maxWorkersFromEnv) && maxWorkersFromEnv > 0 ? maxWorkersFromEnv : defaultWorkers

export default defineConfig({
	resolve: {
		alias: {
			'@': fileURLToPath(new URL('./src', import.meta.url)),
		},
	},
	test: {
		maxWorkers,
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
