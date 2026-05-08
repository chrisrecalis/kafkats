import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'

export default defineConfig({
	resolve: {
		alias: {
			'@': fileURLToPath(new URL('./src', import.meta.url)),
		},
	},
	test: {
		deps: {
			// lmdb's native binding fails under Vite's SSR optimizer; load it directly.
			optimizer: {
				ssr: {
					enabled: false,
				},
			},
		},
	},
})
