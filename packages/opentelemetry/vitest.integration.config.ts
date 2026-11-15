import { defineConfig } from 'vitest/config'

// The test against a real producer and consumer, with the broker the client's integration
// tests start (or the one in KAFKA_BROKERS).
export default defineConfig({
	test: {
		include: ['tests/end-to-end.test.ts'],
		globalSetup: ['../client/tests/integration/kafka.global-setup.ts'],
		testTimeout: 30_000,
		hookTimeout: 60_000,
		teardownTimeout: 60_000,
		server: {
			deps: {
				external: ['testcontainers', '@testcontainers/kafka'],
			},
		},
	},
})
