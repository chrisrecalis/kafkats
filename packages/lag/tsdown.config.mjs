import { defineConfig } from 'tsdown'

export default defineConfig({
	entry: ['src/cli.ts'],
	format: ['esm'],
	platform: 'node',
	target: 'es2022',
	dts: false,
	fixedExtension: false,
	sourcemap: true,
	deps: {
		neverBundle: [/^@kafkats\//, /^prom-client$/],
	},
})
