import { defineConfig } from 'tsdown'

export default defineConfig({
	entry: ['src/index.ts', 'src/testing.ts'],
	format: ['esm', 'cjs'],
	platform: 'node',
	target: 'es2022',
	external: ['vitest'],
	dts: true,
	fixedExtension: false,
	sourcemap: true,
})
