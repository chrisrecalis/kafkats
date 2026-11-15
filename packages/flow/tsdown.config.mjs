import { defineConfig } from 'tsdown'

export default defineConfig({
	entry: { index: 'src/index.ts', testing: 'src/testing.ts', state: 'src/state-kit.ts' },
	format: ['esm', 'cjs'],
	platform: 'node',
	target: 'es2022',
	external: ['vitest'],
	dts: true,
	fixedExtension: false,
	sourcemap: true,
})
