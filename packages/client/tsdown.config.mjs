import { defineConfig } from 'tsdown'

export default defineConfig({
	entry: ['src/index.ts'],
	format: ['esm', 'cjs'],
	platform: 'node',
	target: 'es2022',
	dts: true,
	fixedExtension: false,
	sourcemap: true,
})
