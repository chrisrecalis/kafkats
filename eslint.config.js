import js from '@eslint/js'
import globals from 'globals'
import tseslint from 'typescript-eslint'
import prettierRecommended from 'eslint-plugin-prettier/recommended'
import { defineConfig } from 'eslint/config'

export default defineConfig([
	// Base JS/TS rules
	{
		extends: [js.configs.recommended],
		languageOptions: {
			globals: {
				...globals.node,
				...globals.es2021,
			},
		},
	},

	// TypeScript rules (type-aware)
	{
		extends: [...tseslint.configs.recommendedTypeChecked],
		files: ['packages/*/src/**/*.ts'],
		languageOptions: {
			parserOptions: {
				project: ['packages/*/tsconfig.json'],
				tsconfigRootDir: import.meta.dirname,
			},
		},
		settings: {
			'import/resolver': {
				typescript: {
					project: ['packages/*/tsconfig.json'],
				},
			},
		},
		rules: {
			'@typescript-eslint/no-floating-promises': 'off',
		},
	},

	// Tests: use non-type-aware rules while still honoring globals
	{
		extends: [...tseslint.configs.recommended],
		files: ['packages/*/tests/**/*.ts'],
		languageOptions: {
			globals: {
				...globals.vitest,
			},
		},
		rules: {
			// Allow underscore-prefixed unused vars in tests (common pattern for intentional ignores)
			'@typescript-eslint/no-unused-vars': [
				'error',
				{
					argsIgnorePattern: '^_',
					varsIgnorePattern: '^_',
				},
			],
		},
	},

	// Prettier integration: run prettier as an ESLint rule and disable conflicting rules
	prettierRecommended,

	// Repo-wide ignores
	{
		ignores: ['**/dist/**', '**/node_modules/**', '**/*.tsbuildinfo'],
	},
])
