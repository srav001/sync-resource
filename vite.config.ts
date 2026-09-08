import { defineConfig } from 'vite-plus';

export default defineConfig({
	pack: {
		entry: {
			'client/core': './src/client/core.ts',
			'client/react': './src/client/react.ts',
			'client/solid': './src/client/solid.ts',
			'client/vue': './src/client/vue.ts',
			server: './src/server/index.ts',
			'server/effect': './src/server/effect.ts',
			shared: './src/shared/index.ts'
		},
		format: 'esm',
		platform: 'neutral',
		target: 'es2022',
		dts: true,
		sourcemap: true
	},
	staged: {
		'*': 'vp check --fix'
	},
	fmt: {
		semi: true,
		tabWidth: 4,
		useTabs: true,
		printWidth: 120,
		singleQuote: true,
		bracketSameLine: true,
		trailingComma: 'none',
		ignorePatterns: ['.tmp', 'dist', '.github/**', 'tools/oxlint/**'],
		sortPackageJson: false,
		sortImports: {}
	},
	lint: {
		jsPlugins: [
			{ name: 'anti-slop', specifier: './tools/oxlint/index.ts' },
			{ name: 'anti-slop-effect', specifier: './tools/oxlint/effect.ts' }
		],
		options: {
			typeAware: true,
			typeCheck: true
		},
		ignorePatterns: ['dist/**', '.github/**', 'tools/oxlint/**'],
		overrides: [
			{
				files: ['src/**'],
				rules: {
					'anti-slop/no-chained-type-assertions': 'error',
					'anti-slop/no-conditional-empty-object-spread': 'error',
					'anti-slop/no-known-value-widening': 'off',
					'anti-slop/no-module-mocking': 'error',
					'anti-slop/no-object-parameters': 'error',
					'anti-slop/no-reflect-apply': 'error',
					'anti-slop/no-reflect-get': 'error',
					'anti-slop/no-runtime-typeof': ['error', { allowInTypeGuards: true }],
					'anti-slop/no-shape-in-symbol-names': 'error',
					'anti-slop/no-unknown-parameters': 'error',
					'anti-slop/no-unknown-returns': 'off',
					'anti-slop/no-unknown-type-aliases': 'error',
					'anti-slop/no-unsafe-dictionary-type': 'error',
					'anti-slop/no-widen-then-assert': 'error',
					'anti-slop/require-safety-comment-for-type-assertion': 'off',
					'anti-slop-effect/no-service-constructor-imports': 'error'
				}
			}
		],
		rules: {
			'oxc/no-barrel-file': 'error',
			'no-unassigned-vars': 'off',
			'unicorn/no-new-array': 'off',
			'typescript/no-explicit-any': 'error',
			'typescript/no-floating-promises': 'off',
			'eslint/curly': 'error'
		}
	},
	test: {
		coverage: {
			include: ['src/**/*.ts'],
			reporter: ['text', 'json'],
			thresholds: {
				branches: 70,
				functions: 85,
				lines: 80,
				statements: 80
			}
		}
	},
	run: {
		cache: true
	}
});
