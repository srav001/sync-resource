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
		ignorePatterns: ['.tmp', 'dist', '.github/**'],
		sortPackageJson: false,
		sortImports: {}
	},
	lint: {
		options: {
			typeAware: true,
			typeCheck: true
		},
		ignorePatterns: ['dist/**', '.github/**'],
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
