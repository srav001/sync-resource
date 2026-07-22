import { defineConfig } from 'vite-plus';

export default defineConfig({
	pack: {
		entry: {
			'client/core': './src/client/core.ts',
			server: './src/server/index.ts',
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
			'typescript/no-explicit-any': 'error',
			'typescript/no-floating-promises': 'warn',
			'eslint/curly': 'error'
		}
	},
	run: {
		cache: true
	}
});
