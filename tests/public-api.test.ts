import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, expectTypeOf, it } from 'vite-plus/test';

import * as clientCore from '../src/client/core.ts';
import type { ManagerTypeShape, MethodType } from '../src/client/core.ts';
// @ts-expect-error Runtime adapter construction types remain internal.
import type { createStoreRuntime, StoreRuntimeApi } from '../src/client/core.ts';
import * as plainServer from '../src/server/index.ts';
import type { OperationMeta } from '../src/server/index.ts';
// @ts-expect-error The shared-stream reset is source-level test infrastructure, not public API.
import type { resetSharedStreamForTests } from '../src/server/index.ts';

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));

interface CompatibleManager extends ManagerTypeShape {
	readonly key: 'compatible';
	readonly params: { readonly scope: string };
	readonly methods: { readonly get: MethodType<never, never, string> };
}

const meta: OperationMeta = {
	string: 'value',
	number: 1,
	boolean: true,
	null: null,
	object: { nested: true },
	array: [1, 'two']
};

describe('public package boundaries', () => {
	it('keeps canonical compatibility types while internal helpers stay private', () => {
		const manager: CompatibleManager = {
			key: 'compatible',
			params: { scope: 'one' },
			methods: { get: undefined as never }
		};
		expect(manager.key).toBe('compatible');
		expect(meta).toMatchObject({ string: 'value', number: 1, boolean: true });
		expect('createStoreRuntime' in clientCore).toBe(false);
		expect('resetSharedStreamForTests' in plainServer).toBe(false);
		expectTypeOf<StoreRuntimeApi<CompatibleManager>>().not.toBeNever();
		expectTypeOf<typeof createStoreRuntime>().not.toBeNever();
		expectTypeOf<typeof resetSharedStreamForTests>().not.toBeNever();
	});

	it('keeps Effect isolated to the explicit server/effect source entrypoint', () => {
		const sourceRoot = join(repositoryRoot, 'src');
		const effectImporters = typescriptFiles(sourceRoot)
			.filter((file) => /from ['"]effect(?:\/[^'"]+)?['"]/.test(readFileSync(file, 'utf8')))
			.map((file) => relative(sourceRoot, file));
		expect(effectImporters).toEqual(['server/effect.ts']);

		const packageJson = JSON.parse(readFileSync(join(repositoryRoot, 'package.json'), 'utf8')) as {
			readonly dependencies?: unknown;
			readonly peerDependencies: Record<string, string>;
			readonly peerDependenciesMeta: Record<string, { readonly optional?: boolean }>;
		};
		expect(packageJson.dependencies).toBeUndefined();
		expect(packageJson.peerDependencies.effect).toBe('>=4.0.0-rc.112 <5');
		expect(packageJson.peerDependenciesMeta.effect?.optional).toBe(true);
	});
});

function typescriptFiles(directory: string): string[] {
	return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
		const path = join(directory, entry.name);
		return entry.isDirectory() ? typescriptFiles(path) : entry.name.endsWith('.ts') ? [path] : [];
	});
}
