import { createRoot } from 'solid-js';
import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport
} from '../../src/client/core.ts';
import { createSolidStore } from '../../src/client/solid.ts';
import { createVueStore } from '../../src/client/vue.ts';

interface NotesManager extends ManagerTypeShape {
	readonly key: 'notes-gaps';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			{ readonly limit: number },
			never,
			{ readonly items: readonly { readonly id: string }[] }
		>;
	};
}

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

const transport: RuntimeTransport = {
	async subscribe() {
		return ok(() => {});
	}
};

const config = {
	key: 'notes-gaps' as const,
	getParams: () => ({ workspaceId: 'workspace-1' }),
	getUrl: () => '/notes',
	query: () => ({ limit: 20 })
};

afterEach(() => {
	resetSyncConfiguration();
});

function configureTestRuntime(): void {
	configureSync({ cache: { adapter: cache }, transport });
}

describe('adapter lifecycle gaps', () => {
	it('subscribes Solid snapshot lazily on first read', () => {
		configureTestRuntime();
		let store!: ReturnType<typeof createSolidStore<NotesManager>>;
		createRoot((dispose) => {
			store = createSolidStore<NotesManager>(config);
			const core = store.core;
			const originalSubscribe = core.subscribe.bind(core);
			let calls = 0;
			core.subscribe = ((callback: Parameters<typeof originalSubscribe>[0]) => {
				calls += 1;
				return originalSubscribe(callback);
			}) as typeof core.subscribe;
			expect(calls).toBe(0);
			store.snapshot();
			expect(calls).toBe(1);
			dispose();
		});
		expect(store.core.snapshot().disposed).toBe(true);
	});

	it('does not auto-dispose Vue stores created outside an effect scope', () => {
		configureTestRuntime();
		const store = createVueStore<NotesManager>(config);
		expect(store.disposed.value).toBe(false);
		store.dispose();
		expect(store.disposed.value).toBe(true);
	});
});
