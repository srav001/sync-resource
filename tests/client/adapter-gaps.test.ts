import { createRoot } from 'solid-js';
import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType
} from '../../src/client/core.ts';
import { createSolidStore } from '../../src/client/solid.ts';
import { createVueStore } from '../../src/client/vue.ts';
import { noopCache, noopTransport } from '../fixtures/clientRuntime.ts';

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

const config = {
	key: 'notes-gaps' as const,
	params: { workspaceId: 'workspace-1' },
	getUrl: () => '/notes',
	query: () => ({ limit: 20 })
};

afterEach(() => {
	vi.restoreAllMocks();
	resetSyncConfiguration();
});

function configureTestRuntime(): void {
	configureSync({ cache: { adapter: noopCache }, transport: noopTransport });
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
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const store = createVueStore<NotesManager>(config);
		expect(warn).not.toHaveBeenCalled();
		expect(store.disposed.value).toBe(false);
		store.dispose();
		expect(store.disposed.value).toBe(true);
	});
});
