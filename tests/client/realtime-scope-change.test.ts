import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	err,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport,
	type SyncError,
	type SyncResult
} from '../../src/client/core.ts';

interface ScopeManager extends ManagerTypeShape {
	readonly key: 'scope-change';
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

afterEach(() => resetSyncConfiguration());

describe('realtime subscription follows the store scope', () => {
	it('unsubscribes the old scope and subscribes the new one when params change', async () => {
		const events: string[] = [];
		const transport: RuntimeTransport = {
			async subscribe(options) {
				events.push(`subscribe ${options.connectUrl}`);
				return ok(() => {
					events.push(`unsubscribe ${options.connectUrl}`);
				});
			}
		};
		let workspaceId = 'w1';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => Response.json({ isOk: true, isError: false, value: { items: [] } })
		});
		const store = createStore<ScopeManager>({
			key: 'scope-change',
			getParams: () => ({ workspaceId }),
			getUrl: () => `http://sync.test/${workspaceId}/notes`,
			query: () => ({ limit: 10 })
		});
		try {
			await store.hydrate();
			workspaceId = 'w2';
			await store.hydrate();
			await store.connect();

			expect(events).toEqual([
				'subscribe http://sync.test/w1/notes/connect',
				'unsubscribe http://sync.test/w1/notes/connect',
				'subscribe http://sync.test/w2/notes/connect'
			]);
		} finally {
			store.dispose();
		}
	});
	it('ends a failed registration of the old scope when params change', async () => {
		const signals = new Map<string, AbortSignal>();
		const transport: RuntimeTransport = {
			async subscribe(options) {
				signals.set(options.connectUrl ?? '', options.signal);
				return options.connectUrl?.includes('/w1/') ? err('internal', 'connect failed') : ok(() => {});
			}
		};
		let workspaceId = 'w1';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => Response.json({ isOk: true, isError: false, value: { items: [] } })
		});
		const store = createStore<ScopeManager>({
			key: 'scope-change',
			getParams: () => ({ workspaceId }),
			getUrl: () => `http://sync.test/${workspaceId}/notes`,
			query: () => ({ limit: 10 })
		});
		try {
			expect((await store.hydrate()).isErr()).toBe(true);
			workspaceId = 'w2';
			expect((await store.hydrate()).isOk()).toBe(true);

			expect(signals.get('http://sync.test/w1/notes/connect')?.aborted).toBe(true);
			expect(signals.get('http://sync.test/w2/notes/connect')?.aborted).toBe(false);
		} finally {
			store.dispose();
		}
	});

	it('ends an in-flight registration of the old scope when params change', async () => {
		const events: string[] = [];
		let finishW1: ((result: SyncResult<() => void, SyncError>) => void) | undefined;
		const transport: RuntimeTransport = {
			subscribe(options) {
				const url = options.connectUrl ?? '';
				events.push(`subscribe ${url}`);
				options.signal.addEventListener('abort', () => events.push(`abort ${url}`));
				const unsubscribe = () => events.push(`unsubscribe ${url}`);
				if (url.includes('/w1/')) {
					return new Promise((resolve) => {
						finishW1 = resolve;
					});
				}
				return Promise.resolve(ok(unsubscribe));
			}
		};
		let workspaceId = 'w1';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => Response.json({ isOk: true, isError: false, value: { items: [] } })
		});
		const store = createStore<ScopeManager>({
			key: 'scope-change',
			getParams: () => ({ workspaceId }),
			getUrl: () => `http://sync.test/${workspaceId}/notes`,
			query: () => ({ limit: 10 })
		});
		try {
			const first = store.hydrate();
			await vi.waitFor(() => expect(finishW1).toBeDefined());
			workspaceId = 'w2';
			await store.connect();
			finishW1?.(ok(() => events.push('unsubscribe http://sync.test/w1/notes/connect')));
			await first;

			expect(events).toEqual([
				'subscribe http://sync.test/w1/notes/connect',
				'abort http://sync.test/w1/notes/connect',
				'subscribe http://sync.test/w2/notes/connect',
				'unsubscribe http://sync.test/w1/notes/connect'
			]);
		} finally {
			store.dispose();
		}
	});
});
