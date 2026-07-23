import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	err,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type CacheItem,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport
} from '../../src/client/core.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface HydrationManager extends ManagerTypeShape {
	readonly key: 'hydration-resilience';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			{ readonly limit: number },
			never,
			{ readonly items: readonly { readonly id: string; readonly title: string }[] }
		>;
		readonly add: MethodType<
			never,
			{ readonly id: string; readonly title: string },
			{ readonly id: string; readonly title: string }
		>;
	};
}

const emptyCache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

afterEach(() => {
	resetSyncConfiguration();
});

function createHydrationStore(): ReturnType<typeof createStore<HydrationManager>> {
	return createStore<HydrationManager>({
		key: 'hydration-resilience',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => ({ limit: 20 })
	});
}

describe('hydration resilience', () => {
	it('returns and records a cache read failure without replacing in-memory state', async () => {
		const cache: CacheAdapter = {
			async get() {
				throw new Error('cache read failed');
			},
			async set() {},
			async del() {}
		};
		configureSync({
			cache: { adapter: cache },
			transport: successfulTransport()
		});
		const store = createHydrationStore();

		const result = await store.restore();
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('internal');
			expect(result.error.message).toBe('cache read failed');
		}
		expect(store.error()?.message).toBe('cache read failed');
		expect(store.items()).toEqual([]);
		store.dispose();
	});

	it('retries a failed authoritative read instead of caching the failed hydration', async () => {
		let requests = 0;
		configureSync({
			cache: { adapter: emptyCache },
			transport: successfulTransport(),
			fetch: async () => {
				requests += 1;
				if (requests === 1) {
					throw new Error('temporary read failure');
				}
				return syncResponse({ items: [{ id: 'note-1', title: 'Recovered' }] });
			}
		});
		const store = createHydrationStore();

		const failed = await store.hydrate();
		expect(failed.isErr()).toBe(true);
		expect(store.isHydrating()).toBe(false);

		const retried = await store.hydrate();
		expect(retried.isOk()).toBe(true);
		expect(requests).toBe(2);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Recovered' }]);
		store.dispose();
	});

	it('retries both the read and realtime subscription after connect failure', async () => {
		let reads = 0;
		let subscriptions = 0;
		const transport: RuntimeTransport = {
			async subscribe() {
				subscriptions += 1;
				return subscriptions === 1 ? err('internal', 'transport unavailable') : ok(() => {});
			}
		};
		configureSync({
			cache: { adapter: emptyCache },
			transport,
			fetch: async () => {
				reads += 1;
				return syncResponse({ items: [{ id: 'note-1', title: `Read ${reads}` }] });
			}
		});
		const store = createHydrationStore();

		expect((await store.hydrate()).isErr()).toBe(true);
		expect((await store.hydrate()).isOk()).toBe(true);
		expect({ reads, subscriptions }).toEqual({ reads: 2, subscriptions: 2 });
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Read 2' }]);
		store.dispose();
	});

	it('returns disposed when disposal interrupts an authoritative read and never connects', async () => {
		const response = new Deferred<Response>();
		let fetchStarted = false;
		let subscriptions = 0;
		configureSync({
			cache: { adapter: emptyCache },
			transport: {
				async subscribe() {
					subscriptions += 1;
					return ok(() => {});
				}
			},
			fetch: async () => {
				fetchStarted = true;
				return response.promise;
			}
		});
		const store = createHydrationStore();
		const hydration = store.hydrate();
		await waitFor(() => fetchStarted);
		store.dispose();
		response.resolve(syncResponse({ items: [] }));

		const result = await hydration;
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('disposed');
		}
		expect(subscriptions).toBe(0);
	});

	it('does not replace an active optimistic command with a late cache restore', async () => {
		const cacheRead = new Deferred<CacheItem<unknown> | undefined>();
		const writeResponse = new Deferred<Response>();
		const cache: CacheAdapter = {
			async get() {
				return cacheRead.promise as Promise<CacheItem<never> | undefined>;
			},
			async set() {},
			async del() {}
		};
		configureSync({
			cache: { adapter: cache },
			transport: successfulTransport(),
			createId: (prefix) => `${prefix}-pending`,
			fetch: async (_input, init) => {
				if (init?.method !== 'POST') {
					return syncResponse({ items: [] });
				}
				return writeResponse.promise;
			}
		});
		const store = createHydrationStore();
		const write = store.add({ input: { id: 'optimistic', title: 'Local' } });
		expect(store.pending()).toHaveLength(1);
		expect(store.items()).toEqual([{ id: 'optimistic', title: 'Local' }]);

		cacheRead.resolve({
			value: {
				items: [{ id: 'cached', title: 'Old cache' }],
				baseItems: [{ id: 'cached', title: 'Old cache' }],
				pages: [],
				families: []
			},
			expiry: Date.now() + 10_000
		});
		await waitFor(() => store.pending().length === 1);
		expect(store.items()).toEqual([{ id: 'optimistic', title: 'Local' }]);

		writeResponse.resolve(syncResponse({ id: 'optimistic', title: 'Local' }));
		expect((await write).isOk()).toBe(true);
		store.dispose();
	});
});

function successfulTransport(): RuntimeTransport {
	return {
		async subscribe() {
			return ok(() => {});
		}
	};
}

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}

async function waitFor(condition: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await Promise.resolve();
	}
	throw new Error('Condition was not reached.');
}
