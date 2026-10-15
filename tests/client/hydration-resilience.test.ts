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
import { noopCache, noopTransport } from '../fixtures/clientRuntime.ts';
import { syncResponse } from '../fixtures/responses.ts';
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

afterEach(() => {
	resetSyncConfiguration();
});

function createHydrationStore(): ReturnType<typeof createStore<HydrationManager>> {
	return createStore<HydrationManager>({
		key: 'hydration-resilience',
		params: { workspaceId: 'workspace-1' },
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
			transport: noopTransport
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
			cache: { adapter: noopCache },
			transport: noopTransport,
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

	it('resolves hydrate on the read, reports a connect failure as the store error and only resubscribes on the next hydrate', async () => {
		let reads = 0;
		let subscriptions = 0;
		const transport: RuntimeTransport = {
			async subscribe() {
				subscriptions += 1;
				return subscriptions === 1 ? err('internal', 'transport unavailable') : ok(() => {});
			}
		};
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () => {
				reads += 1;
				return syncResponse({ items: [{ id: 'note-1', title: `Read ${reads}` }] });
			}
		});
		const store = createHydrationStore();
		const registrationFailed = new Deferred();
		store.on.error((error) => error && registrationFailed.resolve());

		expect((await store.hydrate()).isOk()).toBe(true);
		await registrationFailed.promise;
		expect(store.error()?.message).toBe('transport unavailable');
		expect((await store.hydrate()).isOk()).toBe(true);
		expect({ reads, subscriptions }).toEqual({ reads: 1, subscriptions: 2 });
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Read 1' }]);
		store.dispose();
	});

	it('resolves hydrate once the read is applied while the realtime registration is still pending', async () => {
		let subscriptions = 0;
		configureSync({
			cache: { adapter: noopCache },
			transport: {
				subscribe() {
					subscriptions += 1;
					return new Promise(() => {});
				}
			},
			fetch: async () => syncResponse({ items: [{ id: 'note-1', title: 'Read 1' }] })
		});
		const store = createHydrationStore();

		expect((await store.hydrate()).isOk()).toBe(true);
		expect(subscriptions).toBe(1);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Read 1' }]);
		store.dispose();
	});

	it('returns disposed when disposal interrupts an authoritative read and never connects', async () => {
		const response = new Deferred<Response>();
		const fetchStarted = new Deferred();
		let subscriptions = 0;
		configureSync({
			cache: { adapter: noopCache },
			transport: {
				async subscribe() {
					subscriptions += 1;
					return ok(() => {});
				}
			},
			fetch: async () => {
				fetchStarted.resolve();
				return response.promise;
			}
		});
		const store = createHydrationStore();
		const hydration = store.hydrate();
		await fetchStarted.promise;
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
			transport: noopTransport,
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
		expect((await store.restore()).isOk()).toBe(true);
		expect(store.pending()).toHaveLength(1);
		expect(store.items()).toEqual([{ id: 'optimistic', title: 'Local' }]);

		writeResponse.resolve(syncResponse({ id: 'optimistic', title: 'Local' }));
		expect((await write).isOk()).toBe(true);
		store.dispose();
	});
});
