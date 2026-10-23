import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createRuntime,
	createStore,
	ok,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport
} from '../../src/client/core.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface NotesManager extends ManagerTypeShape {
	readonly key: 'notes';
	readonly params: { readonly name: string };
	readonly methods: {
		readonly list: MethodType<
			{ readonly limit: number },
			never,
			{ readonly items: readonly { readonly id: string }[] }
		>;
	};
}

const okList = () => Response.json({ isOk: true, isError: false, value: { items: [] } });

function createFetch(readResponse: (path: string) => Promise<Response> = async () => okList()) {
	const batches: string[][] = [];
	const fetchImpl: typeof fetch = async (input, init) => {
		const request = new Request(typeof input === 'string' ? new URL(input, 'http://sync.test') : input, init);
		const path = new URL(request.url).pathname;
		if (path === '/api/sync-resource/stream') {
			return new Response(new ReadableStream<Uint8Array>({ start() {} }), {
				headers: { 'content-type': 'text/event-stream' }
			});
		}
		if (path === '/api/sync-resource/connect') {
			const { connects = [] } = (await request.json()) as { connects?: string[] };
			batches.push(connects.map((url) => new URL(url, 'http://sync.test').pathname));
			return Response.json({
				results: connects.map((url) => ({
					status: 200,
					body: { isOk: true, isError: false, value: { scope: url } }
				}))
			});
		}
		return readResponse(path);
	};
	return { fetchImpl, batches };
}

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
});

describe('registration hold', () => {
	it('sends the registrations of stores whose reads finish at different times as one request', async () => {
		vi.useFakeTimers();
		const gates: Record<string, Deferred<Response>> = {
			'/a/list': new Deferred(),
			'/b/list': new Deferred(),
			'/c/list': new Deferred()
		};
		const { fetchImpl, batches } = createFetch((path) => gates[path]!.promise);
		configureSync({ cache: { adapter: noopCache }, fetch: fetchImpl });
		const stores = ['a', 'b', 'c'].map((name) =>
			createStore<NotesManager>({
				key: 'notes',
				params: { name },
				getUrl: () => `/${name}`,
				query: () => ({ limit: 10 })
			})
		);
		try {
			const hydrations = stores.map((store) => store.hydrate());
			gates['/a/list']!.resolve(okList());
			gates['/b/list']!.resolve(okList());
			await Promise.all(hydrations.slice(0, 2));
			await vi.advanceTimersByTimeAsync(1_900);
			expect(batches).toEqual([]);

			gates['/c/list']!.resolve(okList());
			const results = await Promise.all(hydrations);
			expect(results.every((result) => result.isOk())).toBe(true);
			await vi.advanceTimersByTimeAsync(1);
			expect(batches).toEqual([['/a/connect', '/b/connect', '/c/connect']]);
		} finally {
			for (const store of stores) {
				store.dispose();
			}
		}
	});

	it('sends held registrations after two seconds when a read never finishes', async () => {
		vi.useFakeTimers();
		const { fetchImpl, batches } = createFetch();
		const runtime = createRuntime({ cache: noopCache, fetch: fetchImpl, createId: (prefix) => `${prefix}-hold` });
		const signal = new AbortController();
		runtime.transport.holdRegistrations?.();
		const registered = runtime.transport.subscribe({
			connectUrl: '/a/connect',
			managerKey: 'notes',
			scope: 'a',
			signal: signal.signal,
			onEnvelope: () => {}
		});
		try {
			await vi.advanceTimersByTimeAsync(1_999);
			expect(batches).toEqual([]);
			await vi.advanceTimersByTimeAsync(1);
			expect((await registered).isOk()).toBe(true);
			expect(batches).toEqual([['/a/connect']]);
		} finally {
			signal.abort();
			runtime.dispose();
		}
	});
});

describe('store read registration hold', () => {
	function createStoreWithSpyTransport(respond: () => Promise<Response>) {
		const events: string[] = [];
		const fetched = new Deferred();
		const transport: RuntimeTransport = {
			async subscribe() {
				events.push('subscribe');
				return ok(() => {});
			},
			holdRegistrations() {
				events.push('hold');
				return () => {
					events.push('release');
				};
			}
		};
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () => {
				events.push('fetch');
				fetched.resolve();
				return respond();
			}
		});
		const store = createStore<NotesManager>({
			key: 'notes',
			params: { name: 'a' },
			getUrl: () => 'http://sync.test/a',
			query: () => ({ limit: 10 })
		});
		return { events, store, fetched };
	}

	it('holds registrations from the start of each read until it ends and releases once', async () => {
		const gate = new Deferred<Response>();
		let reads = 0;
		const { events, store, fetched } = createStoreWithSpyTransport(() =>
			++reads === 1 ? gate.promise : Promise.resolve(okList())
		);
		try {
			const hydrating = store.hydrate();
			await fetched.promise;
			expect(events).toEqual(['hold', 'fetch']);
			gate.resolve(okList());
			expect((await hydrating).isOk()).toBe(true);
			expect(events).toEqual(['hold', 'fetch', 'release', 'subscribe']);

			expect((await store.refresh()).isOk()).toBe(true);
			expect(events.slice(4)).toEqual(['hold', 'fetch', 'release']);
		} finally {
			store.dispose();
		}
	});

	it('releases the hold once when a read fails and does not register', async () => {
		const { events, store } = createStoreWithSpyTransport(async () => Response.json({ isOk: false }));
		try {
			expect((await store.hydrate()).isErr()).toBe(true);
			expect(events).toEqual(['hold', 'fetch', 'release']);
		} finally {
			store.dispose();
		}
	});

	it('releases the hold once when the request rejects', async () => {
		const { events, store } = createStoreWithSpyTransport(() => Promise.reject(new Error('offline')));
		try {
			expect((await store.hydrate()).isErr()).toBe(true);
			expect(events).toEqual(['hold', 'fetch', 'release']);
		} finally {
			store.dispose();
		}
	});
});
