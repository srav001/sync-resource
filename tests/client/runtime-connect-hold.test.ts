import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createRuntime,
	createStore,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType
} from '../../src/client/core.ts';

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

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

function createFetch(readDelayMs: (path: string) => number = () => 0) {
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
			const { connects } = (await request.json()) as { connects: string[] };
			batches.push(connects);
			return Response.json({
				results: connects.map((url) => ({
					status: 200,
					body: { isOk: true, isError: false, value: { scope: url } }
				}))
			});
		}
		await new Promise((resolve) => setTimeout(resolve, readDelayMs(path)));
		return Response.json({ isOk: true, isError: false, value: { items: [] } });
	};
	return { fetchImpl, batches };
}

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
});

describe('registration hold', () => {
	it('sends the registrations of stores whose reads finish at different times as one request', async () => {
		const { fetchImpl, batches } = createFetch(
			(path) => ({ '/a/list': 5, '/b/list': 40, '/c/list': 90 })[path] ?? 0
		);
		configureSync({ cache: { adapter: cache }, fetch: fetchImpl });
		const stores = ['a', 'b', 'c'].map((name) =>
			createStore<NotesManager>({
				key: 'notes',
				getParams: () => ({ name }),
				getUrl: () => `/${name}`,
				query: () => ({ limit: 10 })
			})
		);
		try {
			const results = await Promise.all(stores.map((store) => store.hydrate()));
			expect(results.every((result) => result.isOk())).toBe(true);
			await vi.waitFor(() => expect(batches).toHaveLength(1));
			await new Promise((resolve) => setTimeout(resolve, 20));
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
		const runtime = createRuntime({ cache, fetch: fetchImpl, createId: (prefix) => `${prefix}-hold` });
		const signal = new AbortController();
		runtime.transport.holdRegistrations?.();
		const registered = runtime.transport.subscribe({
			url: '/a/events',
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
