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

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
});

function pathOf(input: URL | RequestInfo): string {
	return typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
}

function createFailingConnectRuntime(failures: number) {
	let connects = 0;
	const runtime = createRuntime({
		batchConnect: false,
		cache,
		fetch: async (input) => {
			if (pathOf(input).endsWith('/stream')) {
				return new Response(new ReadableStream<Uint8Array>({ start() {} }), {
					headers: { 'content-type': 'text/event-stream' }
				});
			}
			connects += 1;
			return connects <= failures
				? new Response('unavailable', { status: 503 })
				: Response.json({ isOk: true, isError: false, value: { scope: 'notes:workspace-1' } });
		},
		streamUrl: '/stream',
		createId: (prefix) => `${prefix}-retry`
	});
	const subscribe = (signal: AbortSignal) =>
		runtime.transport.subscribe({
			url: '/notes/events',
			connectUrl: '/notes/connect',
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			signal,
			onEnvelope: () => {}
		});
	return { runtime, subscribe, connects: () => connects };
}

describe('failed realtime registration', () => {
	it('keeps the subscription, retries with backoff and stops retrying once registered', async () => {
		vi.useFakeTimers();
		const { runtime, subscribe, connects } = createFailingConnectRuntime(2);
		const signal = new AbortController();
		try {
			const first = await subscribe(signal.signal);
			expect(first.isErr()).toBe(true);
			expect(connects()).toBe(1);

			await vi.advanceTimersByTimeAsync(1300);
			expect(connects()).toBe(2);
			await vi.advanceTimersByTimeAsync(2600);
			expect(connects()).toBe(3);
			await vi.advanceTimersByTimeAsync(60_000);
			expect(connects()).toBe(3);
		} finally {
			signal.abort();
			runtime.dispose();
		}
	});

	it('stops retrying when the last subscriber leaves', async () => {
		vi.useFakeTimers();
		const { runtime, subscribe, connects } = createFailingConnectRuntime(Infinity);
		const signal = new AbortController();
		try {
			expect((await subscribe(signal.signal)).isErr()).toBe(true);
			signal.abort();
			await vi.advanceTimersByTimeAsync(60_000);
			expect(connects()).toBe(1);
		} finally {
			runtime.dispose();
		}
	});

	it('still reports the connect error from hydrate and registers on the retry', async () => {
		vi.useFakeTimers();
		let connects = 0;
		configureSync({
			cache: { adapter: cache },
			fetch: async (input) => {
				const path = pathOf(input);
				if (path.endsWith('/stream')) {
					return new Response(new ReadableStream<Uint8Array>({ start() {} }), {
						headers: { 'content-type': 'text/event-stream' }
					});
				}
				if (path.endsWith('/connect')) {
					connects += 1;
					return connects === 1
						? new Response('unavailable', { status: 503 })
						: Response.json({ isOk: true, isError: false, value: { scope: 'notes:workspace-1' } });
				}
				return Response.json({ isOk: true, isError: false, value: { items: [] } });
			},
			streamUrl: '/stream'
		});
		const store = createStore<NotesManager>({
			key: 'notes',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ limit: 10 })
		});
		try {
			const hydrated = await store.hydrate();
			expect(hydrated.isErr()).toBe(true);
			await vi.advanceTimersByTimeAsync(1300);
			expect(connects).toBe(2);
		} finally {
			store.dispose();
		}
	});
});
