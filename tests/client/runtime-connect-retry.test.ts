import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createRuntime,
	createStore,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType
} from '../../src/client/core.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';

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

afterEach(() => {
	vi.useRealTimers();
	vi.restoreAllMocks();
	resetSyncConfiguration();
});

function pathOf(input: URL | RequestInfo): string {
	return typeof input === 'string' ? input : input instanceof URL ? input.pathname : input.url;
}

function createFailingConnectRuntime(fails: (connect: number) => boolean) {
	let connects = 0;
	let streamController!: ReadableStreamDefaultController<Uint8Array>;
	const runtime = createRuntime({
		batchConnect: false,
		cache: noopCache,
		fetch: async (input) => {
			if (pathOf(input).endsWith('/stream')) {
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							streamController = controller;
						}
					}),
					{ headers: { 'content-type': 'text/event-stream' } }
				);
			}
			connects += 1;
			return fails(connects)
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
	return { runtime, subscribe, connects: () => connects, closeStream: () => streamController.close() };
}

describe('failed realtime registration', () => {
	it('keeps the subscription, retries with backoff and stops retrying once registered', async () => {
		vi.useFakeTimers();
		const { runtime, subscribe, connects } = createFailingConnectRuntime((connect) => connect <= 2);
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

	it('restarts the backoff from its base delay once a registration has succeeded', async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, 'random').mockReturnValue(0.5);
		const { runtime, subscribe, connects, closeStream } = createFailingConnectRuntime((connect) =>
			[1, 2, 4].includes(connect)
		);
		const signal = new AbortController();
		try {
			expect((await subscribe(signal.signal)).isErr()).toBe(true);
			await vi.advanceTimersByTimeAsync(1_000);
			expect(connects()).toBe(2);
			await vi.advanceTimersByTimeAsync(2_000);
			expect(connects()).toBe(3);

			closeStream();
			await vi.advanceTimersByTimeAsync(1_000);
			expect(connects()).toBe(4);
			await vi.advanceTimersByTimeAsync(1_000);
			expect(connects()).toBe(5);
		} finally {
			signal.abort();
			runtime.dispose();
		}
	});

	it('stops retrying when the last subscriber leaves', async () => {
		vi.useFakeTimers();
		const { runtime, subscribe, connects } = createFailingConnectRuntime(() => true);
		const signal = new AbortController();
		try {
			expect((await subscribe(signal.signal)).isErr()).toBe(true);
			signal.abort();
			expect(vi.getTimerCount()).toBe(0);
			await vi.advanceTimersByTimeAsync(60_000);
			expect(connects()).toBe(1);
		} finally {
			runtime.dispose();
		}
	});

	it('resolves hydrate on the read, reports the connect error as the store error and registers on the retry', async () => {
		vi.useFakeTimers();
		let connects = 0;
		configureSync({
			cache: { adapter: noopCache },
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
			expect(hydrated.isOk()).toBe(true);
			await vi.advanceTimersByTimeAsync(0);
			expect(store.error()).toMatchObject({ code: 'bad_request', message: 'Sync subscribe failed: 503' });
			await vi.advanceTimersByTimeAsync(1300);
			expect(connects).toBe(2);
		} finally {
			store.dispose();
		}
	});
});
