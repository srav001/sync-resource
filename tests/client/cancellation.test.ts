import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType
} from '../../src/client/core.ts';
import { nextMacrotask, waitFor } from '../fixtures/async.ts';
import { noopCache, noopTransport } from '../fixtures/clientRuntime.ts';
import { syncResponse } from '../fixtures/responses.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface ListManager extends ManagerTypeShape {
	readonly key: 'cancellation-list';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			never,
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

interface GetManager extends ManagerTypeShape {
	readonly key: 'cancellation-get';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly get: MethodType<never, never, { readonly id: string; readonly title: string }>;
	};
}

afterEach(() => {
	resetSyncConfiguration();
});

describe('client request cancellation', () => {
	it('rolls back an optimistic write when its caller signal is already aborted', async () => {
		const caller = new AbortController();
		caller.abort('caller stopped');
		const store = createListStore(abortAwareFetch());

		const result = await store.add({ input: { id: 'note-1', title: 'Temporary' } }, { signal: caller.signal });

		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('aborted');
		}
		expect(store.items()).toEqual([]);
		expect(store.pending()).toEqual([]);
		store.dispose();
	});

	it('rolls back an optimistic write when the caller aborts an in-flight request', async () => {
		const caller = new AbortController();
		let requestStarted = false;
		const store = createListStore(
			abortAwareFetch(() => {
				requestStarted = true;
			})
		);

		const operation = store.add({ input: { id: 'note-1', title: 'Temporary' } }, { signal: caller.signal });
		await waitFor(() => requestStarted);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Temporary' }]);
		expect(store.pending()).toHaveLength(1);
		caller.abort('caller stopped');
		const result = await operation;

		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('aborted');
		}
		expect(store.items()).toEqual([]);
		expect(store.pending()).toEqual([]);
		store.dispose();
	});

	it('ignores a read response that arrives after store disposal', async () => {
		const response = new Deferred<Response>();
		let requestSignal: AbortSignal | null | undefined;
		const store = createGetStore(async (_input, init) => {
			requestSignal = init?.signal;
			return response.promise;
		});
		const operation = store.get();
		store.dispose();
		expect(requestSignal?.aborted).toBe(true);
		response.resolve(syncResponse({ id: 'note-1', title: 'Late' }));

		const result = await operation;
		expect(result.isErr() && result.error.code).toBe('aborted');
		expect(store.data()).toBeUndefined();
		expect(store.pending()).toEqual([]);
	});

	it('ignores a cached snapshot that is read after store disposal', async () => {
		const cached = new Deferred<{ readonly value: unknown; readonly expiry: number } | undefined>();
		configureSync({
			cache: {
				adapter: {
					...noopCache,
					get: <TValue>() => cached.promise as Promise<{ value: TValue; expiry: number }>
				}
			},
			transport: noopTransport,
			fetch: abortAwareFetch(),
			createId: (prefix) => `${prefix}-1`
		});
		const store = createStore<GetManager>({
			key: 'cancellation-get',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/note'
		});
		store.dispose();
		cached.resolve({
			value: { baseData: { id: 'note-1', title: 'Cached' }, baseItems: [], families: [] },
			expiry: 0
		});
		await nextMacrotask();

		expect(store.data()).toBeUndefined();
	});

	it('ignores an optimistic write response that arrives after store disposal', async () => {
		const response = new Deferred<Response>();
		let requestSignal: AbortSignal | null | undefined;
		const store = createListStore(async (_input, init) => {
			requestSignal = init?.signal;
			return response.promise;
		});
		const operation = store.add({ input: { id: 'note-1', title: 'Temporary' } });
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Temporary' }]);
		store.dispose();
		expect(requestSignal?.aborted).toBe(true);
		response.resolve(syncResponse({ id: 'note-1', title: 'Committed' }));

		const result = await operation;
		expect(result.isErr() && result.error.code).toBe('aborted');
		expect(store.items()).toEqual([]);
		expect(store.pending()).toEqual([]);
	});
});

function createListStore(fetchImpl: typeof fetch): ReturnType<typeof createStore<ListManager>> {
	configureSync({
		cache: { adapter: noopCache },
		transport: noopTransport,
		fetch: fetchImpl,
		createId: (prefix) => `${prefix}-1`
	});
	return createStore<ListManager>({
		key: 'cancellation-list',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => undefined
	});
}

function createGetStore(fetchImpl: typeof fetch): ReturnType<typeof createStore<GetManager>> {
	configureSync({
		cache: { adapter: noopCache },
		transport: noopTransport,
		fetch: fetchImpl,
		createId: (prefix) => `${prefix}-1`
	});
	return createStore<GetManager>({
		key: 'cancellation-get',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/note'
	});
}

function abortAwareFetch(onStart?: () => void): typeof fetch {
	return async (_input, init) => {
		onStart?.();
		const signal = init?.signal;
		if (!signal) {
			throw new Error('Expected an abort signal.');
		}
		if (signal.aborted) {
			throw signal.reason;
		}
		return new Promise<Response>((_resolve, reject) => {
			signal.addEventListener('abort', () => reject(signal.reason), { once: true });
		});
	};
}
