import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport
} from '../../src/client/core.ts';

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

	it('characterizes a read response that arrives after store disposal', async () => {
		const response = deferred<Response>();
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
		// DIVERGENCE: a fetch implementation that ignores abort can return success and mutate a disposed store.
		expect(result.isOk()).toBe(true);
		if (result.isOk()) {
			expect(result.value).toEqual({ id: 'note-1', title: 'Late' });
		}
		expect(store.data()).toEqual({ id: 'note-1', title: 'Late' });
		expect(store.pending()).toEqual([]);
	});

	it('characterizes an optimistic write response that arrives after store disposal', async () => {
		const response = deferred<Response>();
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
		// DIVERGENCE: a fetch that ignores abort can still make a disposed store report the write as successful.
		expect(result.isOk()).toBe(true);
		if (result.isOk()) {
			expect(result.value).toEqual({ id: 'note-1', title: 'Committed' });
		}
		expect(store.items()).toEqual([]);
		expect(store.pending()).toEqual([]);
	});
});

function createListStore(fetchImpl: typeof fetch): ReturnType<typeof createStore<ListManager>> {
	configureSync({
		cache: { adapter: cache },
		transport,
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
		cache: { adapter: cache },
		transport,
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

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}

function deferred<TValue>() {
	let resolvePromise!: (value: TValue | PromiseLike<TValue>) => void;
	const promise = new Promise<TValue>((resolve) => {
		resolvePromise = resolve;
	});
	return {
		promise,
		resolve: resolvePromise
	};
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
