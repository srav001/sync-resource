import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType,
	type SyncEnvelope
} from '../../src/client/core.ts';
import { ManualTransport, noopCache } from '../fixtures/clientRuntime.ts';
import { syncResponse } from '../fixtures/responses.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface Profile {
	readonly id: string;
	readonly name: string;
}

interface GetManager extends ManagerTypeShape {
	readonly key: 'get-data';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly get: MethodType<never, never, Profile>;
	};
}

interface QueriedGetManager extends ManagerTypeShape {
	readonly key: 'queried-get-data';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly get: MethodType<{ readonly locale: string }, never, Profile>;
	};
}

interface MutableGetManager extends ManagerTypeShape {
	readonly key: 'mutable-get-data';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly get: MethodType<never, never, Profile>;
		readonly mutate: MethodType<{ readonly id: string }, { readonly name: string }, Profile>;
	};
}

afterEach(() => {
	resetSyncConfiguration();
});

describe('client get and data', () => {
	it('keeps construction network-free and omits an absent query from a direct get', async () => {
		const requests: Request[] = [];
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async (input, init) => {
				requests.push(new Request(input, init));
				return syncResponse({ id: 'profile-1', name: 'Initial' });
			}
		});
		const store = createGetStore();
		const observedData: (Profile | undefined)[] = [];
		store.on.data((data) => observedData.push(data));

		expect(requests).toHaveLength(0);
		expect(store.data()).toBeUndefined();

		const result = await store.get();
		expect(result.isOk()).toBe(true);
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Initial' });
		expect(observedData).toEqual([{ id: 'profile-1', name: 'Initial' }]);
		expect(new URL(requests[0]!.url).pathname).toBe('/profiles/get');
		expect(new URL(requests[0]!.url).searchParams.has('query')).toBe(false);
		store.dispose();
	});

	it('serializes a supplied get query without changing the configured scope URL', async () => {
		const requests: Request[] = [];
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async (input, init) => {
				requests.push(new Request(input, init));
				return syncResponse({ id: 'profile-1', name: 'French' });
			}
		});
		const store = createQueriedGetStore();

		const result = await store.get({ query: { locale: 'fr-AU' } });
		expect(result.isOk()).toBe(true);
		expect(new URL(requests[0]!.url).pathname).toBe('/profiles/get');
		expect(new URL(requests[0]!.url).searchParams.get('query')).toBe(JSON.stringify({ locale: 'fr-AU' }));
		store.dispose();
	});

	it('hydrates data and carries the authoritative response cursor into realtime', async () => {
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () =>
				syncResponse(
					{ id: 'profile-1', name: 'Hydrated' },
					envelope('cursor-7', [
						{ type: 'itemUpdated', id: 'profile-1', value: { id: 'profile-1', name: 'Hydrated' } }
					])
				)
		});
		const store = createGetStore();

		const result = await store.hydrate();
		expect(result.isOk()).toBe(true);
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Hydrated' });
		expect(transport.options?.getCursor?.()).toBe('cursor-7');
		expect(new URL(transport.options?.url ?? '').searchParams.get('after')).toBe('cursor-7');
		store.dispose();
	});

	it('lets a get read without an envelope replace the cursor an earlier read set', async () => {
		const transport = new ManualTransport();
		let reads = 0;
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () =>
				++reads === 1
					? syncResponse({ id: 'profile-1', name: 'First' }, envelope('cursor-7', []))
					: syncResponse({ id: 'profile-1', name: 'Second' })
		});
		const store = createGetStore();
		try {
			await store.hydrate();
			expect(transport.options?.getCursor?.()).toBe('cursor-7');
			expect((await store.get()).isOk()).toBe(true);
			expect(store.data()).toEqual({ id: 'profile-1', name: 'Second' });
			expect(transport.options?.getCursor?.()).toBeUndefined();
		} finally {
			store.dispose();
		}
	});

	it('replaces authoritative data from realtime and rebases a pending mutation over a later envelope', async () => {
		const transport = new ManualTransport();
		const writeResponse = new Deferred<Response>();
		let mutationId = '';
		configureSync({
			cache: { adapter: noopCache },
			transport,
			createId: (prefix) => `${prefix}-1`,
			fetch: async (_input, init) => {
				if (init?.method !== 'POST') {
					return syncResponse({ id: 'profile-1', name: 'Initial' });
				}
				mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
				return writeResponse.promise;
			}
		});
		const store = createMutableGetStore();
		await store.hydrate();

		transport.emit(
			envelope('cursor-1', [{ type: 'itemUpdated', id: 'profile-1', value: { id: 'profile-1', name: 'Remote' } }])
		);
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Remote' });

		const write = store.mutate({ query: { id: 'profile-1' }, input: { name: 'Local' } });
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Local' });
		transport.emit(
			envelope('cursor-2', [
				{ type: 'itemUpdated', id: 'profile-1', value: { id: 'profile-1', name: 'Other client' } }
			])
		);
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Local' });

		writeResponse.resolve(
			syncResponse(
				{ id: 'profile-1', name: 'Committed' },
				{
					...envelope('cursor-3', [
						{ type: 'itemUpdated', id: 'profile-1', value: { id: 'profile-1', name: 'Committed' } }
					]),
					sourceMutationId: mutationId
				}
			)
		);
		expect((await write).isOk()).toBe(true);
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Committed' });
		expect(store.pending()).toEqual([]);
		store.dispose();
	});

	it('rejects malformed get responses without replacing the last valid data', async () => {
		let requestCount = 0;
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () => {
				requestCount += 1;
				return requestCount === 1
					? syncResponse({ id: 'profile-1', name: 'Valid' })
					: Response.json({ malformed: true });
			}
		});
		const store = createGetStore();
		expect((await store.get()).isOk()).toBe(true);

		const result = await store.get();
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('validation');
		}
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Valid' });
		store.dispose();
	});

	it('unsubscribes on disposal and retains the final data snapshot', async () => {
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () => syncResponse({ id: 'profile-1', name: 'Initial' })
		});
		const store = createGetStore();
		await store.hydrate();

		store.dispose();
		expect(transport.unsubscribeCount).toBe(1);
		expect(transport.options).toBeUndefined();
		expect(store.data()).toEqual({ id: 'profile-1', name: 'Initial' });
	});

	it('ignores a transport invoking its captured envelope callback after disposal', async () => {
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: noopCache },
			transport,
			fetch: async () => syncResponse({ id: 'profile-1', name: 'Initial' })
		});
		const store = createGetStore();
		await store.hydrate();
		store.dispose();

		transport.emitCaptured(
			envelope('cursor-after-dispose', [
				{ type: 'itemUpdated', id: 'profile-1', value: { id: 'profile-1', name: 'Late realtime' } }
			])
		);

		expect(store.data()).toEqual({ id: 'profile-1', name: 'Initial' });
	});
});

function createGetStore(): ReturnType<typeof createStore<GetManager>> {
	return createStore<GetManager>({
		key: 'get-data',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/profiles'
	});
}

function createQueriedGetStore(): ReturnType<typeof createStore<QueriedGetManager>> {
	return createStore<QueriedGetManager>({
		key: 'queried-get-data',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/profiles'
	});
}

function createMutableGetStore(): ReturnType<typeof createStore<MutableGetManager>> {
	return createStore<MutableGetManager>({
		key: 'mutable-get-data',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/profiles'
	});
}

function envelope(cursor: string, changes: SyncEnvelope['changes']): SyncEnvelope {
	return {
		managerKey: 'get-data',
		scope: 'workspace-1',
		cursor,
		changes
	};
}
