import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport,
	type SyncEnvelope
} from '../../src/client/core.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface Note {
	readonly id: string;
	readonly title: string;
}

interface OptimisticManager extends ManagerTypeShape {
	readonly key: 'optimistic-rebase';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<never, never, { readonly items: readonly Note[] }>;
		readonly mutate: MethodType<{ readonly id: string }, { readonly title: string }, Note>;
	};
}

class ManualTransport implements RuntimeTransport {
	private onEnvelope: ((envelope: SyncEnvelope) => void) | undefined;

	async subscribe(options: Parameters<RuntimeTransport['subscribe']>[0]) {
		this.onEnvelope = options.onEnvelope;
		return ok(() => {});
	}

	emit(envelope: SyncEnvelope): void {
		this.onEnvelope?.(envelope);
	}
}

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

afterEach(() => {
	resetSyncConfiguration();
});

function createOptimisticStore() {
	return createStore<OptimisticManager>({
		key: 'optimistic-rebase',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => undefined
	});
}

describe('optimistic rebase and mutation identity', () => {
	it('rebases a later pending write when an earlier write to the same item fails', async () => {
		const writes: { readonly id: string; readonly response: Deferred<Response> }[] = [];
		configureSync({
			cache: { adapter: cache },
			transport: new ManualTransport(),
			fetch: async (_input, init) => {
				if (init?.method !== 'POST') {
					return syncResponse({ items: [{ id: 'note-1', title: 'Base' }] });
				}
				const response = new Deferred<Response>();
				writes.push({
					id: new Headers(init.headers).get('x-mutation-id') ?? '',
					response
				});
				return response.promise;
			}
		});
		const store = createOptimisticStore();
		await store.hydrate();

		const first = store.mutate({ query: { id: 'note-1' }, input: { title: 'First' } });
		const second = store.mutate({ query: { id: 'note-1' }, input: { title: 'Second' } });
		await waitFor(() => writes.length === 2);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Second' }]);

		writes[0]?.response.resolve(syncErrorResponse('first write failed'));
		expect((await first).isErr()).toBe(true);
		expect(store.pending()).toHaveLength(1);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Second' }]);

		writes[1]?.response.resolve(
			syncResponse(
				{ id: 'note-1', title: 'Second' },
				finalityEnvelope(writes[1]?.id ?? '', 'client-local', 'Second')
			)
		);
		expect((await second).isOk()).toBe(true);
		expect(store.pending()).toHaveLength(0);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Second' }]);
		store.dispose();
	});

	it('does not finalize local pending state for a colliding mutation id from another client', async () => {
		const transport = new ManualTransport();
		const writeResponse = new Deferred<Response>();
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (_input, init) => {
				if (init?.method !== 'POST') {
					return syncResponse({ items: [{ id: 'note-1', title: 'Base' }] });
				}
				mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
				return writeResponse.promise;
			}
		});
		const store = createOptimisticStore();
		await store.hydrate();
		const write = store.mutate({ query: { id: 'note-1' }, input: { title: 'Local' } });
		await waitFor(() => mutationId.length > 0);

		transport.emit(finalityEnvelope(mutationId, 'different-client', 'Remote'));
		expect(store.pending()).toHaveLength(1);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Local' }]);

		writeResponse.resolve(
			syncResponse({ id: 'note-1', title: 'Local' }, finalityEnvelope(mutationId, '', 'Local'))
		);
		expect((await write).isOk()).toBe(true);
		expect(store.pending()).toHaveLength(0);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Local' }]);
		store.dispose();
	});
});

function finalityEnvelope(mutationId: string, sourceClientId: string, title: string): SyncEnvelope {
	return {
		managerKey: 'optimistic-rebase',
		scope: 'workspace-1',
		cursor: `cursor-${title}`,
		sourceMutationId: mutationId,
		sourceClientId,
		changes: [{ type: 'itemUpdated', id: 'note-1', value: { id: 'note-1', title } }]
	};
}

function syncResponse(value: unknown, envelope?: SyncEnvelope): Response {
	return Response.json({
		isOk: true,
		isError: false,
		value,
		...(envelope ? { envelope } : {})
	});
}

function syncErrorResponse(message: string): Response {
	return Response.json({
		isOk: false,
		isError: true,
		error: {
			code: 'internal',
			message
		}
	});
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
