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

interface BatchNote {
	readonly id: string;
	readonly title: string;
}

interface BatchManager extends ManagerTypeShape {
	readonly key: 'batch-client';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<never, never, { readonly items: readonly BatchNote[] }>;
		readonly add: MethodType<never, BatchNote, BatchNote>;
		readonly mutate: MethodType<{ readonly id: string }, { readonly title: string }, BatchNote>;
		readonly delete: MethodType<{ readonly id: string }, never, { readonly id: string; readonly deleted: true }>;
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

describe('client batch writes', () => {
	it('applies array add, mutate, and delete optimistically, then settles from authoritative envelopes', async () => {
		const addResponse = new Deferred<Response>();
		const mutateResponse = new Deferred<Response>();
		const deleteResponse = new Deferred<Response>();
		const requests: Request[] = [];
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				const method = new URL(request.url).pathname.split('/').at(-1);
				if (method === 'list') {
					return syncResponse({
						items: [
							{ id: 'note-1', title: 'One' },
							{ id: 'note-2', title: 'Two' }
						]
					});
				}
				if (method === 'add') {
					return addResponse.promise;
				}
				if (method === 'mutate') {
					return mutateResponse.promise;
				}
				if (method === 'delete') {
					return deleteResponse.promise;
				}
				throw new Error(`Unexpected client method ${method}.`);
			}
		});
		const store = createBatchStore();

		try {
			expect((await store.hydrate()).isOk()).toBe(true);

			const add = store.add([
				{ input: { id: 'note-3', title: 'Three optimistic' } },
				{ input: { id: 'note-4', title: 'Four optimistic' } }
			]);
			expect(store.items()).toHaveLength(4);
			expect(store.items()).toContainEqual({ id: 'note-3', title: 'Three optimistic' });
			expect(store.items()).toContainEqual({ id: 'note-4', title: 'Four optimistic' });
			expect(store.pending()[0]?.targetIds).toEqual(['note-3', 'note-4']);
			addResponse.resolve(
				syncResponse(
					batchOutput([
						{ index: 0, status: 'ok', value: { id: 'note-3', title: 'Three committed' } },
						{ index: 1, status: 'ok', value: { id: 'note-4', title: 'Four committed' } }
					]),
					envelope('add-final', [
						{
							type: 'itemAdded',
							id: 'note-3',
							value: { id: 'note-3', title: 'Three committed' }
						},
						{
							type: 'itemAdded',
							id: 'note-4',
							value: { id: 'note-4', title: 'Four committed' }
						}
					])
				)
			);
			expect((await add).isOk()).toBe(true);
			expect(store.pending()).toEqual([]);
			expect(store.items()).toContainEqual({ id: 'note-3', title: 'Three committed' });
			expect(store.items()).toContainEqual({ id: 'note-4', title: 'Four committed' });

			const mutate = store.mutate([
				{ query: { id: 'note-1' }, input: { title: 'One optimistic' } },
				{ query: { id: 'note-2' }, input: { title: 'Two optimistic' } }
			]);
			expect(store.items()).toContainEqual({ id: 'note-1', title: 'One optimistic' });
			expect(store.items()).toContainEqual({ id: 'note-2', title: 'Two optimistic' });
			mutateResponse.resolve(
				syncResponse(
					batchOutput([
						{ index: 0, status: 'ok', value: { id: 'note-1', title: 'One committed' } },
						{
							index: 1,
							status: 'error',
							error: { code: 'conflict', message: 'note-2 changed remotely' }
						}
					]),
					envelope('mutate-final', [
						{
							type: 'itemUpdated',
							id: 'note-1',
							value: { id: 'note-1', title: 'One committed' }
						}
					])
				)
			);
			const mutateResult = await mutate;
			expect(mutateResult.isOk()).toBe(true);
			expect(store.pending()).toEqual([]);
			expect(store.items()).toContainEqual({ id: 'note-1', title: 'One committed' });
			expect(store.items()).toContainEqual({ id: 'note-2', title: 'Two' });

			const remove = store.delete([{ query: { id: 'note-3' } }, { query: { id: 'note-4' } }]);
			expect(store.items().some((note) => note.id === 'note-3' || note.id === 'note-4')).toBe(false);
			deleteResponse.resolve(
				syncResponse(
					batchOutput([
						{ index: 0, status: 'ok', value: { id: 'note-3', deleted: true } },
						{ index: 1, status: 'ok', value: { id: 'note-4', deleted: true } }
					]),
					envelope('delete-final', [
						{ type: 'itemDeleted', id: 'note-3', tombstone: { id: 'note-3', deleted: true } },
						{ type: 'itemDeleted', id: 'note-4', tombstone: { id: 'note-4', deleted: true } }
					])
				)
			);
			expect((await remove).isOk()).toBe(true);
			expect(store.pending()).toEqual([]);
			expect(store.items().map((note) => note.id)).toEqual(['note-1', 'note-2']);

			expect(await requestBodies(requests)).toEqual([
				[
					{ input: { id: 'note-3', title: 'Three optimistic' } },
					{ input: { id: 'note-4', title: 'Four optimistic' } }
				],
				[
					{ query: { id: 'note-1' }, input: { title: 'One optimistic' } },
					{ query: { id: 'note-2' }, input: { title: 'Two optimistic' } }
				],
				[{ query: { id: 'note-3' } }, { query: { id: 'note-4' } }]
			]);
		} finally {
			store.dispose();
		}
	});
});

function createBatchStore() {
	return createStore<BatchManager>({
		key: 'batch-client',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => undefined
	});
}

function syncResponse(value: unknown, responseEnvelope?: SyncEnvelope): Response {
	return Response.json({
		isOk: true,
		isError: false,
		value,
		...(responseEnvelope ? { envelope: responseEnvelope } : {})
	});
}

function batchOutput(items: readonly unknown[]) {
	return {
		items,
		execution: {
			mode: 'loop',
			atomic: false,
			okCount: items.filter((item) => readStatus(item) === 'ok').length,
			errorCount: items.filter((item) => readStatus(item) === 'error').length
		}
	};
}

function readStatus(value: unknown): unknown {
	return typeof value === 'object' && value !== null && 'status' in value ? value.status : undefined;
}

function envelope(cursor: string, changes: SyncEnvelope['changes']): SyncEnvelope {
	return {
		managerKey: 'batch-client',
		scope: 'workspace-1',
		cursor,
		changes
	};
}

async function requestBodies(requests: readonly Request[]): Promise<unknown[]> {
	const bodies: unknown[] = [];
	for (const request of requests) {
		if (request.method === 'POST') {
			bodies.push(await request.json());
		}
	}
	return bodies;
}
