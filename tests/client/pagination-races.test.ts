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
	readonly status: 'open' | 'closed';
}

interface ListQuery {
	readonly status: 'open' | 'closed';
	readonly limit: number;
	readonly cursor?: string;
}

interface PaginationManager extends ManagerTypeShape {
	readonly key: 'pagination-races';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			ListQuery,
			never,
			{
				readonly items: readonly Note[];
				readonly pageCursor?: string;
				readonly meta?: { readonly total: number };
			}
		>;
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

function createPaginationStore() {
	return createStore<PaginationManager>(
		{
			key: 'pagination-races',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ status: 'open', limit: 2 })
		},
		(reconcile) =>
			reconcile.defaults({
				matchesQuery: (item, context) => item.status === context.query?.status,
				compare: (left, right) => left.title.localeCompare(right.title)
			})
	);
}

describe('pagination and query-family races', () => {
	it('injects the page cursor, appends pages, and preserves first-page metadata when loadMore omits it', async () => {
		const queries: ListQuery[] = [];
		configureSync({
			cache: { adapter: cache },
			transport: successfulTransport(),
			fetch: async (input) => {
				const query = readQuery(input);
				queries.push(query);
				return query.cursor
					? syncResponse({
							items: [{ id: 'note-3', title: 'Three', status: 'open' }],
							pageCursor: undefined
						})
					: syncResponse({
							items: [
								{ id: 'note-1', title: 'One', status: 'open' },
								{ id: 'note-2', title: 'Two', status: 'open' }
							],
							pageCursor: 'page-2',
							meta: { total: 3 }
						});
			}
		});
		const store = createPaginationStore();

		expect((await store.refresh()).isOk()).toBe(true);
		expect((await store.loadMore()).isOk()).toBe(true);
		expect(queries).toEqual([
			{ status: 'open', limit: 2 },
			{ status: 'open', limit: 2, cursor: 'page-2' }
		]);
		expect(store.items().map((item) => item.id)).toEqual(['note-1', 'note-3', 'note-2']);
		expect(store.pages()).toHaveLength(2);
		expect(store.listMeta()).toEqual({ total: 3 });
		store.dispose();
	});

	it('keeps independently loaded query families isolated', async () => {
		configureSync({
			cache: { adapter: cache },
			transport: successfulTransport(),
			fetch: async (input) => {
				const query = readQuery(input);
				return syncResponse({
					items: [
						{
							id: `${query.status}-1`,
							title: query.status,
							status: query.status
						}
					],
					meta: { total: query.status === 'open' ? 1 : 2 }
				});
			}
		});
		const store = createPaginationStore();
		const open = store.list({ status: 'open', limit: 2 });
		const closed = store.list({ status: 'closed', limit: 2 });

		await open.refresh();
		await closed.refresh();
		expect(open.items().map((item) => item.id)).toEqual(['open-1']);
		expect(open.meta()).toEqual({ total: 1 });
		expect(closed.items().map((item) => item.id)).toEqual(['closed-1']);
		expect(closed.meta()).toEqual({ total: 2 });
		expect(store.items()).toEqual([]);
		store.dispose();
	});

	it('does not let a slower stale query replace the newer active query', async () => {
		const openResponse = new Deferred<Response>();
		let activeStatus: 'open' | 'closed' = 'open';
		configureSync({
			cache: { adapter: cache },
			transport: successfulTransport(),
			fetch: async (input) => {
				const query = readQuery(input);
				if (query.status === 'open') {
					return openResponse.promise;
				}
				return syncResponse({
					items: [{ id: 'closed-1', title: 'Closed', status: 'closed' }],
					meta: { total: 1 }
				});
			}
		});
		const store = createStore<PaginationManager>(
			{
				key: 'pagination-races',
				getParams: () => ({ workspaceId: 'workspace-1' }),
				getUrl: () => 'http://sync.test/notes',
				query: () => ({ status: activeStatus, limit: 2 })
			},
			(reconcile) =>
				reconcile.defaults({
					matchesQuery: (item, context) => item.status === context.query?.status,
					compare: (left, right) => left.title.localeCompare(right.title)
				})
		);
		const stale = store.refresh();
		activeStatus = 'closed';
		const current = store.refresh();
		expect((await current).isOk()).toBe(true);
		openResponse.resolve(
			syncResponse({
				items: [{ id: 'open-1', title: 'Open', status: 'open' }],
				meta: { total: 1 }
			})
		);
		expect((await stale).isOk()).toBe(true);

		expect(store.items()).toEqual([{ id: 'closed-1', title: 'Closed', status: 'closed' }]);
		expect(store.list({ status: 'closed', limit: 2 }).items()).toEqual([
			{ id: 'closed-1', title: 'Closed', status: 'closed' }
		]);
		expect(store.list({ status: 'open', limit: 2 }).items()).toEqual([]);
		store.dispose();
	});

	it('places realtime additions only in matching families and keeps compare ordering', async () => {
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (input) => {
				const query = readQuery(input);
				return syncResponse({
					items: [{ id: 'z', title: 'Zulu', status: query.status }]
				});
			}
		});
		const store = createPaginationStore();
		const open = store.list({ status: 'open', limit: 2 });
		const closed = store.list({ status: 'closed', limit: 2 });
		await open.refresh();
		await closed.refresh();
		await store.connect();

		transport.emit(realtimeAdd('cursor-1', { id: 'a', title: 'Alpha', status: 'open' }));
		transport.emit(realtimeAdd('cursor-2', { id: 'b', title: 'Beta', status: 'closed' }));
		expect(open.items().map((item) => item.id)).toEqual(['a', 'z']);
		expect(closed.items().map((item) => item.id)).toEqual(['b', 'z']);
		store.dispose();
	});
});

function successfulTransport(): RuntimeTransport {
	return {
		async subscribe() {
			return ok(() => {});
		}
	};
}

function readQuery(input: RequestInfo | URL): ListQuery {
	const url = new URL(input instanceof Request ? input.url : String(input));
	return JSON.parse(url.searchParams.get('query') ?? '{}') as ListQuery;
}

function realtimeAdd(cursor: string, note: Note): SyncEnvelope {
	return {
		managerKey: 'pagination-races',
		scope: 'workspace-1',
		cursor,
		changes: [{ type: 'itemAdded', id: note.id, value: note }]
	};
}

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}
