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

interface Message {
	readonly id: string;
	readonly createdAt: string;
	readonly status: 'open' | 'closed';
}

interface MessageQuery {
	readonly status: Message['status'];
	readonly limit: number;
	readonly cursor?: string;
}

interface MessagesManager extends ManagerTypeShape {
	readonly key: 'sorted-add';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			MessageQuery,
			never,
			{ readonly items: readonly Message[]; readonly pageCursor?: string }
		>;
		readonly add: MethodType<never, Message, Message>;
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

describe('sorted add acknowledgement reconciliation', () => {
	it('keeps an acknowledged newest add in a partially loaded sorted window as realtime items arrive and update', async () => {
		const transport = new ManualTransport();
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport,
			createId: (prefix) => `${prefix}-local`,
			fetch: async (_input, init) => {
				if (init?.method !== 'POST') {
					return syncResponse({
						items: [message('existing-new', '2026-09-03'), message('existing-old', '2026-09-02')],
						pageCursor: 'page-2'
					});
				}
				mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
				const envelope = itemAddedEnvelope('add-1', mutationId, message('mine', '2026-09-05'));
				return syncResponse(message('mine', '2026-09-05'), {
					...envelope,
					changes: [
						...envelope.changes,
						{
							type: 'itemAdded',
							id: 'run',
							value: message('run', '2026-09-04')
						}
					]
				});
			}
		});
		const store = createSortedStore({ addPlacement: 'sorted' }, (left, right) =>
			right.createdAt.localeCompare(left.createdAt)
		);
		try {
			await store.refresh();
			await store.connect();
			const add = await store.add({ input: message('mine', '2026-09-05') });
			expect(add.isOk()).toBe(true);
			expect(store.items().map((item) => item.id)).toEqual(['mine', 'run', 'existing-new', 'existing-old']);

			transport.emit(itemAddedEnvelope('add-2', undefined, message('assistant', '2026-09-06')));
			expect(store.items().map((item) => item.id)).toEqual([
				'assistant',
				'mine',
				'run',
				'existing-new',
				'existing-old'
			]);

			transport.emit({
				managerKey: 'sorted-add',
				scope: 'workspace-1',
				cursor: 'update-1',
				changes: [{ type: 'itemUpdated', id: 'assistant', value: message('assistant', '2026-09-01') }]
			});
			expect(store.items().map((item) => item.id)).toEqual([
				'mine',
				'run',
				'existing-new',
				'existing-old',
				'assistant'
			]);
			expect(mutationId).toBe('mutation-local');
		} finally {
			store.dispose();
		}
	});

	it('keeps the cursor boundary recoverable when sorted adds arrive before loadMore', async () => {
		const transport = new ManualTransport();
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (input, init) => {
				if (init?.method === 'POST') {
					mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
					return syncResponse(
						message('mine', '2026-09-21'),
						itemAddedEnvelope('add-1', mutationId, message('mine', '2026-09-21'))
					);
				}
				const query = readQuery(input);
				return query.cursor
					? syncResponse({
							items: [message('existing-18', '2026-09-18'), message('existing-17', '2026-09-17')]
						})
					: syncResponse({
							items: [message('existing-20', '2026-09-20'), message('existing-19', '2026-09-19')],
							pageCursor: 'page-2'
						});
			}
		});
		const store = createSortedStore({ addPlacement: 'sorted' }, (left, right) =>
			right.createdAt.localeCompare(left.createdAt)
		);
		try {
			await store.refresh();
			await store.connect();
			await store.add({ input: message('mine', '2026-09-21') });
			transport.emit(itemAddedEnvelope('add-2', undefined, message('assistant', '2026-09-22')));
			expect(store.items().map((item) => item.id)).toEqual(['assistant', 'mine', 'existing-20', 'existing-19']);

			await store.loadMore();
			expect(store.items().map((item) => item.id)).toEqual([
				'assistant',
				'mine',
				'existing-20',
				'existing-19',
				'existing-18',
				'existing-17'
			]);
		} finally {
			store.dispose();
		}
	});

	it('reconciles one own add across loaded pages and query families', async () => {
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport: new ManualTransport(),
			fetch: async (input, init) => {
				if (init?.method === 'POST') {
					mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
					return syncResponse(
						message('mine', '2026-09-02.5'),
						itemAddedEnvelope('add-1', mutationId, message('mine', '2026-09-02.5'))
					);
				}
				const query = readQuery(input);
				if (query.status === 'closed') {
					return syncResponse({
						items: [
							message('closed-new', '2026-09-03', 'closed'),
							message('closed-old', '2026-09-02', 'closed')
						],
						pageCursor: 'closed-page-2'
					});
				}
				if (query.cursor) {
					return syncResponse({
						items: [message('open-older', '2026-09-01'), message('open-oldest', '2026-09-00')],
						pageCursor: 'open-page-3'
					});
				}
				return syncResponse({
					items: [message('open-new', '2026-09-03'), message('open-old', '2026-09-02')],
					pageCursor: 'open-page-2'
				});
			}
		});
		const store = createSortedStore({ addPlacement: 'sorted' }, (left, right) =>
			right.createdAt.localeCompare(left.createdAt)
		);
		const open = store.list({ status: 'open', limit: 2 });
		const closed = store.list({ status: 'closed', limit: 2 });
		try {
			await open.refresh();
			await open.loadMore();
			await closed.refresh();
			const add = await store.add({ input: message('mine', '2026-09-02.5') });
			expect(add.isOk()).toBe(true);
			expect(open.items().map((item) => item.id)).toEqual([
				'open-new',
				'mine',
				'open-old',
				'open-older',
				'open-oldest'
			]);
			expect(open.pages().map((page) => page.ids)).toEqual([
				['open-new', 'mine'],
				['open-old', 'open-older', 'open-oldest']
			]);
			expect(closed.items().map((item) => item.id)).toEqual(['closed-new', 'closed-old']);
		} finally {
			store.dispose();
		}
	});

	it('preserves the legacy append behavior unless sorted placement is opted in', async () => {
		const transport = new ManualTransport();
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (_input, init) =>
				init?.method === 'POST'
					? (() => {
							mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
							return syncResponse(
								message('mine', '2026-09-05'),
								itemAddedEnvelope('add-1', mutationId, message('mine', '2026-09-05'))
							);
						})()
					: syncResponse({
							items: [message('existing-new', '2026-09-03'), message('existing-old', '2026-09-02')],
							pageCursor: 'page-2'
						})
		});
		const store = createSortedStore(undefined, (left, right) => right.createdAt.localeCompare(left.createdAt));
		try {
			await store.refresh();
			await store.connect();
			await store.add({ input: message('mine', '2026-09-05') });
			expect(store.pages()[0]?.ids).toEqual(['existing-new', 'existing-old', 'mine']);
			transport.emit(itemAddedEnvelope('add-2', undefined, message('assistant', '2026-09-06')));
			expect(store.items().some((item) => item.id === 'mine')).toBe(false);
		} finally {
			store.dispose();
		}
	});

	it('preserves default append handling for every itemAdded change in an own envelope', async () => {
		let mutationId = '';
		configureSync({
			cache: { adapter: cache },
			transport: new ManualTransport(),
			fetch: async (_input, init) => {
				if (init?.method === 'POST') {
					mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
					const envelope = itemAddedEnvelope('add-1', mutationId, message('mine', '2026-09-05'));
					return syncResponse(message('mine', '2026-09-05'), {
						...envelope,
						changes: [
							...envelope.changes,
							{ type: 'itemAdded', id: 'run', value: message('run', '2026-09-01') }
						]
					});
				}
				return syncResponse({
					items: [message('existing-new', '2026-09-03'), message('existing-old', '2026-09-02')],
					pageCursor: 'page-2'
				});
			}
		});
		const store = createSortedStore(undefined, (left, right) => right.createdAt.localeCompare(left.createdAt));
		try {
			await store.refresh();
			await store.add({ input: message('mine', '2026-09-05') });
			expect(store.pages()[0]?.ids).toEqual(['existing-new', 'existing-old', 'mine', 'run']);
		} finally {
			store.dispose();
		}
	});

	it('rolls back a pending sorted add without changing loaded pages', async () => {
		const transport = new ManualTransport();
		const postResponse = new Deferred<Response>();
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (_input, init) =>
				init?.method === 'POST'
					? postResponse.promise
					: syncResponse({
							items: [message('existing-new', '2026-09-03'), message('existing-old', '2026-09-02')],
							pageCursor: 'page-2'
						})
		});
		const store = createSortedStore({ addPlacement: 'sorted' }, (left, right) =>
			right.createdAt.localeCompare(left.createdAt)
		);
		try {
			await store.refresh();
			await store.connect();
			const resultPromise = store.add({ input: message('mine', '2026-09-05') });
			await Promise.resolve();
			expect(store.items().map((item) => item.id)).toEqual(['mine', 'existing-new', 'existing-old']);
			transport.emit(itemAddedEnvelope('remote-1', undefined, message('remote', '2026-09-06')));
			expect(store.items().map((item) => item.id)).toEqual(['remote', 'mine', 'existing-new', 'existing-old']);
			postResponse.resolve(syncErrorResponse('rejected'));
			const result = await resultPromise;
			expect(result.isErr()).toBe(true);
			expect(store.items().map((item) => item.id)).toEqual(['remote', 'existing-new', 'existing-old']);
			expect(store.pages()[0]?.ids).toEqual(['remote', 'existing-new', 'existing-old']);
		} finally {
			store.dispose();
		}
	});

	it('rejects sorted placement without a comparator at store construction', () => {
		configureSync({ cache: { adapter: cache }, transport: new ManualTransport() });
		expect(() => createSortedStore({ addPlacement: 'sorted' }, undefined)).toThrow(
			"reconcile.addPlacement 'sorted' requires reconcile.compare."
		);
	});
});

function createSortedStore(
	placement: { readonly addPlacement: 'sorted' } | undefined,
	compare: ((left: Message, right: Message) => number) | undefined
) {
	return createStore<MessagesManager>(
		{
			key: 'sorted-add',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/messages',
			query: () => ({ status: 'open', limit: 2 })
		},
		(reconcile) =>
			reconcile.defaults({
				...placement,
				matchesQuery: (item, context) => item.status === context.query?.status,
				...(compare ? { compare } : {})
			})
	);
}

function message(id: string, createdAt: string, status: Message['status'] = 'open'): Message {
	return { id, createdAt, status };
}

function itemAddedEnvelope(cursor: string, sourceMutationId: string | undefined, value: Message): SyncEnvelope {
	return {
		managerKey: 'sorted-add',
		scope: 'workspace-1',
		cursor,
		...(sourceMutationId ? { sourceMutationId, sourceClientId: 'client-local' } : {}),
		changes: [{ type: 'itemAdded', id: value.id, value }]
	};
}

function readQuery(input: RequestInfo | URL): MessageQuery {
	const url = new URL(input instanceof Request ? input.url : String(input));
	return JSON.parse(url.searchParams.get('query') ?? '{}') as MessageQuery;
}

function syncResponse(value: unknown, envelope?: SyncEnvelope): Response {
	return Response.json({ isOk: true, isError: false, value, ...(envelope ? { envelope } : {}) });
}

function syncErrorResponse(message: string): Response {
	return Response.json({ isOk: false, isError: true, error: { code: 'internal', message } });
}
