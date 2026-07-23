import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport,
	type SyncEnvelope,
	type SyncError
} from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/index.ts';
import { createSyncTestSystem, Deferred } from '../fixtures/syncSystem.ts';

interface EventNote {
	readonly id: string;
	readonly title: string;
}

interface EventListManager extends ManagerTypeShape {
	readonly key: 'event-list';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			never,
			never,
			{
				readonly items: readonly EventNote[];
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
		return ok(() => {
			this.onEnvelope = undefined;
		});
	}

	emit(envelope: SyncEnvelope): void {
		this.onEnvelope?.(envelope);
	}
}

const eventCache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('client events and signals', () => {
	it('notifies fixed event subscribers and removes them idempotently', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		const itemValues: string[][] = [];
		const pendingValues: number[] = [];
		const disposedValues: boolean[] = [];
		const unsubscribeItems = store.on.items((items) => itemValues.push(items.map((item) => item.id)));
		const unsubscribePending = store.on.pending((pending) => pendingValues.push(pending.length));
		store.on.disposed((disposed) => disposedValues.push(disposed));

		try {
			expect((await store.hydrate()).isOk()).toBe(true);
			expect(
				(
					await store.add({
						input: { id: 'note-2', title: 'Added' }
					})
				).isOk()
			).toBe(true);
			expect(itemValues).toContainEqual(['note-1']);
			expect(itemValues).toContainEqual(['note-1', 'note-2']);
			expect(pendingValues).toContain(1);
			expect(pendingValues.at(-1)).toBe(0);

			unsubscribeItems();
			unsubscribeItems();
			unsubscribePending();
			const itemEventCount = itemValues.length;
			const pendingEventCount = pendingValues.length;
			expect(
				(
					await store.add({
						input: { id: 'note-3', title: 'Not observed' }
					})
				).isOk()
			).toBe(true);
			expect(itemValues).toHaveLength(itemEventCount);
			expect(pendingValues).toHaveLength(pendingEventCount);
			store.dispose();
			expect(disposedValues).toEqual([true]);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('delivers typed live-only signals without changing synchronized state or scheduling cache writes', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		const payloads: unknown[] = [];
		const otherPayloads: unknown[] = [];
		store.on.signal('toast', (payload) => payloads.push(payload));
		store.on.signal('other', (payload) => otherPayloads.push(payload));

		try {
			expect((await store.hydrate()).isOk()).toBe(true);
			const before = store.snapshot();
			const cacheWrites = system.cache.writes.length;
			system.transport.emit({
				managerKey: 'notes',
				scope: 'notes:workspace-1',
				cursor: 'signal-1',
				changes: [],
				signals: [{ type: 'toast', payload: { message: 'Saved' } }]
			});

			expect(payloads).toEqual([{ message: 'Saved' }]);
			expect(otherPayloads).toEqual([]);
			expect(store.items()).toEqual(before.items);
			expect(store.pending()).toEqual(before.pending);
			expect(system.cache.writes).toHaveLength(cacheWrites);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('emits list, hydration, repair, error, and disposal events through a real store lifecycle', async () => {
		vi.useFakeTimers();
		const transport = new ManualTransport();
		const initialRead = new Deferred<Response>();
		const repairRead = new Deferred<Response>();
		let reads = 0;
		configureSync({
			cache: { adapter: eventCache },
			transport,
			fetch: async () => {
				reads += 1;
				return reads === 1 ? initialRead.promise : repairRead.promise;
			}
		});
		const store = createStore<EventListManager>({
			key: 'event-list',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => undefined
		});
		const metaValues: ({ readonly total: number } | undefined)[] = [];
		const pageLengths: number[] = [];
		const hydratingValues: boolean[] = [];
		const refreshingValues: boolean[] = [];
		const errorValues: (SyncError | null)[] = [];
		const disposedValues: boolean[] = [];
		store.on.listMeta((meta) => metaValues.push(meta));
		store.on.pages((pages) => pageLengths.push(pages.length));
		store.on.hydrating((hydrating) => hydratingValues.push(hydrating));
		store.on.refreshing((refreshing) => refreshingValues.push(refreshing));
		store.on.error((error) => errorValues.push(error));
		store.on.disposed((disposed) => disposedValues.push(disposed));

		try {
			const hydration = store.hydrate();
			expect(hydratingValues).toContain(true);
			initialRead.resolve(
				syncResponse({
					items: [{ id: 'note-1', title: 'Initial' }],
					pageCursor: 'next-page',
					meta: { total: 1 }
				})
			);
			expect((await hydration).isOk()).toBe(true);
			expect(hydratingValues.at(-1)).toBe(false);
			expect(metaValues.at(-1)).toEqual({ total: 1 });
			expect(pageLengths.at(-1)).toBe(1);

			transport.emit(resetEnvelope());
			await vi.advanceTimersByTimeAsync(0);
			expect(refreshingValues).toContain(true);
			repairRead.resolve(syncErrorResponse('repair read failed'));
			await waitFor(() => refreshingValues.at(-1) === false && errorValues.length > 0);
			expect(errorValues.at(-1)?.message).toBe('repair read failed');

			store.dispose();
			expect(disposedValues).toEqual([true]);
		} finally {
			store.dispose();
			vi.useRealTimers();
		}
	});
});

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}

function syncErrorResponse(message: string): Response {
	return Response.json({
		isOk: false,
		isError: true,
		error: { code: 'internal', message }
	});
}

function resetEnvelope(): SyncEnvelope {
	const manifest = {
		scope: 'workspace-1',
		reason: 'retention_gap' as const,
		previousCursor: 'cursor-old',
		nextCursor: 'cursor-reset'
	};
	return {
		managerKey: 'event-list',
		scope: 'workspace-1',
		cursor: 'cursor-reset',
		changes: [{ type: 'reset', manifest }],
		reset: manifest
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
