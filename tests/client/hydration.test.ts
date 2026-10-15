import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { nextMacrotask, waitFor } from '../fixtures/async.ts';
import { createSyncTestSystem, Deferred, SYNC_TEST_NOW } from '../fixtures/syncSystem.ts';

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

const cacheKey = 'sync-resource:notes:{"workspaceId":"workspace-1"}:view:v2';
const cacheNote = (system: ReturnType<typeof createSyncTestSystem>, title: string) =>
	system.cache.set(cacheKey, {
		value: { baseItems: [{ id: 'note-1', title }], families: [] },
		expiry: SYNC_TEST_NOW + 60_000
	});
const recordTitles = (store: ReturnType<ReturnType<typeof createSyncTestSystem>['createStore']>) => {
	const snapshots: string[][] = [];
	store.on.items((items) => snapshots.push(items.map((note) => note.title)));
	return snapshots;
};

describe('client hydration and cache', () => {
	it('deletes an expired authoritative snapshot instead of restoring it', async () => {
		const system = createSyncTestSystem();
		const cacheKey = 'sync-resource:notes:{"workspaceId":"workspace-1"}:view:v2';
		await system.cache.set(cacheKey, {
			value: { baseItems: [{ id: 'expired', title: 'Expired' }], families: [] },
			expiry: SYNC_TEST_NOW - 1
		});
		const store = system.createStore();
		try {
			const restored = await store.restore();
			expect(restored.isOk()).toBe(true);
			expect(store.items()).toEqual([]);
			expect(system.cache.deletes).toContain(cacheKey);
			expect(system.cache.peek(cacheKey)).toBeUndefined();
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('keeps construction network-free and restores an authoritative cache snapshot', async () => {
		const system = createSyncTestSystem();
		const cacheKey = 'sync-resource:notes:{"workspaceId":"workspace-1"}:view:v2';
		await system.cache.set(cacheKey, {
			value: { baseItems: [{ id: 'cached', title: 'Cached' }], families: [] },
			expiry: SYNC_TEST_NOW + 60_000
		});
		const store = system.createStore();
		try {
			expect(system.fetchRequests).toHaveLength(0);
			expect(system.cache.reads).toEqual([cacheKey]);
			const restored = await store.restore();
			expect(restored.isOk()).toBe(true);
			expect(store.items()).toEqual([{ id: 'cached', title: 'Cached' }]);

			// The constructor's cache read can finish after a read applied; it is older than that data.
			system.database.seed('workspace-1', [{ id: 'cached', title: 'Fresh' }]);
			const lateCache = new Deferred();
			const get = system.cache.get.bind(system.cache);
			vi.spyOn(system.cache, 'get').mockImplementationOnce(async (key) => {
				await lateCache.promise;
				return get(key);
			});
			const lateStore = system.createStore();
			try {
				expect((await lateStore.refresh()).isOk()).toBe(true);
				expect((await lateStore.hydrate()).isOk()).toBe(true);
				lateCache.resolve();
				await nextMacrotask();
				expect(lateStore.items()).toEqual([{ id: 'cached', title: 'Fresh' }]);
			} finally {
				lateStore.dispose();
			}
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('refetches instead of restoring a snapshot saved in an earlier format', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Fresh' }]);
		await system.cache.set('sync-resource:notes:{"workspaceId":"workspace-1"}:view', {
			value: {
				items: [{ id: 'old', title: 'Old' }],
				baseItems: [{ id: 'old', title: 'Old' }],
				families: [{ key: 'q', query: {}, pages: [{ ids: ['old'], complete: true }] }],
				pages: []
			},
			expiry: SYNC_TEST_NOW + 60_000
		});
		const store = system.createStore();
		try {
			expect((await store.hydrate()).isOk()).toBe(true);
			expect(store.items()).toEqual([{ id: 'note-1', title: 'Fresh' }]);
			expect(store.error()).toBeNull();
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('hydrates once, shares concurrent calls, and reuses successful hydration', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		try {
			const first = store.hydrate();
			const second = store.hydrate();
			const results = await Promise.all([first, second]);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(system.fetchRequests).toHaveLength(1);
			const requestCount = system.fetchRequests.length;
			expect((await store.hydrate()).isOk()).toBe(true);
			expect(system.fetchRequests).toHaveLength(requestCount);
			expect(store.items()).toEqual([{ id: 'note-1', title: 'Initial' }]);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('never restores a cache entry over data a read or a mutation already applied', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Hydrated' }]);
		const store = system.createStore();
		const snapshots = recordTitles(store);
		try {
			// A reconnect hydrate, even after a failed read, keeps the held data instead of an older cache entry.
			expect((await store.hydrate()).isOk()).toBe(true);
			await cacheNote(system, 'Older');
			system.database.seed('workspace-1', [{ id: 'note-1', title: 'Newer' }]);
			vi.spyOn(system.manager.http, 'list').mockResolvedValueOnce(new Response('boom', { status: 500 }));
			await system.transport.reconnect(true);
			await waitFor(() => !store.isHydrating());
			expect(store.error()).not.toBeNull();
			await system.transport.reconnect(true);
			await waitFor(() => !store.isHydrating() && store.items()[0]?.title === 'Newer');
			expect(snapshots.flat()).not.toContain('Older');
			expect(system.cache.reads).toHaveLength(1);
		} finally {
			store.dispose();
		}

		// A mutation applied while the first read failed also makes the state newer than the cache.
		await cacheNote(system, 'Older');
		const second = system.createStore();
		const secondSnapshots = recordTitles(second);
		try {
			vi.spyOn(system.manager.http, 'list').mockResolvedValueOnce(new Response('boom', { status: 500 }));
			expect((await second.hydrate()).isErr()).toBe(true);
			expect((await second.add({ input: { id: 'note-2', title: 'Mutated' } })).isOk()).toBe(true);
			expect((await second.connect()).isOk()).toBe(true);
			const seen = secondSnapshots.length;
			await system.transport.reconnect(true);
			await waitFor(() => !second.isHydrating());
			expect(secondSnapshots.length).toBeGreaterThan(seen);
			expect(secondSnapshots.slice(seen).every((titles) => titles.includes('Mutated'))).toBe(true);
			expect(second.items().map((note) => note.title)).toContain('Mutated');
		} finally {
			second.dispose();
			await system.transport.close();
		}
	});
});
