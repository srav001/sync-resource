import { afterEach, beforeEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { createSyncTestSystem, SYNC_TEST_NOW } from '../fixtures/syncSystem.ts';

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

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
		await system.cache.set('sync-resource:notes:{"workspaceId":"workspace-1"}:view:v2', {
			value: { baseItems: [{ id: 'cached', title: 'Cached' }], families: [] },
			expiry: SYNC_TEST_NOW + 60_000
		});
		const store = system.createStore();
		try {
			expect(system.fetchRequests).toHaveLength(0);
			expect(system.cache.reads.length).toBeGreaterThan(0);
			const restored = await store.restore();
			expect(restored.isOk()).toBe(true);
			expect(store.items()).toEqual([{ id: 'cached', title: 'Cached' }]);
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
});
