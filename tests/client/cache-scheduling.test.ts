import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/index.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

beforeEach(() => {
	vi.useFakeTimers();
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('authoritative cache scheduling', () => {
	it('uses a trailing 50ms write after hydration', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		try {
			await store.hydrate();
			expect(system.cache.writes).toHaveLength(0);
			await vi.advanceTimersByTimeAsync(49);
			expect(system.cache.writes).toHaveLength(0);
			await vi.advanceTimersByTimeAsync(1);
			expect(system.cache.writes).toHaveLength(1);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('flushes pending authoritative cache state on dispose', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		await store.hydrate();
		store.dispose();
		await vi.runAllTimersAsync();
		expect(system.cache.writes.length).toBeGreaterThan(0);
		await system.transport.close();
	});
});
