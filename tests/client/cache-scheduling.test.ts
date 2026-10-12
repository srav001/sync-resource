import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
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
	it('flushes pending authoritative cache state on dispose and cancels the scheduled write', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		await store.hydrate();
		expect(system.cache.writes).toHaveLength(0);

		store.dispose();
		expect(system.cache.writes).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(250);
		expect(system.cache.writes).toHaveLength(1);
		await system.transport.close();
	});
});
