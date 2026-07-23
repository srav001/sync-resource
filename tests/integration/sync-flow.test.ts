import { afterEach, beforeEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/index.ts';
import { createSyncTestSystem, type Note } from '../fixtures/syncSystem.ts';

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('authoritative synchronization flow', () => {
	it('hydrates, commits optimistic writes, replays missed envelopes, and repairs a retention gap', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();

		try {
			const hydration = await store.hydrate();
			expect(hydration.isOk()).toBe(true);
			expect(store.items()).toEqual([{ id: 'note-1', title: 'Initial' }]);
			expect(system.database.operations).toEqual(['list:workspace-1']);

			const commit = system.database.pauseNextCommit();
			const addPromise = store.add({
				input: { id: 'note-2', title: 'Optimistic' }
			});
			await Promise.resolve();
			expect(store.items()).toEqual([
				{ id: 'note-1', title: 'Initial' },
				{ id: 'note-2', title: 'Optimistic' }
			]);
			expect(store.isPending()).toBe(true);
			expect(system.database.read('workspace-1', 'note-2')).toBeUndefined();

			commit.resolve(undefined);
			const addResult = await addPromise;
			expect(addResult.isOk()).toBe(true);
			await system.transport.waitForEnvelopeCount(1);
			expect(system.database.read('workspace-1', 'note-2')).toEqual({
				id: 'note-2',
				title: 'Optimistic'
			});
			expect(system.persistence.mutationWrites).toHaveLength(1);
			expect(system.persistence.envelopeWrites).toHaveLength(1);
			expect(system.transport.received[0]?.changes).toEqual([
				{
					type: 'itemAdded',
					id: 'note-2',
					value: { id: 'note-2', title: 'Optimistic' }
				}
			]);
			expect(store.isPending()).toBe(false);

			system.transport.disconnect();
			const bound = system.manager.bind({ workspaceId: 'workspace-1' });
			const missedOne = await bound.add(
				{ input: { id: 'note-3', title: 'Missed one' } },
				{ mutationId: 'server-mutation-1' }
			);
			const missedTwo = await bound.add(
				{ input: { id: 'note-4', title: 'Missed two' } },
				{ mutationId: 'server-mutation-2' }
			);
			expect(missedOne.isOk()).toBe(true);
			expect(missedTwo.isOk()).toBe(true);
			expect(store.items().map((note) => note.id)).toEqual(['note-1', 'note-2']);

			await system.transport.reconnect();
			await system.transport.waitForEnvelopeCount(3);
			expect(store.items().map((note) => note.id)).toEqual(['note-1', 'note-2', 'note-3', 'note-4']);
			expect(system.transport.received.slice(1, 3).map((envelope) => envelope.changes[0]?.type)).toEqual([
				'itemAdded',
				'itemAdded'
			]);

			system.transport.disconnect();
			system.persistence.setRetention(1);
			const retainedOne = await bound.add(
				{ input: { id: 'note-5', title: 'Retention one' } },
				{ mutationId: 'server-mutation-3' }
			);
			const retainedTwo = await bound.add(
				{ input: { id: 'note-6', title: 'Retention two' } },
				{ mutationId: 'server-mutation-4' }
			);
			expect(retainedOne.isOk()).toBe(true);
			expect(retainedTwo.isOk()).toBe(true);
			expect(system.persistence.retained('notes:workspace-1')).toHaveLength(1);

			await system.transport.reconnect();
			await system.transport.waitForEnvelopeCount(4);
			const resetEnvelope = system.transport.received[3];
			expect(resetEnvelope?.reset).toMatchObject({
				scope: 'notes:workspace-1',
				reason: 'retention_gap'
			});
			const repair = await store.repair();
			expect(repair.isOk()).toBe(true);
			expect(store.items()).toEqual(system.database.list('workspace-1'));

			store.dispose();
			await Promise.resolve();
			const cacheKey = 'sync-resource:notes:{"workspaceId":"workspace-1"}:view';
			expect(system.cache.peek(cacheKey)?.value).toBeDefined();
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('rolls back an optimistic write when the authoritative database commit fails', async () => {
		const system = createSyncTestSystem();
		const initial: readonly Note[] = [{ id: 'note-1', title: 'Initial' }];
		system.database.seed('workspace-1', initial);
		const store = system.createStore();

		try {
			expect((await store.hydrate()).isOk()).toBe(true);
			system.database.failNextCommit('database unavailable');
			const result = await store.mutate({
				query: { id: 'note-1' },
				input: { title: 'Optimistic title' }
			});

			expect(result.isErr()).toBe(true);
			if (result.isErr()) {
				expect(result.error.message).toBe('database unavailable');
				expect(result.error.recovery).toEqual({
					restored: true,
					source: 'memory'
				});
			}
			expect(store.items()).toEqual(initial);
			expect(system.database.read('workspace-1', 'note-1')).toEqual(initial[0]);
			expect(system.persistence.mutationWrites).toHaveLength(0);
			expect(system.persistence.envelopeWrites).toHaveLength(0);
			expect(store.isPending()).toBe(false);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});
});
