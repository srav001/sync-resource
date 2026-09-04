import { afterEach, describe, expect, it } from 'vite-plus/test';

import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { createSyncTestSystem, type SyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => resetSharedStreamForTests());

describe('manager execution and HTTP', () => {
	it('infers add/mutate/delete changes and persists envelopes', async () => {
		const system: SyncTestSystem = createSyncTestSystem();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		const added = await bound.add(
			{ input: { id: 'n1', title: 'one' } },
			{ mutationId: 'm1', meta: { clientId: 'c1' } }
		);
		expect(added.isOk()).toBe(true);
		expect(system.persistence.mutationWrites[0]?.envelope?.changes[0]).toMatchObject({
			type: 'itemAdded',
			id: 'n1'
		});
		expect(system.persistence.mutationWrites[0]?.envelope?.sourceClientId).toBe('c1');
		expect(system.database.operations).toEqual(['add:workspace-1:n1']);
		const mutated = await bound.mutate({ query: { id: 'n1' }, input: { title: 'two' } }, { mutationId: 'm2' });
		expect(mutated.isOk()).toBe(true);
		expect(system.persistence.mutationWrites[1]?.envelope?.changes[0]).toMatchObject({
			type: 'itemUpdated',
			id: 'n1',
			patch: { title: 'two' }
		});
		const deleted = await bound.delete({ query: { id: 'n1' } }, { mutationId: 'm3' });
		expect(deleted.isOk()).toBe(true);
		expect(system.persistence.mutationWrites[2]?.envelope?.changes[0]).toMatchObject({
			type: 'itemDeleted',
			id: 'n1'
		});
	});

	it('parses HTTP query/body and returns protocol result/status', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'n1', title: 'one' }]);
		const listResponse = await system.manager.http.list!({
			request: new Request('http://sync.test/list?query=%7B%22limit%22%3A20%7D'),
			params: { workspaceId: 'workspace-1' }
		});
		expect(listResponse.status).toBe(200);
		expect(await listResponse.json()).toMatchObject({ isOk: true, value: { items: [{ id: 'n1' }] } });
		const bad = await system.manager.http.add!({
			request: new Request('http://sync.test/add', { method: 'POST', body: '{bad' }),
			params: { workspaceId: 'workspace-1' }
		});
		expect(bad.status).toBe(400);
		expect((await bad.json()).error.code).toBe('bad_request');
	});

	it('serves replay through the diagnostic SSE endpoint', async () => {
		const system = createSyncTestSystem();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		await bound.add({ input: { id: 'n1', title: 'one' } }, { mutationId: 'm1' });
		const cursor = system.persistence.retained('notes:workspace-1')[0]!.cursor;
		await bound.add({ input: { id: 'n2', title: 'two' } }, { mutationId: 'm2' });
		const response = await system.manager.http.events!({
			request: new Request(`http://sync.test/events?after=${encodeURIComponent(cursor)}`),
			params: { workspaceId: 'workspace-1' }
		});
		expect(response.headers.get('content-type')).toContain('text/event-stream');
		const reader = response.body!.getReader();
		const first = await reader.read();
		const text = new TextDecoder().decode(first.value);
		expect(text).toContain('event: ready');
		await reader.cancel();
	});
});
