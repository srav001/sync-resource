import { afterEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/index.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('direct manager SSE', () => {
	it('opens with a ready event', async () => {
		const system = createSyncTestSystem();
		const response = await system.manager.http.events?.({
			request: new Request('http://sync.test/events', { headers: { accept: 'text/event-stream' } }),
			params: { workspaceId: 'workspace-1' }
		});
		expect(response?.ok).toBe(true);
		const reader = response?.body?.getReader();
		const first = await reader?.read();
		expect(first?.done).toBe(false);
		const event = parseSseEvent(new TextDecoder().decode(first?.value));
		expect(event.eventName).toBe('ready');
		expect(JSON.parse(event.data)).toMatchObject({ scope: 'notes:workspace-1' });
		await reader?.cancel();
	});

	it('delivers committed writes to a live subscriber', async () => {
		const system = createSyncTestSystem();
		await system.transport.subscribe({
			url: 'http://sync.test/events',
			signal: new AbortController().signal,
			onEnvelope: () => {}
		});
		const result = await system.manager.bind({ workspaceId: 'workspace-1' }).add({
			input: { id: 'note-1', title: 'One' }
		});
		expect(result.isOk()).toBe(true);
		await system.transport.waitForEnvelopeCount(1);
		expect(system.transport.received[0]?.changes[0]).toMatchObject({ type: 'itemAdded', id: 'note-1' });
		expect(system.persistence.retained('notes:workspace-1')).toHaveLength(1);
		await system.transport.close();
	});

	it('removes the subscriber when the request is aborted', async () => {
		const system = createSyncTestSystem();
		await system.transport.subscribe({
			url: 'http://sync.test/events',
			signal: new AbortController().signal,
			onEnvelope: () => {}
		});
		await system.transport.close();
		await system.manager.bind({ workspaceId: 'workspace-1' }).add({
			input: { id: 'after-close', title: 'After close' }
		});
		await Promise.resolve();
		expect(system.transport.received).toHaveLength(0);
	});

	it('replays envelopes after a cursor and emits a reset after retention loss', async () => {
		const system = createSyncTestSystem();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		const firstResponse = await system.manager.http.events?.({
			request: new Request('http://sync.test/events'),
			params: { workspaceId: 'workspace-1' }
		});
		const firstReader = firstResponse?.body?.getReader();
		await firstReader?.read();
		await bound.add({ input: { id: 'note-1', title: 'One' } });
		const firstEvent = parseSseEvent(new TextDecoder().decode((await firstReader?.read())?.value));
		const cursor = JSON.parse(firstEvent.data).cursor as string;
		expect(cursor).toBeTruthy();
		await firstReader?.cancel();
		await bound.add({ input: { id: 'note-2', title: 'Two' } });
		const replayResponse = await system.manager.http.events?.({
			request: new Request(`http://sync.test/events?after=${encodeURIComponent(cursor)}`),
			params: { workspaceId: 'workspace-1' }
		});
		const replayReader = replayResponse?.body?.getReader();
		await replayReader?.read();
		const replayEvent = parseSseEvent(new TextDecoder().decode((await replayReader?.read())?.value));
		const replayEnvelope = JSON.parse(replayEvent.data);
		expect(replayEvent.eventName).toBe('sync');
		expect(replayEnvelope).toMatchObject({
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			cursor: system.persistence.retained('notes:workspace-1')[1]?.cursor,
			changes: [{ type: 'itemAdded', id: 'note-2', value: { id: 'note-2', title: 'Two' } }]
		});
		await replayReader?.cancel();
		system.persistence.setRetention(1);
		await bound.add({ input: { id: 'note-3', title: 'Three' } });
		const resetResponse = await system.manager.http.events?.({
			request: new Request(`http://sync.test/events?after=${encodeURIComponent(cursor)}`),
			params: { workspaceId: 'workspace-1' }
		});
		const resetReader = resetResponse?.body?.getReader();
		await resetReader?.read();
		const resetEvent = parseSseEvent(new TextDecoder().decode((await resetReader?.read())?.value));
		expect(JSON.parse(resetEvent.data)).toMatchObject({
			scope: 'notes:workspace-1',
			changes: [
				{
					type: 'reset',
					manifest: {
						reason: 'retention_gap',
						previousCursor: cursor
					}
				}
			]
		});
		await resetReader?.cancel();
	});
});
