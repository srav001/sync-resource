import { afterEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { manager, resource, type ManagerOutboxRead, type SyncEnvelope } from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
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

	describe('replay from a custom outbox', () => {
		const retained = (cursor: string): SyncEnvelope => ({
			managerKey: 'notes',
			scope: 'notes:ws',
			cursor,
			changes: [{ type: 'itemAdded', id: cursor, value: { id: cursor } }]
		});

		async function replayEvents(read: (limit: number) => ManagerOutboxRead) {
			const limits: number[] = [];
			const notes = manager({
				key: 'notes',
				resource: resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
					get: method.get({
						output: { parse: (value: unknown) => value as { id: string } },
						handler: ({ ctx }) => ctx.ok({ id: 'ws' })
					})
				})),
				authorize: () => true,
				scope: (params) => params.workspaceId,
				replayLimit: 2,
				outbox: {
					append: () => {},
					readAfter: (_scope, _cursor, limit) => {
						limits.push(limit);
						return read(limit);
					}
				}
			});
			const response = await notes.http.events!({
				request: new Request('http://sync.test/events?after=old'),
				params: { workspaceId: 'ws' }
			});
			const reader = response.body!.getReader();
			await reader.read();
			const event = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
			await reader.cancel();
			return { limits, envelope: JSON.parse(event.data) as SyncEnvelope };
		}

		it('resets an unknown cursor even when the outbox retains nothing', async () => {
			const { envelope } = await replayEvents(() => ({
				envelopes: [],
				cursorFound: false,
				retainedEnvelopeCount: 0
			}));
			expect(envelope.changes).toMatchObject([{ type: 'reset', manifest: { reason: 'retention_gap' } }]);
		});

		it('reads one envelope past the replay limit and resets instead of sending a truncated replay', async () => {
			const all = ['c1', 'c2', 'c3', 'c4'].map(retained);
			const { limits, envelope } = await replayEvents((limit) => ({
				envelopes: all.slice(0, limit),
				cursorFound: true,
				retainedEnvelopeCount: all.length
			}));
			expect(limits).toEqual([3]);
			expect(envelope.changes).toMatchObject([{ type: 'reset', manifest: { reason: 'retention_gap' } }]);
		});

		it('replays without a reset when the pending envelopes exactly fill the replay limit', async () => {
			const all = ['c1', 'c2'].map(retained);
			const { envelope } = await replayEvents((limit) => ({
				envelopes: all.slice(0, limit),
				cursorFound: true,
				retainedEnvelopeCount: all.length
			}));
			expect(envelope.cursor).toBe('c1');
		});
	});
});
