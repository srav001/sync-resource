import { afterEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { manager, resource, type ManagerOutboxRead, type SyncEnvelope } from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { openSharedStream, readSyncEnvelope } from '../fixtures/sharedStream.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('manager on the shared stream', () => {
	it('delivers committed writes to a live subscriber', async () => {
		const system = createSyncTestSystem();
		await system.transport.subscribe({
			url: 'http://sync.test/connect',
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
			url: 'http://sync.test/connect',
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
		const params = { workspaceId: 'workspace-1' };
		const bound = system.manager.bind(params);
		const firstReader = await openSharedStream(system.manager.http, params);
		await bound.add({ input: { id: 'note-1', title: 'One' } });
		const { cursor } = await readSyncEnvelope(firstReader);
		expect(cursor).toBeTruthy();
		await firstReader.cancel();
		await bound.add({ input: { id: 'note-2', title: 'Two' } });
		const replayReader = await openSharedStream(system.manager.http, params, { afterCursor: cursor });
		expect(await readSyncEnvelope(replayReader)).toMatchObject({
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			cursor: system.persistence.retained('notes:workspace-1')[1]?.cursor,
			changes: [{ type: 'itemAdded', id: 'note-2', value: { id: 'note-2', title: 'Two' } }]
		});
		await replayReader.cancel();
		system.persistence.setRetention(1);
		await bound.add({ input: { id: 'note-3', title: 'Three' } });
		const resetReader = await openSharedStream(system.manager.http, params, { afterCursor: cursor });
		expect(await readSyncEnvelope(resetReader)).toMatchObject({
			scope: 'notes:workspace-1',
			changes: [{ type: 'reset', manifest: { reason: 'retention_gap', previousCursor: cursor } }]
		});
		await resetReader.cancel();
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
			const reader = await openSharedStream(notes.http, { workspaceId: 'ws' }, { afterCursor: 'old' });
			const envelope = await readSyncEnvelope(reader);
			await reader.cancel();
			return { limits, envelope };
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
