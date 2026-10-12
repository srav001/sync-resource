import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { manager, resource, type ManagerOutboxRead } from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { openSharedStream, readCursorsUntil, readSyncEnvelope, streamEnvelope } from '../fixtures/sharedStream.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('manager on the shared stream', () => {
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

	it('drops the envelope history of an idle scope except its newest envelope', async () => {
		vi.useFakeTimers();
		const note = { parse: (value: unknown) => value as { id: string } };
		const notes = manager({
			key: 'notes',
			resource: resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
				add: method.add({ input: note, output: note, handler: ({ input, ctx }) => ctx.ok(input) })
			})),
			authorize: () => true,
			scope: (params) => params.workspaceId
		});
		const params = { workspaceId: 'ws' };
		const bound = notes.bind(params);
		const first = await openSharedStream(notes.http, params);
		const cursors: string[] = [];
		for (const id of ['n1', 'n2', 'n3']) {
			await bound.add({ input: { id } });
			cursors.push((await readSyncEnvelope(first)).cursor);
		}
		await first.cancel();
		await vi.advanceTimersByTimeAsync(5 * 60_000);
		vi.useRealTimers();

		const current = await openSharedStream(notes.http, params, { afterCursor: cursors[2] });
		await bound.add({ input: { id: 'n4' } });
		expect((await readSyncEnvelope(current)).changes).toMatchObject([{ type: 'itemAdded', id: 'n4' }]);
		await current.cancel();

		const stale = await openSharedStream(notes.http, params, { afterCursor: cursors[0] });
		expect((await readSyncEnvelope(stale)).changes).toMatchObject([
			{ type: 'reset', manifest: { reason: 'retention_gap', previousCursor: cursors[0] } }
		]);
		await stale.cancel();
	});

	describe('replay from a custom outbox', () => {
		async function replayReader(read: (limit: number) => ManagerOutboxRead) {
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
			return { limits, reader };
		}

		it('resets an unknown cursor even when the outbox retains nothing', async () => {
			const { reader } = await replayReader(() => ({
				envelopes: [],
				cursorFound: false,
				retainedEnvelopeCount: 0
			}));
			expect((await readSyncEnvelope(reader)).changes).toMatchObject([
				{ type: 'reset', manifest: { reason: 'retention_gap' } }
			]);
		});

		it('reads one envelope past the replay limit and resets instead of sending a truncated replay', async () => {
			const all = ['c1', 'c2', 'c3', 'c4'].map((cursor) => streamEnvelope(cursor, 'notes:ws'));
			const { limits, reader } = await replayReader((limit) => ({
				envelopes: all.slice(0, limit),
				cursorFound: true,
				retainedEnvelopeCount: all.length
			}));
			expect((await readSyncEnvelope(reader)).changes).toMatchObject([
				{ type: 'reset', manifest: { reason: 'retention_gap' } }
			]);
			expect(limits).toEqual([3]);
		});

		it('replays without a reset when the pending envelopes exactly fill the replay limit', async () => {
			const all = ['c1', 'c2'].map((cursor) => streamEnvelope(cursor, 'notes:ws'));
			const { reader } = await replayReader((limit) => ({
				envelopes: all.slice(0, limit),
				cursorFound: true,
				retainedEnvelopeCount: all.length
			}));
			expect(await readCursorsUntil(reader, 'c2')).toEqual(['c1', 'c2']);
		});
	});
});
