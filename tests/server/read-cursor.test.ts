import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	manager,
	MemoryManagerOutbox,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	resource,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';

afterEach(() => resetSharedStreamForTests());

const SCOPE = 'notes:ws';
const env = (cursor: string): SyncEnvelope => ({
	managerKey: 'notes',
	scope: SCOPE,
	cursor,
	changes: [{ type: 'itemAdded', id: cursor, value: { id: cursor } }]
});
const list = async (outbox: MemoryManagerOutbox, duringRead?: () => void) => {
	const notes = resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
		list: method.list({
			query: { parse: (value: unknown) => value as { limit: number } },
			output: { parse: (value: unknown) => value as { items: readonly { id: string }[] } },
			handler: ({ ctx }) => {
				duringRead?.();
				return ctx.ok({ items: [{ id: 'n1' }] });
			}
		})
	}));
	const notesManager = manager({
		key: 'notes',
		resource: notes,
		authorize: () => true,
		scope: (params) => params.workspaceId,
		outbox
	});
	const response = await notesManager.http.list!({
		request: new Request('http://sync.test/list?query=%7B%22limit%22%3A20%7D'),
		params: { workspaceId: 'ws' }
	});
	return (await response.json()) as { value: unknown; envelope?: SyncEnvelope };
};

describe('read sync cursor', () => {
	it('returns no cursor for a read on an empty scope', async () => {
		const body = await list(new MemoryManagerOutbox());
		expect(body.envelope).toBeUndefined();
	});

	it('attaches the latest outbox cursor to a read without a resource cursor', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		outbox.append(env('c2'));
		const body = await list(outbox);
		expect(body.envelope?.cursor).toBe('c2');
		expect(body.envelope?.changes[0]).toMatchObject({ type: 'pageLoaded', syncCursor: 'c2' });
	});

	it('captures the cursor before the read so replay covers a commit during the read', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		const { envelope } = await list(outbox, () => outbox.append(env('c2')));
		expect(envelope?.cursor).toBe('c1');

		registerSharedStreamManager({ key: 'notes' });
		const connect = new Request('http://x/connect', { method: 'POST', headers: { 'x-sync-transport-id': 't' } });
		await registerSharedStreamSubscription({
			request: connect,
			managerKey: 'notes',
			scope: SCOPE,
			afterCursor: envelope?.cursor,
			replayLimit: 100,
			persistence: outbox,
			nextCursor: () => 'reset'
		});
		const stream = await httpSharedSyncStream(
			new Request('http://x/stream', {
				method: 'POST',
				headers: { 'x-sync-transport-id': 't', 'x-forwarded-for': '1.1.1.1' }
			})
		);
		const reader = stream.body!.getReader();
		const seen: string[] = [];
		while (!seen.includes('c2')) {
			const event = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
			if (event.eventName === 'sync') {
				seen.push(JSON.parse(event.data).cursor);
			}
		}
		expect(seen).toEqual(['c2']);
		await reader.cancel();
	});
});
