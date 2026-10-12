import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	manager,
	MemoryManagerOutbox,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	resource,
	type ManagerOutbox,
	type ManagerOutboxRead,
	type OperationExecution,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import { openSharedStream } from '../fixtures/sharedStream.ts';

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

async function replayed(outbox: MemoryManagerOutbox, afterCursor: string | undefined, until: string) {
	registerSharedStreamManager({ key: 'notes' });
	await registerSharedStreamSubscription({
		request: new Request('http://x/connect', { method: 'POST', headers: { 'x-sync-transport-id': 't' } }),
		managerKey: 'notes',
		scope: SCOPE,
		afterCursor,
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
	while (!seen.includes(until)) {
		const event = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
		if (event.eventName === 'sync') {
			seen.push(JSON.parse(event.data).cursor);
		}
	}
	await reader.cancel();
	return seen;
}

describe('read sync cursor', () => {
	it('gives a read of an empty scope the origin cursor, which replays the first envelope of the scope', async () => {
		const outbox = new MemoryManagerOutbox();
		const { envelope } = await list(outbox, () => outbox.append(env('c1')));
		expect(envelope?.cursor).toMatch(/^origin:/);
		expect(await replayed(outbox, envelope?.cursor, 'c1')).toEqual(['c1']);
	});

	it('resets an origin cursor once the scope may have been trimmed, or from another outbox', async () => {
		const outbox = new MemoryManagerOutbox(2);
		const origin = outbox.latestCursor(SCOPE);
		outbox.append(env('c1'));
		expect(outbox.readAfter(SCOPE, origin, 10)).toMatchObject({ cursorFound: true, envelopes: [{ cursor: 'c1' }] });
		outbox.append(env('c2'));
		expect(outbox.readAfter(SCOPE, origin, 10).cursorFound).toBe(false);
		expect(new MemoryManagerOutbox().readAfter(SCOPE, origin, 10).cursorFound).toBe(false);
	});

	it('attaches the latest outbox cursor to a get read', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		const notes = resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
			get: method.get({
				output: { parse: (value: unknown) => value as { id: string } },
				handler: ({ ctx }) => ctx.ok({ id: 'ws' })
			})
		}));
		const notesManager = manager({
			key: 'notes',
			resource: notes,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			outbox
		});
		const response = await notesManager.http.get!({
			request: new Request('http://sync.test/get'),
			params: { workspaceId: 'ws' }
		});
		const body = (await response.json()) as { envelope?: SyncEnvelope };
		expect(body.envelope).toMatchObject({ cursor: 'c1', changes: [] });
	});

	it('attaches the latest outbox cursor to a list read without a resource cursor', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		outbox.append(env('c2'));
		const body = await list(outbox);
		expect(body.envelope).toEqual({ managerKey: 'notes', scope: 'notes:ws', cursor: 'c2', changes: [] });
	});

	it('captures the cursor before the read so replay covers a commit during the read', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		const { envelope } = await list(outbox, () => outbox.append(env('c2')));
		expect(envelope?.cursor).toBe('c1');
		expect(await replayed(outbox, envelope?.cursor, 'c2')).toEqual(['c2']);
	});
});

const noteOutput = { parse: (value: unknown) => value as { id: string } };
const noteResource = resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
	get: method.get({ output: noteOutput, handler: ({ ctx }) => ctx.ok({ id: 'ws' }) }),
	list: method.list({
		output: { parse: (value: unknown) => value as { items: readonly { id: string }[] } },
		handler: ({ ctx }) => ctx.ok({ items: [{ id: 'n1' }] })
	}),
	add: method.add({
		input: noteOutput,
		output: noteOutput,
		handler: ({ input, ctx }) =>
			ctx.ok({ output: input, changes: [{ type: 'itemAdded' as const, id: input.id, value: input }] })
	})
}));

function plainManager(outbox: ManagerOutbox, authorize: (...args: unknown[]) => boolean = () => true) {
	return manager({
		key: 'notes',
		resource: noteResource,
		authorize: (_context, _params, execution) => authorize(execution),
		scope: (params) => params.workspaceId,
		outbox
	});
}

const params = { workspaceId: 'ws' };
const emptyRead: ManagerOutboxRead = { envelopes: [], cursorFound: true, retainedEnvelopeCount: 0 };

describe('custom outbox read cursor', () => {
	it('uses the cursor of a plain outbox that implements latestCursor for list reads and the page cursor', async () => {
		const notes = plainManager({ append: () => {}, readAfter: () => emptyRead, latestCursor: () => 'custom:7' });
		const response = await notes.http.list!({ request: new Request('http://sync.test/list'), params });
		const { envelope } = (await response.json()) as { envelope?: SyncEnvelope };
		expect(envelope).toEqual({ managerKey: 'notes', scope: 'notes:ws', cursor: 'custom:7', changes: [] });
	});

	it('keeps the cursor a list resource returns over the outbox cursor, with no page in the envelope', async () => {
		const outbox = new MemoryManagerOutbox();
		outbox.append(env('c1'));
		const pageManager = (output: { items: readonly { id: string }[]; syncCursor?: string }, syncCursor?: string) =>
			manager({
				key: 'notes',
				resource: resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
					list: method.list({
						output: { parse: (value: unknown) => value as typeof output },
						handler: ({ ctx }) => ctx.ok({ output, syncCursor })
					})
				})),
				authorize: () => true,
				scope: (scopeParams) => scopeParams.workspaceId,
				outbox
			});
		const read = async (notes: ReturnType<typeof pageManager>) => {
			const response = await notes.http.list!({ request: new Request('http://sync.test/list'), params });
			return ((await response.json()) as { envelope?: SyncEnvelope }).envelope;
		};
		const items = [{ id: 'n1' }];
		expect(await read(pageManager({ items }, 'resource:1'))).toEqual({
			managerKey: 'notes',
			scope: 'notes:ws',
			cursor: 'resource:1',
			changes: []
		});
		expect((await read(pageManager({ items, syncCursor: 'page:2' })))?.cursor).toBe('page:2');
		expect((await read(pageManager({ items })))?.cursor).toBe('c1');
	});

	it('keeps explicit changes of a list read in its envelope', async () => {
		const notes = manager({
			key: 'notes',
			resource: resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
				list: method.list({
					output: { parse: (value: unknown) => value as { items: readonly { id: string }[] } },
					handler: ({ ctx }) =>
						ctx.ok({
							output: { items: [{ id: 'n1' }] },
							changes: [{ type: 'pageLoaded', items: [{ id: 'n1' }], syncCursor: 'explicit:1' }]
						})
				})
			})),
			authorize: () => true,
			scope: (scopeParams) => scopeParams.workspaceId,
			outbox: new MemoryManagerOutbox()
		});
		const response = await notes.http.list!({ request: new Request('http://sync.test/list'), params });
		const { envelope } = (await response.json()) as { envelope?: SyncEnvelope };
		expect(envelope).toMatchObject({
			cursor: 'explicit:1',
			changes: [{ type: 'pageLoaded', items: [{ id: 'n1' }] }]
		});
	});

	it('invents no cursor when a plain outbox cannot report one', async () => {
		const notes = plainManager({ append: () => {}, readAfter: () => emptyRead });
		for (const method of ['list', 'get'] as const) {
			const response = await notes.http[method]!({ request: new Request(`http://sync.test/${method}`), params });
			expect(response.status).toBe(200);
			const body = (await response.json()) as { value: unknown; envelope?: SyncEnvelope };
			expect(body.value).toBeDefined();
			expect(body.envelope).toBeUndefined();
		}
	});
});

describe('operation execution', () => {
	const calls: Record<string, OperationExecution | undefined> = {};
	const recordingOutbox: ManagerOutbox = {
		append: (_envelope, execution) => {
			calls.append = execution;
		},
		readAfter: (_scope, _cursor, _limit, execution) => {
			calls.readAfter = execution;
			return emptyRead;
		},
		latestCursor: (_scope, execution) => {
			calls.latestCursor = execution;
			return 'c1';
		}
	};
	const abort = new AbortController();
	const request = (url: string, init?: RequestInit) => new Request(url, { ...init, signal: abort.signal });
	const signalOf = (execution: OperationExecution | undefined) => {
		expect(execution?.signal).toBeInstanceOf(AbortSignal);
		return execution!.signal;
	};

	it('reaches the authorize hook and the outbox latestCursor, append and readAfter', async () => {
		let authorized: OperationExecution | undefined;
		const notes = plainManager(recordingOutbox, (execution) => {
			authorized = execution as OperationExecution;
			return true;
		});
		await notes.http.list!({ request: request('http://sync.test/list'), params });
		const added = await notes.http.add!({
			request: request('http://sync.test/add', { method: 'POST', body: JSON.stringify({ input: { id: 'n2' } }) }),
			params
		});
		expect(added.status).toBe(200);
		const reader = await openSharedStream(notes.http, params, { afterCursor: 'c1', signal: abort.signal });
		await vi.waitFor(() => expect(calls).toHaveProperty('readAfter'));
		await reader.cancel();

		const signals = [authorized, calls.latestCursor, calls.append, calls.readAfter].map(signalOf);
		abort.abort();
		expect(signals.every((signal) => signal.aborted)).toBe(true);
	});

	it('reaches the outbox append when the write carries a mutation id', async () => {
		let appended: OperationExecution | undefined;
		const notes = plainManager({
			append: (_envelope, execution) => {
				appended = execution;
			},
			readAfter: () => emptyRead
		});
		const added = await notes.http.add!({
			request: new Request('http://sync.test/add?mutationId=m1', {
				method: 'POST',
				body: JSON.stringify({ input: { id: 'n3' } })
			}),
			params
		});
		expect(added.status).toBe(200);
		expect(signalOf(appended)).toBeInstanceOf(AbortSignal);
	});
});
