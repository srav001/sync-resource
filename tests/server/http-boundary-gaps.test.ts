import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	manager,
	ok,
	resource,
	type ManagerMutationRecord,
	type ManagerOutboxRead,
	type ManagerRealtimeBus,
	type ManagerSyncPersistence,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';

const paramsSchema = {
	parse(value: unknown): { workspaceId: string } {
		if (
			!value ||
			typeof value !== 'object' ||
			typeof (value as { workspaceId?: unknown }).workspaceId !== 'string'
		) {
			throw new Error('invalid params');
		}
		return value as { workspaceId: string };
	}
};

const recordSchema = {
	parse(value: unknown): Record<string, unknown> {
		if (!value || typeof value !== 'object' || Array.isArray(value)) {
			throw new Error('invalid record');
		}
		return value as Record<string, unknown>;
	}
};

afterEach(() => resetSharedStreamForTests());

describe('manager authorization boundaries', () => {
	it('denies direct and HTTP calls before the resource handler runs', async () => {
		let handlerCalls = 0;
		const reported: { code: string; method: string; scope: string }[] = [];
		const repository = resource(paramsSchema, (method) => ({
			get: method.get({
				output: recordSchema,
				handler() {
					handlerCalls += 1;
					return ok({ value: true });
				}
			})
		}));
		const notes = manager({
			key: 'authorization-denial',
			resource: repository,
			authorize: () => false,
			scope: (params) => params.workspaceId,
			handleError(error, context) {
				reported.push({ code: error.code, method: context.method, scope: context.scope });
			}
		});

		const direct = await notes.bind({ workspaceId: 'w1' }).get();
		expect(direct.isErr() && direct.error.code).toBe('forbidden');

		const response = await notes.http.get!({
			request: new Request('https://sync.test/get'),
			params: { workspaceId: 'w1' }
		});
		expect(response.status).toBe(403);
		expect(await response.json()).toMatchObject({ isOk: false, error: { code: 'forbidden' } });
		expect(handlerCalls).toBe(0);
		expect(reported).toEqual([
			{ code: 'forbidden', method: 'get', scope: 'authorization-denial:w1' },
			{ code: 'forbidden', method: 'get', scope: 'authorization-denial:w1' }
		]);
	});

	it('denies an HTTP write before its handler, persistence, outbox, or realtime publication runs', async () => {
		let handlerCalls = 0;
		const persistenceCalls: string[] = [];
		const realtimeCalls: string[] = [];
		const persistence: ManagerSyncPersistence = {
			append() {
				persistenceCalls.push('append');
			},
			readAfter(): ManagerOutboxRead {
				persistenceCalls.push('readAfter');
				return { envelopes: [], cursorFound: false, retainedEnvelopeCount: 0 };
			},
			readMutation(): ManagerMutationRecord | undefined {
				persistenceCalls.push('readMutation');
				return undefined;
			},
			recordMutation() {
				persistenceCalls.push('recordMutation');
			}
		};
		const realtimeBus: ManagerRealtimeBus = {
			publish(_envelope: SyncEnvelope) {
				realtimeCalls.push('publish');
			},
			subscribe() {
				realtimeCalls.push('subscribe');
				return () => {};
			}
		};
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: recordSchema,
				output: recordSchema,
				handler({ input, ctx }) {
					handlerCalls += 1;
					return ctx.ok({ id: input.id });
				}
			})
		}));
		const notes = manager({
			key: 'authorization-write-denial',
			resource: repository,
			authorize: () => false,
			scope: (params) => params.workspaceId,
			persistence,
			realtimeBus
		});

		const response = await notes.http.add!({
			request: new Request('https://sync.test/add', {
				method: 'POST',
				body: JSON.stringify({ input: { id: 'n1' } })
			}),
			params: { workspaceId: 'w1' }
		});

		expect(response.status).toBe(403);
		expect(await response.json()).toMatchObject({ isOk: false, error: { code: 'forbidden' } });
		expect(handlerCalls).toBe(0);
		expect(persistenceCalls).toEqual([]);
		expect(realtimeCalls).toEqual([]);
	});

	it('maps a thrown authorize hook to internal while a throwing error hook cannot replace it', async () => {
		let handlerCalls = 0;
		let errorHookCalls = 0;
		const repository = resource(paramsSchema, (method) => ({
			get: method.get({
				output: recordSchema,
				handler() {
					handlerCalls += 1;
					return ok({});
				}
			})
		}));
		const notes = manager<'authorization-throw', typeof repository, { userId: string }>({
			key: 'authorization-throw',
			resource: repository,
			authorize() {
				throw new Error('identity backend unavailable');
			},
			scope: (params) => params.workspaceId,
			handleError() {
				errorHookCalls += 1;
				throw new Error('reporter unavailable');
			}
		});

		const response = await notes.http.get!({
			request: new Request('https://sync.test/get'),
			params: { workspaceId: 'w1' },
			context: { userId: 'u1' }
		});
		const body = (await response.json()) as { error: { code: string; message: string } };
		expect(response.status).toBe(500);
		expect(body.error).toMatchObject({ code: 'internal', message: 'identity backend unavailable' });
		expect(handlerCalls).toBe(0);
		expect(errorHookCalls).toBe(1);
	});
});

describe('manager HTTP argument and metadata boundaries', () => {
	it('uses encoded query JSON before URL fallback and excludes client mutation fields from fallback', async () => {
		const seen: unknown[] = [];
		const repository = resource(paramsSchema, (method) => ({
			list: method.list({
				query: recordSchema,
				output: recordSchema,
				handler({ query, ctx }) {
					seen.push({ query, mutationId: ctx.mutationId, meta: ctx.meta });
					return ctx.ok({ items: [] });
				}
			})
		}));
		const notes = manager({
			key: 'http-query',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId
		});

		await notes.http.list!({
			request: new Request(
				'https://sync.test/list?query=%7B%22tag%22%3A%22encoded%22%7D&tag=fallback&clientId=url-client&mutationId=url-mutation'
			),
			params: { workspaceId: 'w1' }
		});
		await notes.http.list!({
			request: new Request('https://sync.test/list?tag=fallback&after=domain-cursor&clientId=c1&mutationId=m1'),
			params: { workspaceId: 'w1' }
		});

		expect(seen).toEqual([
			{ query: { tag: 'encoded' }, mutationId: 'url-mutation', meta: { clientId: 'url-client' } },
			{
				query: { tag: 'fallback', after: 'domain-cursor' },
				mutationId: 'm1',
				meta: { clientId: 'c1' }
			}
		]);
	});

	it('falls back to the delete URL query only when there is no JSON body', async () => {
		const seen: unknown[] = [];
		const repository = resource(paramsSchema, (method) => ({
			delete: method.delete({
				query: recordSchema,
				output: recordSchema,
				handler({ query, ctx }) {
					seen.push(query);
					return ctx.ok({ id: query.id });
				}
			})
		}));
		const notes = manager({
			key: 'http-delete',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId
		});

		await notes.http.delete!({
			request: new Request('https://sync.test/delete?id=url-id', { method: 'DELETE' }),
			params: { workspaceId: 'w1' }
		});
		await notes.http.delete!({
			request: new Request('https://sync.test/delete?id=url-id', {
				method: 'DELETE',
				body: JSON.stringify({ query: { id: 'body-id' } })
			}),
			params: { workspaceId: 'w1' }
		});

		expect(seen).toEqual([{ id: 'url-id' }, { id: 'body-id' }]);
	});

	it('merges request headers and JSON metadata with explicit operation options taking precedence', async () => {
		const seen: unknown[] = [];
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: recordSchema,
				output: recordSchema,
				handler({ input, ctx }) {
					seen.push({ input, mutationId: ctx.mutationId, meta: ctx.meta });
					return ctx.ok({ id: input.id });
				}
			})
		}));
		const notes = manager({
			key: 'http-metadata',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId
		});
		const response = await notes.http.add!({
			request: new Request('https://sync.test/add?mutationId=url-mutation&clientId=url-client', {
				method: 'POST',
				headers: {
					'x-mutation-id': 'header-mutation',
					'x-client-id': 'header-client',
					'x-sync-meta': JSON.stringify({ requestOnly: true, shared: 'request' })
				},
				body: JSON.stringify({ input: { id: 'n1' } })
			}),
			params: { workspaceId: 'w1' },
			options: {
				mutationId: 'explicit-mutation',
				meta: { explicitOnly: true, shared: 'explicit' }
			}
		});

		expect(response.status).toBe(200);
		expect(seen).toEqual([
			{
				input: { id: 'n1' },
				mutationId: 'explicit-mutation',
				meta: {
					requestOnly: true,
					clientId: 'header-client',
					explicitOnly: true,
					shared: 'explicit'
				}
			}
		]);
	});

	it('rejects a chunked body as soon as streamed bytes exceed the configured limit', async () => {
		let handlerCalls = 0;
		let cancelled = false;
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: recordSchema,
				output: recordSchema,
				handler({ ctx }) {
					handlerCalls += 1;
					return ctx.ok({});
				}
			})
		}));
		const notes = manager({
			key: 'http-size',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			maxPayloadBytes: 12
		});
		const body = new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(new TextEncoder().encode('{"input":'));
				controller.enqueue(new TextEncoder().encode('{"id":"too-large"}}'));
			},
			cancel() {
				cancelled = true;
			}
		});
		const request = new Request('https://sync.test/add', {
			method: 'POST',
			body,
			duplex: 'half'
		} as RequestInit & { duplex: 'half' });
		const response = await notes.http.add!({
			request,
			params: { workspaceId: 'w1' }
		});

		expect(response.status).toBe(413);
		expect(await response.json()).toMatchObject({ error: { code: 'payload_too_large' } });
		expect(cancelled).toBe(true);
		expect(handlerCalls).toBe(0);
	});

	it('counts exact UTF-8 request bytes rather than UTF-16 characters', async () => {
		const body = JSON.stringify({ input: { id: '😀' } });
		const byteLength = new TextEncoder().encode(body).byteLength;
		let handlerCalls = 0;
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: recordSchema,
				output: recordSchema,
				handler({ input, ctx }) {
					handlerCalls += 1;
					return ctx.ok(input);
				}
			})
		}));
		const exact = manager({
			key: 'http-unicode-exact',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			maxPayloadBytes: byteLength
		});
		const tooSmall = manager({
			key: 'http-unicode-small',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			maxPayloadBytes: byteLength - 1
		});

		const accepted = await exact.http.add!({
			request: new Request('https://sync.test/add', { method: 'POST', body }),
			params: { workspaceId: 'w1' }
		});
		const rejected = await tooSmall.http.add!({
			request: new Request('https://sync.test/add', { method: 'POST', body }),
			params: { workspaceId: 'w1' }
		});

		expect(accepted.status).toBe(200);
		expect(rejected.status).toBe(413);
		expect(handlerCalls).toBe(1);
	});

	it('composes request and explicit signals and classifies explicit cancellation as aborted', async () => {
		let started!: () => void;
		const didStart = new Promise<void>((resolve) => {
			started = resolve;
		});
		let handlerSignal: AbortSignal | undefined;
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: recordSchema,
				output: recordSchema,
				handler({ ctx }) {
					handlerSignal = ctx.signal;
					started();
					return new Promise((_, reject) => {
						ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason), { once: true });
					});
				}
			})
		}));
		const notes = manager({
			key: 'http-signal-composition',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId
		});
		const explicitAbort = new AbortController();
		const request = new Request('https://sync.test/add', {
			method: 'POST',
			body: JSON.stringify({ input: { id: 'n1' } })
		});
		const responsePromise = notes.http.add!({
			request,
			params: { workspaceId: 'w1' },
			options: { signal: explicitAbort.signal }
		});
		await didStart;
		explicitAbort.abort(new Error('caller stopped'));
		const response = await responsePromise;

		expect(request.signal.aborted).toBe(false);
		expect(handlerSignal?.aborted).toBe(true);
		expect(await response.json()).toMatchObject({ error: { code: 'aborted' } });
	});
});
