import { Context, Effect, Fiber, Layer } from 'effect';
import { HttpRouter, HttpServerRequest, HttpServerResponse } from 'effect/http';
import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncConnectRoute,
	httpSharedSyncStreamRoute,
	manager,
	managerHttpRoutes,
	resource,
	type EffectManagerPersistence
} from '../../src/server/effect.ts';
import {
	httpSharedSyncStream,
	syncError,
	type ManagerMutationRecord,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { nextMacrotask } from '../fixtures/async.ts';
import { openSharedStream } from '../fixtures/sharedStream.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface Params {
	readonly workspaceId: string;
}

interface Note {
	readonly id: string;
	readonly title: string;
}

const paramsSchema = {
	parse(value: unknown): Params {
		if (
			!value ||
			typeof value !== 'object' ||
			typeof (value as { workspaceId?: unknown }).workspaceId !== 'string'
		) {
			throw new Error('invalid params');
		}
		return value as Params;
	}
};

const noteSchema = {
	parse(value: unknown): Note {
		if (
			!value ||
			typeof value !== 'object' ||
			typeof (value as { id?: unknown }).id !== 'string' ||
			typeof (value as { title?: unknown }).title !== 'string'
		) {
			throw new Error('invalid note');
		}
		return value as Note;
	}
};

const notesSchema = {
	parse(value: unknown): readonly Note[] {
		if (!Array.isArray(value)) {
			throw new Error('invalid notes');
		}
		return value.map((item) => noteSchema.parse(item));
	}
};

const querySchema = {
	parse(value: unknown): { readonly id: string } {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('invalid query');
		}
		return value as { readonly id: string };
	}
};

afterEach(() => {
	vi.useRealTimers();
	resetSharedStreamForTests();
});

describe('Effect server entrypoint', () => {
	it('preserves handler service requirements and typed failures', async () => {
		const NotePrefix = Context.Service<{ readonly value: string }>('sync-resource/test/NotePrefix');
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) =>
					Effect.map(NotePrefix, ({ value }) => ({
						id: params.workspaceId,
						title: `${value}:${params.workspaceId}`
					}))
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-context',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId
			})
		);

		const value = await Effect.runPromise(
			Effect.provideService(notesManager.bind({ workspaceId: 'w1' }).get(), NotePrefix, { value: 'note' })
		);
		expect(value).toEqual({ id: 'w1', title: 'note:w1' });

		const failing = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: () =>
					Effect.as(Effect.fail(syncError('not_found', 'missing')), {
						id: 'unreachable',
						title: 'unreachable'
					})
			})
		}));
		const failingManager = await Effect.runPromise(
			manager({
				key: 'effect-failure',
				resource: failing,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId
			})
		);
		const failure = await Effect.runPromise(Effect.flip(failingManager.bind({ workspaceId: 'w1' }).get()));
		expect(failure).toMatchObject({ code: 'not_found', message: 'missing' });
	});

	it('interrupts handlers and waits for their finalizers', async () => {
		const started = new Deferred();
		const releaseFinalizer = new Deferred();
		let finalized = false;
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: () =>
					Effect.ensuring(
						Effect.andThen(
							Effect.sync(() => started.resolve()),
							Effect.as(Effect.never, { id: 'unreachable', title: 'unreachable' })
						),
						Effect.promise(() => releaseFinalizer.promise).pipe(
							Effect.andThen(
								Effect.sync(() => {
									finalized = true;
								})
							)
						)
					)
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-interrupt',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId
			})
		);
		const fiber = Effect.runFork(notesManager.bind({ workspaceId: 'w1' }).get());
		await started.promise;
		let interruptionFinished = false;
		const interrupted = Effect.runPromise(Fiber.interrupt(fiber)).then(() => {
			interruptionFinished = true;
		});
		await nextMacrotask();
		expect(interruptionFinished).toBe(false);
		releaseFinalizer.resolve();
		await interrupted;
		expect(finalized).toBe(true);
	});

	it('finishes persistence finality after a successful source commit before interruption completes', async () => {
		let sourceCommitted = false;
		let finalityStarted!: () => void;
		const didStartFinality = new Promise<void>((resolve) => {
			finalityStarted = resolve;
		});
		let releaseFinality!: () => void;
		const finalityGate = new Promise<void>((resolve) => {
			releaseFinality = resolve;
		});
		const records: ManagerMutationRecord[] = [];
		const persistence: EffectManagerPersistence<never> = {
			append: () => Effect.void,
			readAfter: () => Effect.succeed({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 }),
			readMutation: () => Effect.succeed(undefined),
			recordMutation: (record) =>
				Effect.promise(async () => {
					finalityStarted();
					await finalityGate;
					records.push(record);
				})
		};
		const notes = resource(paramsSchema, (method) => ({
			add: method.add({
				input: noteSchema,
				output: noteSchema,
				handler: ({ input }) =>
					Effect.succeed({
						output: input,
						changes: [{ type: 'itemAdded' as const, id: input.id, value: input }],
						sourceCommit: {
							commit: () =>
								Effect.sync(() => {
									sourceCommitted = true;
								})
						}
					})
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-finality',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId,
				persistence
			})
		);
		const fiber = Effect.runFork(
			notesManager.bind({ workspaceId: 'w1' }).add({ input: { id: 'n1', title: 'one' } }, { mutationId: 'm1' })
		);
		await didStartFinality;
		let interruptionFinished = false;
		const interrupted = Effect.runPromise(Fiber.interrupt(fiber)).then(() => {
			interruptionFinished = true;
		});
		await nextMacrotask();
		expect(sourceCommitted).toBe(true);
		expect(interruptionFinished).toBe(false);
		releaseFinality();
		await interrupted;
		expect(records).toHaveLength(1);
	});

	it('gives repeated and distinct caller contexts their own services next to the manager context', async () => {
		const Region = Context.Service<{ readonly value: string }>('sync-resource/test/Region');
		const NotePrefix = Context.Service<{ readonly value: string }>('sync-resource/test/NotePrefix');
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) =>
					Effect.map(NotePrefix, ({ value }) => ({ id: params.workspaceId, title: value }))
			})
		}));
		const notesManager = await Effect.runPromise(
			Effect.provideService(
				manager({
					key: 'effect-merge',
					resource: notes,
					authorize: () => Effect.map(Region, ({ value }) => value === 'base'),
					scope: (p) => p.workspaceId
				}),
				Region,
				{ value: 'base' }
			)
		);
		const bound = notesManager.bind({ workspaceId: 'w1' });
		const titles = (prefix: string) =>
			Effect.runPromise(
				Effect.provideService(
					Effect.all([bound.get(), bound.get()]).pipe(Effect.map((values) => values.map((v) => v.title))),
					NotePrefix,
					{ value: prefix }
				)
			);
		expect(await titles('a')).toEqual(['a', 'a']);
		expect(await titles('b')).toEqual(['b', 'b']);
		expect(await titles('a')).toEqual(['a', 'a']);
	});

	it('mounts only the supported HTTP routes', async () => {
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-routes',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId
			})
		);
		const options = {
			manager: notesManager,
			path: '/notes' as const,
			params: () => Effect.succeed({ workspaceId: 'w1' })
		};
		expect(managerHttpRoutes(options)).toHaveLength(2);

		const routes = managerHttpRoutes<undefined, Params, never, never, never>(options);
		const managerWeb = HttpRouter.toWebHandler(HttpRouter.addAll(routes), {
			disableLogger: true
		});
		const response = await managerWeb.handler(new Request('https://sync.test/notes/get'));
		expect(response.status).toBe(200);
		expect(await response.json()).toMatchObject({ value: { id: 'w1', title: 'one' } });
		await managerWeb.dispose();

		const streamWeb = HttpRouter.toWebHandler(HttpRouter.addAll([httpSharedSyncStreamRoute('/sync')]), {
			disableLogger: true
		});
		expect((await streamWeb.handler(new Request('https://sync.test/sync', { method: 'POST' }))).status).toBe(400);
		await streamWeb.dispose();
	});

	it('keeps the status and headers of buffered JSON responses', async () => {
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-headers',
				resource: notes,
				authorize: (_context, params) => Effect.succeed(params.workspaceId === 'w1'),
				scope: (p) => p.workspaceId
			})
		);
		const routesFor = (workspaceId: string) =>
			HttpRouter.toWebHandler(
				HttpRouter.addAll(
					managerHttpRoutes<undefined, Params, never, never, never>({
						manager: notesManager,
						path: '/notes',
						params: () => Effect.succeed({ workspaceId })
					})
				),
				{ disableLogger: true }
			);

		const allowed = routesFor('w1');
		const ok = await allowed.handler(new Request('https://sync.test/notes/get'));
		expect(ok.status).toBe(200);
		expect(ok.headers.get('content-type')).toBe('application/json; charset=utf-8');
		expect(ok.headers.get('cache-control')).toBe('no-store');
		await allowed.dispose();

		const denied = routesFor('w2');
		const forbidden = await denied.handler(new Request('https://sync.test/notes/get'));
		expect(forbidden.status).toBe(403);
		expect(forbidden.headers.get('content-type')).toBe('application/json; charset=utf-8');
		expect(await forbidden.json()).toMatchObject({ isError: true, error: { code: 'forbidden' } });
		await denied.dispose();
	});

	it('runs each batched connect through the full router, including its middleware and params', async () => {
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const seen: Record<string, string[]> = { a: [], b: [] };
		const middlewareSaw: string[] = [];
		let rejectB = true;
		const routesFor = async (key: 'a' | 'b') => {
			const notesManager = await Effect.runPromise(
				manager({
					key: `batch-${key}`,
					resource: notes,
					authorize: () => Effect.succeed(true),
					scope: (p) => p.workspaceId
				})
			);
			return managerHttpRoutes<undefined, Params, never, never, never>({
				manager: notesManager,
				path: `/${key}`,
				params: (request) => {
					seen[key]!.push(request.url);
					return Effect.succeed({ workspaceId: `w-${key}` });
				}
			});
		};
		const authentication = HttpRouter.middleware((handler) =>
			Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) => {
				middlewareSaw.push(request.url);
				return rejectB && request.url.startsWith('/b/')
					? Effect.succeed(HttpServerResponse.empty({ status: 401 }))
					: handler;
			})
		).layer;
		const app = Layer.mergeAll(
			HttpRouter.addAll([...(await routesFor('a')), ...(await routesFor('b'))]),
			httpSharedSyncConnectRoute('/api/connect')
		).pipe(Layer.provide(authentication));
		const web = HttpRouter.toWebHandler(app, { disableLogger: true });
		const connect = async (connects: string[]) => {
			const response = await web.handler(
				new Request('http://sync.test/api/connect', {
					method: 'POST',
					headers: { 'content-type': 'application/json', 'x-sync-transport-id': 'transport-batch' },
					body: JSON.stringify({ connects })
				})
			);
			expect(response.status).toBe(200);
			return ((await response.json()) as { results: { status: number; body: unknown }[] }).results;
		};

		const rejected = await connect(['/a/connect?after=cursor-1', '/b/connect']);
		expect(rejected.map((result) => result.status)).toEqual([200, 401]);
		expect(rejected[0]?.body).toMatchObject({ isOk: true, value: { subscribed: true, managerKey: 'batch-a' } });
		expect(seen).toEqual({ a: ['/a/connect?after=cursor-1'], b: [] });
		expect(middlewareSaw).toEqual(['/api/connect', '/a/connect?after=cursor-1', '/b/connect']);

		rejectB = false;
		const accepted = await connect(['/b/connect']);
		expect(accepted[0]).toMatchObject({ status: 200, body: { value: { managerKey: 'batch-b' } } });
		expect(seen.b).toEqual(['/b/connect']);
		await web.dispose();
	});

	it('dispatches batched connects with the services of the batch request', async () => {
		const Tenant = Context.Service<{ readonly id: string }>('sync-resource/test/Tenant');
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'batch-tenant',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (p) => p.workspaceId
			})
		);
		const routes = managerHttpRoutes<undefined, Params, never, { readonly id: string }, never>({
			manager: notesManager,
			path: '/tenant',
			params: () => Effect.map(Tenant, ({ id }) => ({ workspaceId: id }))
		});
		const app = Layer.mergeAll(HttpRouter.addAll(routes), httpSharedSyncConnectRoute('/api/connect'));
		const web = HttpRouter.toWebHandler(app, { disableLogger: true });
		const response = await web.handler(
			new Request('http://sync.test/api/connect', {
				method: 'POST',
				headers: { 'content-type': 'application/json', 'x-sync-transport-id': 'transport-tenant' },
				body: JSON.stringify({ connects: ['/tenant/connect'] })
			}),
			Context.make(Tenant, { id: 'w-tenant' })
		);
		expect(response.status).toBe(200);
		expect(await response.json()).toMatchObject({
			results: [{ status: 200, body: { isOk: true, value: { managerKey: 'batch-tenant' } } }]
		});
		await web.dispose();
	});

	it('adapts every resource method, self call, batch handler, and resource hook', async () => {
		const events: string[] = [];
		const notes = resource(
			paramsSchema,
			(method) => ({
				get: method.get({
					output: noteSchema,
					handler: ({ params, self }) =>
						Effect.map(self.list!(), (listed) => ({
							id: params.workspaceId,
							title: (listed as { readonly output: readonly Note[] }).output[0]?.title ?? 'empty'
						}))
				}),
				list: method.list({
					output: notesSchema,
					handler: () => Effect.succeed([{ id: 'n1', title: 'one' }])
				}),
				add: method.add({
					input: noteSchema,
					output: noteSchema,
					handler: ({ input }) => Effect.succeed(input),
					batchHandler: ({ items }) =>
						Effect.succeed({
							items: items.map((item, index) => ({
								index,
								status: 'ok' as const,
								value: { output: item.input }
							})),
							execution: { mode: 'bulk', atomic: false, okCount: items.length, errorCount: 0 }
						})
				}),
				mutate: method.mutate({
					query: querySchema,
					input: noteSchema,
					output: noteSchema,
					handler: ({ query, input }) => Effect.succeed({ ...input, id: query.id })
				}),
				delete: method.delete({
					query: querySchema,
					output: noteSchema,
					handler: ({ query }) => Effect.succeed({ id: query.id, title: 'deleted' })
				})
			}),
			{
				name: 'effect-resource-hooks',
				telemetry: {
					start: (context) => Effect.sync(() => events.push(`start:${context.method}`)),
					success: (context, metrics) =>
						Effect.sync(() => events.push(`success:${context.method}:${metrics.length}`)),
					error: (context, error) => Effect.sync(() => events.push(`error:${context.method}:${error.code}`))
				},
				handleError: (error, context) =>
					Effect.sync(() => events.push(`handled:${context.method}:${error.code}`))
			}
		);
		const params = { workspaceId: 'w1' };
		expect(await Effect.runPromise(notes.get({ params }))).toMatchObject({ output: { id: 'w1', title: 'one' } });
		expect(await Effect.runPromise(notes.add({ params, input: { id: 'n2', title: 'two' } }))).toMatchObject({
			output: { id: 'n2' }
		});
		expect(
			await Effect.runPromise(
				notes.add([
					{ params, input: { id: 'n3', title: 'three' } },
					{ params, input: { id: 'n4', title: 'four' } }
				])
			)
		).toMatchObject({ execution: { mode: 'bulk', okCount: 2 } });
		expect(
			await Effect.runPromise(notes.mutate({ params, query: { id: 'n5' }, input: { id: 'old', title: 'five' } }))
		).toMatchObject({ output: { id: 'n5' } });
		expect(await Effect.runPromise(notes.delete({ params, query: { id: 'n6' } }))).toMatchObject({
			output: { id: 'n6' }
		});
		const invalid = await Effect.runPromise(Effect.flip(notes.get({ params: {} as Params })));
		expect(invalid.code).toBe('validation');
		expect(events).toContain('success:get:0');
		expect(events).toContain('success:add:0');
		expect(events).toContain('error:get:validation');
		expect(events).toContain('handled:get:validation');
	});

	it('adapts persistence, realtime, manager telemetry, HTTP, and scoped cleanup', async () => {
		const events: string[] = [];
		const envelopes: SyncEnvelope[] = [];
		const mutations = new Map<string, ManagerMutationRecord>();
		let realtimeFinalized = false;
		const realtimeClosed = new Deferred();
		const notes = resource(paramsSchema, (method) => ({
			add: method.add({
				input: noteSchema,
				output: noteSchema,
				handler: ({ input }) =>
					Effect.succeed({
						output: input,
						changes: [{ type: 'itemAdded' as const, id: input.id, value: input }]
					})
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-manager-hooks',
				resource: notes,
				authorize: () => Effect.sync(() => (events.push('authorize'), true)),
				scope: (params) => params.workspaceId,
				persistence: {
					append: (envelope) => Effect.sync(() => envelopes.push(envelope)),
					readAfter: (_scope, cursor) =>
						Effect.succeed({
							cursorFound: envelopes.some((envelope) => envelope.cursor === cursor),
							envelopes: [],
							retainedEnvelopeCount: envelopes.length
						}),
					readMutation: (_scope, mutationId) => Effect.succeed(mutations.get(mutationId)),
					recordMutation: (record) =>
						Effect.sync(() => {
							mutations.set(record.mutationId, record);
							if (record.envelope) {
								envelopes.push(record.envelope);
							}
						})
				},
				realtimeBus: {
					publish: () => Effect.sync(() => events.push('publish')),
					subscribe: (_scope, _onEnvelope) =>
						Effect.andThen(
							Effect.addFinalizer(() =>
								Effect.sync(() => {
									realtimeFinalized = true;
									realtimeClosed.resolve();
								})
							),
							Effect.sync(() => events.push('subscribe'))
						)
				},
				telemetry: {
					start: (context) => Effect.sync(() => events.push(`start:${context.method}`)),
					success: (context, metrics) =>
						Effect.sync(() => events.push(`success:${context.method}:${metrics.length}`)),
					error: (context, error) => Effect.sync(() => events.push(`error:${context.method}:${error.code}`))
				},
				handleError: (error, context) =>
					Effect.sync(() => events.push(`handled:${context.method}:${error.code}`))
			})
		);
		const bound = notesManager.bind({ workspaceId: 'w1' });
		expect(await Effect.runPromise(bound.add({ input: { id: 'n1', title: 'one' } }, { mutationId: 'm1' }))).toEqual(
			{ id: 'n1', title: 'one' }
		);
		expect(envelopes).toHaveLength(1);
		expect(mutations.has('m1')).toBe(true);
		expect(events).toContain('publish');
		expect(events).toContain('success:add:0');

		const duplicate = await Effect.runPromise(
			bound.add({ input: { id: 'n1', title: 'one' } }, { mutationId: 'm1' })
		);
		expect(duplicate).toEqual({ id: 'n1', title: 'one' });

		const response = await notesManager.http.add!({
			request: new Request('https://sync.test/add', {
				method: 'POST',
				body: JSON.stringify({ input: { id: 'n2', title: 'two' } })
			}),
			params: { workspaceId: 'w1' }
		});
		expect(response.status).toBe(200);

		vi.useFakeTimers();
		const reader = await openSharedStream(notesManager.http, { workspaceId: 'w1' });
		await reader.cancel();
		expect(events).toContain('subscribe');
		expect(realtimeFinalized).toBe(false);
		await vi.advanceTimersByTimeAsync(5 * 60_000);
		await realtimeClosed.promise;
	});

	it('forwards the persistence latest cursor to list reads', async () => {
		const notes = resource(paramsSchema, (method) => ({
			list: method.list({
				output: { parse: (value: unknown) => value as { readonly items: readonly Note[] } },
				handler: () => Effect.succeed({ items: [{ id: 'n1', title: 'one' }] })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-read-cursor',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (params) => params.workspaceId,
				persistence: {
					append: () => Effect.void,
					readAfter: () => Effect.succeed({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 }),
					latestCursor: (scope) => Effect.succeed(`latest:${scope}`),
					readMutation: () => Effect.succeed(undefined),
					recordMutation: () => Effect.void
				}
			})
		);
		const response = await notesManager.http.list!({
			request: new Request('https://sync.test/list'),
			params: { workspaceId: 'w1' }
		});
		const body = (await response.json()) as { readonly envelope?: { readonly cursor: string } };
		expect(body.envelope?.cursor).toBe('latest:effect-read-cursor:w1');
	});

	it('forwards the outbox latest cursor to list reads only when the outbox implements it', async () => {
		const notes = resource(paramsSchema, (method) => ({
			list: method.list({
				output: { parse: (value: unknown) => value as { readonly items: readonly Note[] } },
				handler: () => Effect.succeed({ items: [{ id: 'n1', title: 'one' }] })
			})
		}));
		const cursorOf = async (key: string, latestCursor?: (scope: string) => Effect.Effect<string>) => {
			const notesManager = await Effect.runPromise(
				manager({
					key,
					resource: notes,
					authorize: () => Effect.succeed(true),
					scope: (params) => params.workspaceId,
					outbox: {
						append: () => Effect.void,
						readAfter: () => Effect.succeed({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 }),
						...(latestCursor ? { latestCursor } : {})
					}
				})
			);
			const response = await notesManager.http.list!({
				request: new Request('https://sync.test/list'),
				params: { workspaceId: 'w1' }
			});
			return ((await response.json()) as { readonly envelope?: { readonly cursor: string } }).envelope?.cursor;
		};
		expect(await cursorOf('effect-outbox-cursor', (scope) => Effect.succeed(`latest:${scope}`))).toBe(
			'latest:effect-outbox-cursor:w1'
		);
		expect(await cursorOf('effect-outbox-no-cursor')).toBeUndefined();
	});

	it('closes the scope of a realtime subscription that fails after acquiring a resource', async () => {
		const finalized = new Deferred();
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-failing-bus',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (params) => params.workspaceId,
				realtimeBus: {
					publish: () => Effect.void,
					subscribe: () =>
						Effect.andThen(
							Effect.addFinalizer(() =>
								Effect.sync(() => {
									finalized.resolve();
								})
							),
							Effect.fail(syncError('internal', 'subscribe failed'))
						)
				}
			})
		);
		const reader = await openSharedStream(notesManager.http, { workspaceId: 'w1' });
		await reader.cancel();
		await finalized.promise;
	});

	it('adapts the standalone outbox and shared-stream idle hook', async () => {
		vi.useFakeTimers();
		const events: string[] = [];
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: ({ params }) => Effect.succeed({ id: params.workspaceId, title: 'one' })
			})
		}));
		const notesManager = await Effect.runPromise(
			manager({
				key: 'effect-outbox',
				resource: notes,
				authorize: () => Effect.succeed(true),
				scope: (params) => params.workspaceId,
				outbox: {
					append: () => Effect.sync(() => events.push('append')),
					readAfter: () =>
						Effect.sync(() => {
							events.push('readAfter');
							return { cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 };
						})
				},
				stream: {
					idleTtlMs: 1,
					onScopeIdle: (scope) => Effect.sync(() => events.push(`idle:${scope}`))
				}
			})
		);
		const connected = await notesManager.http.connect!({
			request: new Request('https://sync.test/connect?after=seed', {
				method: 'POST',
				headers: { 'x-sync-transport-id': 'effect-transport' }
			}),
			params: { workspaceId: 'w1' }
		});
		expect(connected.status).toBe(200);
		const response = await httpSharedSyncStream(
			new Request('https://sync.test/stream', {
				method: 'POST',
				headers: { 'x-sync-transport-id': 'effect-transport' }
			})
		);
		const reader = response.body!.getReader();
		await reader.read();
		await reader.cancel();
		await vi.advanceTimersByTimeAsync(5 * 60_000);
		expect(events).toContain('readAfter');
		expect(events).toContain('idle:effect-outbox:w1');
	});
});
