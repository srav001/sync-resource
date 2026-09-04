import { Context, Effect, Fiber } from 'effect';
import { HttpRouter } from 'effect/unstable/http';
import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
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

afterEach(() => resetSharedStreamForTests());

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
		let started!: () => void;
		const didStart = new Promise<void>((resolve) => {
			started = resolve;
		});
		let finalized = false;
		const notes = resource(paramsSchema, (method) => ({
			get: method.get({
				output: noteSchema,
				handler: () =>
					Effect.ensuring(
						Effect.andThen(
							Effect.sync(started),
							Effect.as(Effect.never, { id: 'unreachable', title: 'unreachable' })
						),
						Effect.sync(() => {
							finalized = true;
						})
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
		await didStart;
		await Effect.runPromise(Fiber.interrupt(fiber));
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
		await Promise.resolve();
		expect(sourceCommitted).toBe(true);
		expect(interruptionFinished).toBe(false);
		releaseFinality();
		await interrupted;
		expect(records).toHaveLength(1);
	});

	it('mounts only supported HTTP routes unless diagnostic events are requested', async () => {
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
		expect(managerHttpRoutes({ ...options, events: true })).toHaveLength(3);

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

		const stream = await notesManager.http.events!({
			request: new Request('https://sync.test/events'),
			params: { workspaceId: 'w1' }
		});
		const reader = stream.body!.getReader();
		await reader.read();
		await reader.cancel();
		await vi.waitFor(() => expect(realtimeFinalized).toBe(true));
		expect(events).toContain('subscribe');
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
