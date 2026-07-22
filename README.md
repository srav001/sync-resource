# Live Resource

Live Resource is a typed, server-authoritative synchronization library for applications that need cached reads,
optimistic writes, realtime updates, replay, repair, and mutation finality without coupling the client core to a UI
framework or database.

The database remains authoritative. Browser cache accelerates startup, optimistic state keeps interactions immediate,
and realtime envelopes deliver committed changes. Cursor replay and repair preserve correctness when delivery is
interrupted.

## Status

The package builds ESM JavaScript, TypeScript declarations, and source maps for its three public subpaths. The protocol
and public API are implemented; comprehensive characterization tests are the next major reliability milestone.

## Installation

```bash
npm install live-resource
```

No frontend framework is required. The client core works directly with browser JavaScript, DOM APIs, and application
code.

## Public Entry Points

| Import                      | Purpose                                       |
| --------------------------- | --------------------------------------------- |
| `live-resource/server`      | Resources, managers, streams, and persistence |
| `live-resource/client/core` | Framework-neutral runtime and stores          |
| `live-resource/shared`      | Protocol, schemas, errors, and shared types   |

## Development

Requirements: Node 24 and pnpm 11.1.2.

```bash
pnpm install
pnpm run validate
```

Focused commands:

```bash
pnpm run typecheck
pnpm run lint
pnpm run format
pnpm run build
pnpm run check
```

Formatting, linting, and type-aware checks are provided by Vite+, Oxfmt, and Oxlint.

## Core Model

The public model has three layers:

```txt
resource
  -> manager
  -> client store
```

The backend `resource` is the typed DB/domain IO boundary. It owns schemas, validation, handler typing, DB calls, output validation for state methods, signal validation for live-only methods, resource telemetry, and adapter-reported cost metrics.

The backend `manager` wraps a resource with sync behavior. It owns auth, scoped identity, HTTP/direct method exposure, realtime fanout, outbox replay, mutation finality, reset/repair, conflict/idempotency, lifecycle, and manager telemetry.

The frontend `client store` owns cache, hydration, optimistic state, pending commands, realtime connect, rollback,
repair, and framework-agnostic state events.

## Design Rules

- Use `resource(...)` for backend DB/domain IO.
- Use `manager(...)` for sync lifecycle and route exposure.
- Synchronized writes must go through the corresponding manager so realtime fanout, outbox replay, mutation finality,
  and client stores stay in sync.
- Resource methods may update the backing datastore because they are the manager-owned database boundary.
- Worker-only code must never import managers or anything connected to managers. Managers and manager-connected code
  are host-side only.
- Worker-only code should avoid direct Live Resource access where possible. If a worker truly needs persisted data
  access, the maximum allowed boundary is a resource, never a manager.
- Use `createStore<ManagerType>(...)` for framework-agnostic frontend state.
- Frontend imports only exported manager types, never backend runtime objects.
- There are no public resource modes such as `document`, `list`, or `event-stream`; method presence defines behavior.
- Do not expose public string dispatch APIs such as `execute('mutate', ...)`.
- All public resource, manager, and store methods are dot-callable and strongly typed.
- All non-trivial sync operations return strict `SyncResult` unions.
- Resource schemas may be `.parse(...)` validators or Standard Schema-compatible objects with `~standard.validate(...)`.
- Browser cache must use an IndexedDB-backed `CacheAdapter`; the concrete adapter belongs in the app, not the Live Resource package.
- Realtime, optimistic updates, rollback, rebase, and reconciliation are required for every synced store.
- Choose the sync method from the UI's synced unit, not from the database storage shape.
- UIs that render independently updated rows should expose those rows through `list` / `items()`, even when the resource reads and writes one aggregate DB document internally.
- Use `get` / `data()` only when the UI treats the scoped value as one unit.
- Prefer collections for independently updated application state. One-document domains can be modeled as one-item
  collections so they use `list` / `items()` and normal item optimistic behavior.
- Paginated collections should use `list`; do not full-hydrate large collections by default.
- Database-specific concerns stay in resources, resource-owned helpers, or persistence adapters, not in the core API.

## Method Surface

The system recognizes these method names:

| Method   | Meaning                   | Args                         | Store behavior                                                                                              |
| -------- | ------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `get`    | one scoped value          | optional `query`, no `input` | exposes `data()` and `get()`                                                                                |
| `list`   | paginated collection read | optional `query`, no `input` | exposes default `items()`, `listMeta()`, `pages()`, `refresh()`, `loadMore()`, and `list(query)` when typed |
| `add`    | create write              | `input` only                 | exposes `add()`                                                                                             |
| `mutate` | update/patch write        | optional `query` and `input` | exposes `mutate()`                                                                                          |
| `delete` | tombstone/delete write    | optional `query`, no `input` | exposes `delete()`                                                                                          |

Storage shape does not decide method shape. Use `list` when the UI renders rows, row actions, counts, filtering, or row-level optimistic updates, even if persistence is one aggregate document. Use `get` / `data()` only when the UI treats the scoped value as one unit.

Recommendation: use collections for synced application state. If a domain has one persisted document but the UI
updates nested records independently, expose those records as list items rather than one `get` / `data()` value.

Method schemas use this logical model:

```txt
params -> shared bound scope
query  -> selector/filter/cursor
input  -> write payload
output -> result payload for state methods
```

`params` is defined once at the resource level. Do not add method-level params. Runtime metadata such as `signal`, `actor`, deadline, and mutation id is passed as operation options/context, not as schema-validated business input.

Omit a method `query` schema when params already fully identify the synced unit. When a method defines a `query` schema, callers must pass a query that satisfies it. When a method omits `query`, resource and bound manager calls omit `query`, and HTTP requests do not send a `query` value.

## Reserved Protocol Fields

Keep protocol-owned fields out of domain meaning:

- `cursor` in list queries is the pagination cursor field injected by the store for `loadMore()`. UI code must not pass it directly.
- `after` on stream/connect URLs is the replay cursor.
- `clientId` and `mutationId` query fields, plus `x-client-id` and `x-mutation-id` headers, are client runtime/finality metadata.
- `x-sync-meta` carries JSON operation metadata.
- `x-sync-transport-id` identifies the browser transport for shared stream de-duplication.
- `id: string` is the default item identity convention. Use explicit reconcile only when a domain uses another stable identity.

Schema outputs drive both runtime values and TypeScript inference. If a Standard Schema transforms input to output, handlers receive the transformed output type and value. Async Standard Schema validation is intentionally not supported by the core parser; keep validation synchronous at the resource boundary.

List outputs may include optional list-level `meta`. This is for display metadata tied to the exact list query, such as total counts or other read-only summary values for the current result family. `meta` is inferred from the backend `list.output` schema and exposed on the client as `store.listMeta()`.

Use this rule for paginated metadata:

- first page / refresh responses should include authoritative `meta` when the UI needs it
- load-more responses with a cursor may omit `meta` or return it as `undefined` to avoid repeated summary queries
- when a response omits `meta` or returns `undefined`, the client keeps the current query family's existing metadata
- search/filter fields that change server results belong in `list.query`, so cache and metadata stay scoped to that query family
- UIs that show more than one query family at once should use `store.list(query)` handles instead of treating `items()` as a global collection

## Strict Results

Sync methods return dependency-free discriminated unions with Better Result-style guards:

```ts
type SyncResult<TValue, TError extends SyncError = SyncError> = SyncOk<TValue, TError> | SyncErr<TValue, TError>;

class SyncOk<TValue, TError extends SyncError = SyncError> {
	readonly status: 'ok';
	readonly value: TValue;
	isOk(): this is SyncOk<TValue, TError>;
	isErr(): this is SyncErr<TValue, TError>;
}

class SyncErr<TValue, TError extends SyncError = SyncError> {
	readonly status: 'error';
	readonly error: TError;
	isOk(): this is SyncOk<never, TError>;
	isErr(): this is SyncErr<TValue, TError>;
}
```

Use narrowing:

```ts
const result = await notes.mutate({ query: { id }, input: patch });

if (result.isOk()) {
	result.value;
} else {
	result.error;
}
```

Access `value` or `error` only after narrowing with `.isOk()`, `.isErr()`, their negative branches, or `status`.
Successful results own only `status` and `value`; failed results own only `status` and `error`. Guard methods are
shared through prototypes rather than allocated per result.

`SyncError` is an `Error` subclass on both server and client, so a narrowed `result.error` passes both
`instanceof SyncError` and `instanceof Error`. HTTP/SSE keeps its separate plain wire DTO, and client transport code
recreates `SyncError` while constructing the public result. Direct calls do not serialize or deserialize results.
Live Resource internals construct failed results with `err(code, message, options)` or preserve an existing
`SyncError` with `err(syncErrorValue)`. Resource handlers continue to use `ctx.error(...)`.

## Backend Resource API

Define resources with the params schema first and method factory second:

Before changing resource behavior, read `src/server/resource.ts` and `src/server/manager.ts`. The examples below are
the public shape; the source is the
authoritative guide for exact validation, self-call, batch, envelope, and error behavior.

```ts
import { resource } from 'live-resource/server';

const notesRepository = getNotesRepository();

export const notesResource = resource(noteParamsSchema, (method) => ({
	list: method.list({
		query: notePageQuerySchema,
		output: notePageSchema,
		async handler({ params, query, ctx }) {
			const page = await notesRepository.list<NoteDocument>({ params, query });
			if (page.isErr()) {
				return ctx.error(ctx.syncError('internal', page.error.message, { cause: page.error }));
			}

			return ctx.ok({
				items: page.value.items,
				pageCursor: buildNextNoteCursor(page.value.items)
			});
		}
	}),

	add: method.add({
		input: createNoteSchema,
		output: noteSchema,
		async handler({ params, input, ctx }) {
			const note = buildNoteDocument(params, input, ctx.actor);
			const created = await notesRepository.create<NoteDocument>(note, 'create_note');
			if (created.isErr()) {
				return ctx.error(ctx.syncError('internal', created.error.message, { cause: created.error }));
			}
			if (!created.value.resource) {
				return ctx.error(ctx.syncError('internal', 'Repository create did not return a note.'));
			}

			return ctx.ok(created.value.resource);
		}
	}),

	mutate: method.mutate({
		query: noteByIdSchema,
		input: notePatchSchema,
		output: noteSchema,
		async handler({ params, query, input, ctx }) {
			const patched = await notesRepository.patch<NoteDocument>(
				query.id,
				{ operations: buildNotePatchOperations(input, ctx.actor) },
				'patch_note',
				params.workspaceId
			);
			if (patched.isErr()) {
				return ctx.error(ctx.syncError('internal', patched.error.message, { cause: patched.error }));
			}
			if (!patched.value.resource) {
				return ctx.error(ctx.syncError('not_found', 'Note not found.'));
			}

			return ctx.ok(patched.value.resource);
		}
	}),

	delete: method.delete({
		query: noteByIdSchema,
		output: noteTombstoneSchema,
		async handler({ params, query, ctx }) {
			const deleted = await notesRepository.delete(query.id, 'delete_note', params.workspaceId);
			if (deleted.isErr()) {
				return ctx.error(ctx.syncError('internal', deleted.error.message, { cause: deleted.error }));
			}

			return ctx.ok({ id: query.id, deleted: true });
		}
	})
}));
```

Resource dependencies such as repositories, clocks, and id generators belong in closures or factory parameters:

```ts
interface CreateNotesResourceDeps {
	readonly repository: NotesRepository;
	readonly clock: Clock;
}

export function createNotesResource(deps: CreateNotesResourceDeps) {
	return resource(noteParamsSchema, (method) => ({
		mutate: method.mutate({
			query: noteByIdSchema,
			input: notePatchSchema,
			output: noteSchema,
			async handler({ params, query, input, ctx }) {
				const patched = await deps.repository.patch<NoteDocument>(
					query.id,
					{ operations: buildNotePatchOperations(input, deps.clock.now()) },
					'patch_note',
					params.workspaceId
				);
				if (patched.isErr()) {
					return ctx.error(ctx.syncError('internal', patched.error.message, { cause: patched.error }));
				}
				if (!patched.value.resource) {
					return ctx.error(ctx.syncError('not_found', 'Note not found.'));
				}

				return ctx.ok(patched.value.resource);
			}
		})
	}));
}
```

Keep database details out of manager config. The resource is the database/domain IO boundary, so it can call the
backing database directly, use a file-backed test store, or depend on another persistence adapter without changing the
manager API.

## Resource Handler Context

Every resource handler receives `ctx`:

```ts
interface ResourceHandlerContext {
	readonly signal: AbortSignal;
	readonly deadline?: number;
	readonly mutationId?: string;
	readonly meta?: OperationMeta;
	readonly actor?: SyncActor;
	ok(): SyncOk<void>;
	ok<TValue>(value: TValue): SyncOk<TValue>;
	error(error: SyncError): SyncErr<never, SyncError>;
	syncError(code: SyncErrorCode, message: string, options?: SyncErrorOptions): SyncError;
}
```

Use `ctx.signal` for DB and network cancellation. Use `ctx.actor` for audit/permission-aware writes. Use `ctx.ok`, `ctx.error`, and `ctx.syncError` so handlers stay on the local result shape.

Resource handlers are wrapped so thrown errors become typed sync errors and trigger error hooks.

Handlers also receive `self`, a runtime sibling-method object for calling the same resource without manager auth, outbox, realtime fanout, or mutation finality. `self` auto-binds current `params`, `signal`, `meta`, `actor`, and mutation context. Use it only for internal canonical reads/writes where bypassing manager sync behavior is intentional.

## Resource Outputs And Sync Changes

The common handler return is the domain output:

```ts
return ctx.ok(note);
```

The manager can infer normal sync changes from method shape and ids:

- `list` output with `items` becomes `pageLoaded`.
- `add` output with `id` becomes `itemAdded`.
- `mutate` query/output with `id` becomes `itemUpdated`.
- `delete` query/output with `id` becomes `itemDeleted`.

Advanced handlers may return an explicit envelope source:

```ts
return ok({
	output: note,
	changes: [change.itemUpdated({ id: note.id, patch: input, value: note })],
	syncCursor,
	metrics: [{ name: 'database.operationCost', value: operationCost, unit: 'units' }],
	sourceCommit
});
```

Use explicit `changes` only when default inference is not enough. Use `metrics` for generic adapter cost reporting.
Core sync does not attach database-specific meaning to those metrics.

Read responses do not get synthetic replay cursors. A `list` read without a real `syncCursor` returns no response envelope, so the client does not connect with a cursor that the outbox cannot replay. If a read response needs to advance the replay cursor, the resource must return a real persisted cursor through `syncCursor` or explicit changes that include it. Writes with state changes get manager-generated cursors and are persisted through the outbox/finality path.

Handlers may also return live-only `signals`:

```ts
return ctx.ok({
	signals: [
		{
			type: 'user-event',
			payload: input
		}
	]
});
```

Signal results are exclusive: return `signals` only, with no `output` and no `changes`. State results use `output`
with optional `changes`, and must not include `signals`.

Signals are delivered only to currently connected subscribers. They are not cached, written to the outbox, replayed
from cursors, or applied to document/list state. Use them for one-shot notifications and stream-only deltas, not for
durable data.

## Direct Resource Usage

Resources can be called directly:

```ts
const result = await notesResource.mutate(
	{
		params,
		query: { id: noteId },
		input: patch
	},
	{ actor, signal }
);
```

Good direct resource use cases:

- workflow-engine or worker code that must avoid manager/runtime imports
- tests
- internal maintenance flows that intentionally skip sync side effects
- typed DB/domain composition where realtime/finality is not needed

Direct resource calls still validate schemas and return strict results. They do not perform manager auth, realtime publish, durable outbox append, finality, or reset/catch-up handling. Normal user-facing synced writes should go through the manager.

## Backend Manager API

Managers wrap resources:

```ts
import { manager } from 'live-resource/server';

export const notesManager = manager({
	key: 'notes',
	resource: notesResource,
	authorize(context, params) {
		return workspaceAccess(context, params);
	},
	scope(params) {
		return params.workspaceId;
	}
});

export type NotesManager = typeof notesManager.type;
```

Manager rules:

- `key` is the public sync identity.
- `scope(params)` returns the smallest stable identity for fanout, replay, and reset.
- `resource` does not define or know the sync key.
- Manager type exports are type-only frontend API surfaces.
- Manager direct calls use `bind(params, context?)`.

Scope examples:

```ts
scope: (params) => params.orgId;
scope: (params) => params.projectId;
scope: (params) => [params.projectId, params.userId];
```

Do not return object scopes for normal usage. The manager internally namespaces the scope with the manager key.

Direct manager usage:

```ts
const boundNotes = notesManager.bind({ workspaceId }, context);

const result = await boundNotes.mutate(
	{
		query: { id: noteId },
		input: { $set: { title: 'Renamed' } }
	},
	{ signal }
);
```

Direct manager calls are useful inside host-side code that needs auth/sync/finality behavior without going through HTTP.

## Idempotency And Mutation Identity

Writes are tracked by `(scope, mutationId)`.

Frontend stores generate mutation ids internally. Application code does not pass mutation ids for normal optimistic writes.

Manager behavior:

- same `mutationId`, method, and stable args hash replays the persisted output/envelope
- same `mutationId` with different method or arguments returns `conflict`
- same-process concurrent writes with the same `scope` and `mutationId` share one in-flight execution
- successful writes with authoritative changes are recorded as `finalized`
- successful writes without an envelope are recorded as `acknowledged`

Production persistence should enforce durable uniqueness for `(scope, mutationId)` and should write source data, mutation record, and outbox envelope atomically where possible.

In-memory persistence keeps mutation records only for a bounded idempotency window. Do not rely on memory persistence for replay or mutation finality across API restarts.
The package currently ships only memory persistence. Durable production persistence is app-owned so the storage schema, partitioning, TTL, and source-write atomicity can match the backing database.

## Persistence, Source Commit, And DB Cost

The core persistence boundary is DB-agnostic:

```ts
interface ManagerOutbox {
	append(envelope: SyncEnvelope): Awaitable<void>;
	readAfter(scope: string, cursor: string, limit: number): Awaitable<ManagerOutboxRead>;
}

interface ManagerSyncPersistence extends ManagerOutbox {
	readMutation(scope: string, mutationId: string): Awaitable<ManagerMutationRecord | undefined>;
	recordMutation(record: ManagerMutationRecord): Awaitable<void>;
}
```

Cursor contract:

- Cursors are opaque replay tokens scoped to one manager scope.
- `readAfter(scope, cursor, limit)` must return envelopes strictly after that cursor when it is retained.
- If the cursor is not retained but the scope has retained history, return `cursorFound: false` and the retained count so the stream can emit a reset.
- Manager-generated cursors include process entropy to avoid cross-process token collisions, but they are not a durable ordered sequence by themselves.
- Durable persistence should make replay ordering stable across processes. If the backing store owns sequence numbers, the resource/commit path should return that persisted cursor through `syncCursor`.
- Read/list responses must not invent replay cursors that are not present in durable persistence.

Use `sourceCommit` when a resource needs the manager-supplied mutation record and envelope before committing the source DB write:

```ts
return ctx.ok({
	output: note,
	changes: [change.itemUpdated({ id: note.id, value: note })],
	sourceCommit: {
		async commit({ mutationRecord, envelope }) {
			const response = await notesRepository.patch<NoteDocument>(
				note.id,
				{ operations: buildNoteAndSyncPatch(note, mutationRecord, envelope) },
				'commit_note_and_sync',
				note.workspaceId
			);
			if (response.isErr()) {
				return ctx.error(ctx.syncError('internal', response.error.message, { cause: response.error }));
			}

			return ctx.ok({
				syncPersisted: true,
				metrics: [
					{
						name: 'database.operationCost',
						value: response.value.operationCost ?? 0,
						unit: 'units'
					}
				]
			});
		}
	}
});
```

This lets a resource use DB-native transactions, batch writes, or stored procedures without forcing the core sync API
to know about that DB.

For database adapters:

- keep partition-aware reads and writes inside resources or resource-owned helpers
- return database cost through generic `CostMetric` values
- avoid broad cross-partition reads in `list`
- prefer targeted updates where possible
- use database-native transaction support in `batchHandler` or `sourceCommit` when needed
- keep durable sync persistence atomic with source writes where the database allows it

## Batch Writes

Do not add separate public batch methods. The normal write method accepts one call or a homogeneous array:

```ts
await notes.mutate({
	query: { id: noteId },
	input: patch
});

await notes.mutate([
	{ query: { id: 'note_1' }, input: patch1 },
	{ query: { id: 'note_2' }, input: patch2 }
]);
```

This applies to `add`, `mutate`, and `delete`.

Rules:

- Batches are homogeneous.
- Do not mix add/mutate/delete in one public call.
- Do not add `addBatch`, `mutateBatch`, or `deleteBatch`.
- Use method-level `batchHandler` for DB-native batch behavior.
- Without `batchHandler`, the resource runtime loops item handlers.
- Atomic batch failure rolls back the whole optimistic group.
- Non-atomic item failure rolls back failed item patches only.

Example DB-native batch handler:

```ts
mutate: method.mutate({
	query: noteByIdSchema,
	input: notePatchSchema,
	output: noteSchema,
	async batchHandler({ params, items, ctx }) {
		const outputs: SyncBatchItem<ResourceOk<NoteDocument>>[] = [];
		let okCount = 0;
		let errorCount = 0;

		for (let index = 0; index < items.length; index += 1) {
			const item = items[index];
			const patched = await notesRepository.patch<NoteDocument>(
				item.query.id,
				{ operations: buildNotePatchOperations(item.input) },
				'patch_notes_batch',
				params.workspaceId
			);

			if (patched.isErr()) {
				errorCount += 1;
				outputs.push({
					index,
					status: 'error',
					error: ctx.syncError('internal', patched.error.message, { cause: patched.error })
				});
				continue;
			}

			if (!patched.value.resource) {
				errorCount += 1;
				outputs.push({
					index,
					status: 'error',
					error: ctx.syncError('not_found', 'Note not found.')
				});
				continue;
			}

			okCount += 1;
			outputs.push({
				index,
				status: 'ok',
				value: { output: patched.value.resource }
			});
		}

		return ctx.ok({
			items: outputs,
			execution: {
				mode: 'bulk',
				atomic: false,
				okCount,
				errorCount
			}
		});
	}
});
```

## HTTP Integration

Managers expose framework-neutral handlers that accept a standard Web `Request` and return a `Response`. Mount those
handlers inside the web framework or server used by the host application.

Mount the shared physical stream once:

```ts
import { httpSharedSyncStream } from 'live-resource/server';

export function handleLiveResourceStream(request: Request): Promise<Response> {
	return httpSharedSyncStream(request);
}
```

Mount manager handlers only for methods defined by that manager:

```ts
export function connectNotes(request: Request, workspaceId: string): Promise<Response> | undefined {
	return notesManager.http.connect?.({
		request,
		params: { workspaceId }
	});
}
```

| Route      | HTTP   | Used by                       |
| ---------- | ------ | ----------------------------- |
| `/get`     | `GET`  | scoped value read             |
| `/list`    | `GET`  | paginated collection read     |
| `/add`     | `POST` | create write                  |
| `/mutate`  | `POST` | update/patch write            |
| `/delete`  | `POST` | tombstone/delete write        |
| `/connect` | `POST` | logical realtime subscription |

Do not mount manager `/events` routes for normal application synchronization. Normal realtime uses one shared SSE stream
plus manager `/connect` routes for logical subscriptions. Manager `/events` is a direct diagnostic stream.

Transport details:

- `list` reads `query` from the URL search parameter as JSON.
- `delete` can read `query` from the URL when no JSON body is present.
- Writes send JSON bodies shaped like `{ input }`, `{ query, input }`, `{ query }`, or arrays of those shapes.
- Protocol parameters such as `clientId` and `mutationId` are excluded from resource query parameters.
- `x-client-id` identifies the browser runtime for optimistic finality origin checks.
- `x-mutation-id` is generated by the store for writes.
- The manager reads `after` from the query string for replay.

If a framework pre-reads the request body, its adapter must rebuild a clean `Request` before calling the manager.
Manager parsing remains the single synchronization transport boundary.

## Realtime Protocol

The browser transport uses one physical stream for the browser session and logical subscriptions per manager/scope.

Before touching stream behavior, read `src/server/streamMultiplexer.ts`, `src/client/runtime.ts`, and
`src/shared/protocol.ts`.

Manager `stream.maxEventBytes` is the logical envelope-size allowance when a manager legitimately emits larger
envelopes. Because the physical stream is shared, the stream uses the largest registered manager allowance to decide
whether an envelope is allowed at all. Envelopes larger than the runtime SSE frame budget are split into `sync-chunk`
frames on the wire and reassembled by the client runtime before normal `SyncEnvelope` validation and dispatch.

Default route shape:

```txt
POST /api/live-resource/stream
POST <manager-sync-route>/connect
```

The physical stream carries envelopes:

```ts
interface SyncEnvelope {
	readonly managerKey: string;
	readonly scope: string;
	readonly cursor: string;
	readonly sourceMutationId?: string;
	readonly sourceClientId?: string;
	readonly changes: readonly SyncChange[];
	readonly signals?: readonly SyncSignal[];
	readonly reset?: ResetManifest;
	readonly metrics?: readonly CostMetric[];
}
```

Rules:

- Never open one physical stream per store, manager, or project.
- Duplicate stores for the same manager/scope share one logical subscription.
- Reconnect re-registers active logical subscriptions with their latest cursor.
- Replay/reset remains per logical scope through durable outbox cursors.
- `/connect` returns the authoritative encoded scope before replay is delivered.
- A pending `/connect` is abortable when the last local subscriber is disposed.
- Manager `stream.onScopeIdle` is only for releasing manager-owned ephemeral per-scope memory.
- Shared-stream replay failure reports through the manager error hook, emits a `replay_failed` reset when possible, and closes the stream so reconnect/repair owns recovery.
- Ping events are ignored on the client hot path before JSON parsing.
- Server streams use bounded event size, bounded backpressure, heartbeat, stale transport TTL, and per-transport/IP limits.
- The connection cap is per browser transport id plus IP. This limits duplicate streams from one browser session without punishing many real users behind one NAT. It is not a standalone per-IP abuse ceiling.
- Stream close/cancel clears per-stream cursor replay buffers immediately, so high-fanout connections do not retain cursor bookkeeping after disconnect while waiting for GC.

Realtime is required, but correctness comes from cursor replay and repair. A missed SSE event is not data loss if the outbox can replay or trigger reset.

## Cross-Process Realtime

Use a manager realtime bus when multiple API processes can serve sync streams:

```ts
import { createPubSubManagerRealtimeBus } from 'live-resource/server';

const realtimeBus = createPubSubManagerRealtimeBus({
	transport: {
		publish(channel, payload) {
			return redis.publish(channel, payload);
		},
		subscribe(channel, onPayload) {
			return subscribeWithRedis(channel, onPayload);
		}
	}
});

export const notesManager = manager({
	key: 'notes',
	resource: notesResource,
	authorize,
	scope: (params) => params.workspaceId,
	realtimeBus
});
```

The transport wrapper can be Redis, NATS, Azure Service Bus, or another pub/sub system. Bus failures must not invalidate committed writes; durable outbox replay is still the correctness backstop.

## Frontend Runtime Configuration

Configure sync once near app startup:

```ts
import { configureSync } from 'live-resource/client/core';
import { IndexedDbCacheAdapter } from './liveResourceIndexedDbCache.js';

const DAY = 24 * 60 * 60 * 1000;

configureSync({
	fetch: globalFetch,
	streamUrl: '/api/live-resource/stream',
	cache: {
		adapter: new IndexedDbCacheAdapter('live-resource-sync', 'store'),
		ttlMs: 14 * DAY
	}
});
```

Rules:

- Do not pass runtime/fetch/transport/cache adapter into individual stores.
- The app fetch/auth/session-cookie behavior belongs in one `configureSync(...)`.
- The cache adapter is app-wide.
- Store-level `cache` is only a policy override such as TTL.
- Store construction must not trigger network requests.
- Store construction can restore cached data in the background for instant UI.
- List stores must define a default `query()`; this is used by hydrate, reconnect repair, and `refresh()` when no explicit query is passed. For no-query list resources, return `undefined` so the request sends no query.
- Browser stores must use IndexedDB.

## Frontend Store API

TypeScript frontends import only manager types. JavaScript frontends use the same runtime API without type imports or
generic parameters.

Before creating or changing stores, read `src/client/store.ts` and `src/client/optimistic.ts`.

```ts
import type { NotesManager } from './notesManager.js';
```

Collection store:

Use a collection store for any UI whose primary surface is rows, cards, or records with independent actions. The
backend `list` handler can flatten rows from an aggregate document; the resource remains the DB/domain boundary.
This collection shape also works when the collection contains exactly one item.

```ts
import { createStore } from 'live-resource/client/core';

const notes = createStore<NotesManager>(
	{
		key: 'notes',
		getParams() {
			return { workspaceId };
		},
		getUrl(params) {
			return `/api/workspaces/${params.workspaceId}/notes/sync`;
		},
		query() {
			return {
				status: statusFilter(),
				search: searchQuery(),
				limit: 50
			};
		},
		cache: {
			ttlMs: 14 * DAY
		}
	},
	(reconcile) =>
		reconcile.defaults({
			matchesQuery(note, { query }) {
				return note.status === query.status;
			},
			compare(left, right) {
				return right.createdAt.localeCompare(left.createdAt);
			}
		})
);

notes.items();
notes.refresh();
notes.loadMore();
notes.add({ input: newNote });
notes.mutate({ query: { id: noteId }, input: { $set: { title: 'Renamed' } } });
notes.delete({ query: { id: noteId } });
```

Simple collection usage:

```ts
await notes.hydrate();

const visibleNotes = notes.items();

const firstPage = await notes.refresh();
if (firstPage.isErr()) {
	showError(firstPage.error.message);
}

const nextPage = await notes.loadMore();
if (nextPage.isOk()) {
	render(notes.items());
}

const listMeta = notes.listMeta();
if (listMeta) {
	renderSummary(listMeta.totalCount);
}

const rename = await notes.mutate({
	query: { id: noteId },
	input: { $set: { title: 'Renamed' } }
});

if (rename.isErr()) {
	showError(rename.error.message);
}
```

Simple pagination usage:

```ts
// The configured query() is the default visible list.
await notes.hydrate();

// The store injects the last page cursor internally.
await notes.loadMore();

// A second concurrent list family uses a scoped handle.
const archivedNotes = notes.list({ status: 'archived', limit: 100 });

await archivedNotes.refresh();
render(archivedNotes.items());

await archivedNotes.loadMore();
```

UI code should not pass cursors, page indexes, loaded id arrays, or family keys. It passes only the domain query
that a user understands, such as status/search/limit. Scoped handles are for cases where the UI displays more than
one query family at the same time. Read `store.ts` and `optimistic.ts` before changing this.

Package-level one-value store:

The generic package still supports `get` / `data()` for codebases that edit or display a whole scoped value as one unit.
Use this only when the UI treats the complete scoped value as one unit.
If the UI maps over nested children and mutates them independently, make those children the `list` items instead.

```ts
import type { OrgManager } from './orgManager.js';

const org = createStore<OrgManager>({
	key: 'org',
	getParams() {
		return { orgId };
	},
	getUrl(params) {
		return `/api/orgs/${params.orgId}/sync`;
	}
});

org.data();
org.get();
org.mutate({ input: { name: 'Acme' } });
```

Store methods return `SyncResult`, so UI code should branch with `.isOk()` / `.isErr()`.

## Plain JavaScript Examples

These examples use browser JavaScript directly. The IndexedDB adapter remains application-owned and is passed once
through `configureSync(...)`.

### Create and hydrate a store

```js
import { configureSync, createStore } from 'live-resource/client/core';
import { cacheAdapter } from './cache.js';

configureSync({
	fetch: (...args) => fetch(...args),
	streamUrl: '/api/live-resource/stream',
	cache: {
		adapter: cacheAdapter,
		ttlMs: 14 * 24 * 60 * 60 * 1000
	}
});

let status = 'active';

const notes = createStore(
	{
		key: 'notes',
		getParams: () => ({ workspaceId: 'workspace-123' }),
		getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
		query: () => ({ status, limit: 50 })
	},
	(reconcile) =>
		reconcile.defaults({
			matchesQuery: (note, context) => note.status === context.query.status,
			compare: (left, right) => right.createdAt.localeCompare(left.createdAt)
		})
);

const hydrated = await notes.hydrate();
if (hydrated.isErr()) {
	console.error(hydrated.error);
}
```

Store construction is network-free. `hydrate()` restores cached authoritative state, refreshes from the server, and
connects realtime delivery.

### Render updates with DOM APIs

```js
const noteList = document.querySelector('[data-note-list]');
const pendingLabel = document.querySelector('[data-pending]');

function renderNotes(items) {
	const rows = items.map((note) => {
		const row = document.createElement('li');
		row.dataset.noteId = note.id;
		row.textContent = note.title;
		return row;
	});

	noteList.replaceChildren(...rows);
}

renderNotes(notes.items());

const stopItems = notes.on.items(renderNotes);
const stopPending = notes.on.pending((pending) => {
	pendingLabel.hidden = pending.length === 0;
});
const stopErrors = notes.on.error((error) => {
	if (error) {
		console.error(error);
	}
});

window.addEventListener(
	'pagehide',
	() => {
		stopItems();
		stopPending();
		stopErrors();
		notes.dispose();
	},
	{ once: true }
);
```

Typed event slots notify only the state that changed. Each subscription returns an unsubscribe function, and
`dispose()` releases transport, timers, repair work, and subscriptions owned by the store.

### Write and paginate without a framework

```js
const created = await notes.add({
	input: {
		id: crypto.randomUUID(),
		title: 'First note',
		status: 'active'
	}
});

if (created.isErr()) {
	console.error(created.error);
}

const renamed = await notes.mutate({
	query: { id: 'note-123' },
	input: { $set: { title: 'Renamed' } }
});

if (renamed.isErr()) {
	console.error(renamed.error);
}

const nextPage = await notes.loadMore();
if (nextPage.isErr()) {
	console.error(nextPage.error);
}

status = 'archived';
const archived = await notes.refresh();
if (archived.isErr()) {
	console.error(archived.error);
}
```

Writes update visible state optimistically when identity and patch information are available. The returned HTTP result
is an acknowledgement; authoritative envelope or reset coverage determines finality.

## Store State And Events

Core store exposes:

```ts
interface StoreSnapshot<TManager> {
	readonly data: OutputOf<TManager, 'get'> | undefined;
	readonly items: readonly CollectionItem<TManager>[];
	readonly pages: readonly PageState[];
	readonly pending: readonly PendingCommand[];
	readonly hydrating: boolean;
	readonly refreshing: boolean;
	readonly disposed: boolean;
	readonly error: SyncError | null;
}
```

It also exposes typed event slots:

```ts
store.on.data((data) => {});
store.on.items((items) => {});
store.on.pages((pages) => {});
store.on.signal('user-event', (payload) => {});
store.on.pending((pending) => {});
store.on.hydrating((hydrating) => {});
store.on.refreshing((refreshing) => {});
store.on.error((error) => {});
```

Use `on.signal(type, callback)` for live-only messages. Signal callbacks do not imply hydration, cache writes, pending settlement, or state updates.

The core emits only slots affected by an operation/envelope. For example, if only `hydrating` changes, item
subscribers are not notified. Consumers own equality checks, batching, memoization, and deep/proxy reactivity.

`snapshot()` and coarse `subscribe(snapshot)` exist for compatibility/debug use. Consumers should prefer typed event
slots so unrelated UI subscriptions do not rerun.

## Cache-First Behavior

The client cache defaults to a long TTL, normally two weeks.

Rules:

- If cached data exists, display it immediately.
- If cached data exists and the store is hydrating, show the cached data plus a background refresh indicator.
- If no cached data exists and hydration is in flight, show loading UI in the content area.
- If no data exists and hydration is not in flight, show empty/no-data UI.
- Store construction is network-free.
- `hydrate()` restores cache, reads from server, persists fresh authoritative state, and connects realtime.
- Public `hydrate()` is successful-once per store/scope lifetime: concurrent calls share the active attempt, later calls
  reuse a completed success without IO, and failed attempts remain retryable. `isHydrating()` stays true through cache
  restore, the authoritative read, and realtime connection.
- Reconnect catch-up forces a new authoritative hydration even after public hydration succeeded. Explicit `refresh()`
  and repair/reset reads also remain independent of the public hydration success.
- `restore()` only loads authoritative cache. In-memory pending commands are preserved, but pending commands are not restored from IndexedDB.
- `connect()` attaches realtime from the current cached cursor without forcing a server refresh.
- IndexedDB stores only last-success authoritative state. Optimistic pending commands stay memory-only.
- Cache writes use a trailing debounce with a max-wait cap, currently `delayMs: 50` and `maxWaitMs: 250`. Each dirty event resets the short delay so normal SSE bursts write once after they quiet down, but the first dirty event also starts the max-wait timer so continuous realtime traffic cannot postpone IndexedDB forever. Events that arrive during an in-flight write schedule the next debounce cycle after the current write completes. The store passes plain values to the cache adapter; the adapter/library owns IndexedDB storage details.

A common wrapper component can derive UI state from `data/items`, `isHydrating()`, `isRefreshing()`, `isPending()`, and `error()`. Do not duplicate derived booleans in the store unless there is a real runtime reason.

## Optimistic Writes, Finality, And Rebase

Optimistic flow:

```txt
apply local pending patch
send manager write
ACK keeps command finalizing
authoritative envelope settles finality
rollback or repair on failure
```

HTTP success is not finality. Finality means the client has authoritative delta/reset coverage for the write.

Rules:

- Client can show optimistic state immediately.
- Optimistic `add` needs a client-known item identity in the write input, or an explicit reconcile `itemId` that can derive it. If the server is the only place that creates the id, the authoritative response will add the item later but automatic optimistic insertion cannot happen.
- Pending commands remain tracked until authoritative coverage arrives.
- Client `mutate` writes with known target ids apply optimistic patches to only those visible items before re-positioning them in sorted collections.
- Optimistic `mutate` is patch-based: the client applies the write `input` to the visible target item before the server responds.
- If a mutation is expressed as a domain command such as `{ action: 'use' }`, the client cannot infer the UI patch from that command. Include the expected optimistic `$set` / `$unset` fields in `input`, or do not expect automatic optimistic UI.
- External side effects, such as sending an Ask message or starting a backend operation, can run separately. The canonical synced state change should still go through `store.mutate()` immediately when the UI should update immediately.
- A direct successful write response with an envelope is authoritative for that local mutation and must clear that mutation's pending state immediately.
- Remote changes apply to the authoritative base.
- Pending local patches replay over the newest authoritative base.
- Local origin metadata prevents reapplying the same optimistic write from a realtime echo.
- If a remote client uses a colliding mutation id, `sourceClientId` prevents clearing local pending state.
- Pending commands are memory-only. Reloading during an in-flight write drops the optimistic overlay, shows the last cached authoritative data, and lets hydrate/realtime settle from the server.
- Rejected writes remove failed pending patches.
- Same-session rollback removes the failed pending patch and replays remaining pending patches over the in-memory authoritative base.

Error recovery shape:

```ts
type SyncErrorCode =
	| 'validation'
	| 'bad_request'
	| 'unauthorized'
	| 'forbidden'
	| 'not_found'
	| 'conflict'
	| 'payload_too_large'
	| 'rate_limited'
	| 'aborted'
	| 'timeout'
	| 'disposed'
	| 'internal';

type SyncRecoverySource = 'memory' | 'idb' | 'server';

type SyncRecoveryMetadata =
	| { readonly restored: true; readonly source: SyncRecoverySource }
	| { readonly restored?: false; readonly source?: never };

class SyncError extends Error {
	readonly code: SyncErrorCode;
	readonly details?: unknown;
	readonly cause?: unknown;
	readonly recovery?: SyncRecoveryMetadata;
}
```

Normal same-session rollback should report `source: 'memory'`.

## Reconcile Defaults

Users should not define reconcile for normal stores.

Default collection reconcile works when:

- `list.output.items[number]` has `id: string`
- write query has `id`
- or write query has one obvious target id field

Compile-time checks should require explicit reconcile when item or target identity cannot be inferred.

Explicit reconcile stays as the second argument to `createStore`:

```ts
const notes = createStore<NotesManager>(config, (reconcile) =>
	reconcile.defaults({
		itemId: (note) => note.noteKey,
		targetId: ({ query }) => query.noteKey,
		matchesQuery: (note, { query }) => note.status === query.status,
		compare: (left, right) => right.createdAt.localeCompare(left.createdAt)
	})
);
```

`itemId` receives `{ params }`, `targetId` receives `{ params, query, input }`, and `matchesQuery` receives `{ params, query }`. Use params for identity when the resource method omits query because the scoped params already identify the one item.

`matchesQuery` is optional. Use it when realtime external adds can be safely checked against the current page query. If it returns false, the store keeps the entity in `baseItems` but does not add it to visible page ids.

`compare` is optional. Use it when user-visible order is domain-specific.

For realtime external `itemAdded` on a loaded page window:

- if `matchesQuery` rejects the item, keep it out of the visible window
- if the loaded family has one complete empty page and `matchesQuery` accepts the item, insert it into that page
- if `compare` is missing, keep other non-empty entities cached but do not guess page placement
- if `compare` proves the item sorts into a non-complete loaded window, insert and trim to the current loaded capacity
- if the loaded family is complete, insert the matching item and grow the visible list
- if it sorts beyond the loaded window, leave visible ids unchanged and do not refetch

This behavior is source-agnostic. An add from another browser, an agent, a worker, or a backend tool follows the same query-window reconciliation rules as a local optimistic add after the authoritative envelope arrives. Visibility is based on query membership and ordering, not on whether the current browser created the mutation.

## Pagination And Repair

`list` stores page/window state and optional list metadata per query family. The query family key is derived from the domain query without cursor fields. The default store methods use the configured `query()`. Additional simultaneous families use `store.list(query)` handles.

Before changing pagination, external-add visibility, stale-page repair, or query-family behavior, read
`src/client/optimistic.ts` first and then `src/client/store.ts`.

```ts
interface PageState {
	readonly cursorIn?: string;
	readonly cursorOut?: string;
	readonly ids: readonly string[];
	readonly coverage: 'prefix' | 'partial' | 'full' | 'stale';
	readonly complete: boolean;
	readonly stale: boolean;
	readonly repairNeeded: boolean;
	readonly lastSyncCursor?: string;
	readonly source: 'cache' | 'network' | 'realtime';
}
```

Rules:

- Collection reads are pagination-first.
- Every list store has a default `query()` so automatic hydrate and repair always know the visible family to load. For no-query list resources, the default query returns `undefined`.
- `refresh()` loads the default configured query family.
- `loadMore()` reads the default family's last `cursorOut` and injects the cursor internally.
- `store.list(query)` returns a scoped handle with `items()`, `meta()`, `pages()`, `refresh()`, and `loadMore()` for an additional query family.
- UI code does not pass cursor, page index, batch index, family key, or loaded ids.
- The store keeps one normalized entity cache and many page families.
- `listMeta()` returns typed metadata from the default family, such as `totalCount` or other query-level summary values.
- First-page responses should return `meta` when the UI needs exact query-level counts.
- Load-more responses may omit `meta` or return `undefined`; the store preserves that family's previous metadata.
- Text search should be part of `list.query` when it must search beyond loaded rows.
- Frontend-only filters are acceptable when they intentionally apply only to loaded visible items.
- UIs with multiple visible result families should use scoped `store.list(query)` handles for each displayed family.
- Reusing a scoped handle query restores its cached page ids immediately.
- Realtime subscriptions are scope-based, not page-based.
- Remote row updates patch loaded rows by id.
- Remote deletes apply tombstones.
- External adds are evaluated against each loaded family's `matchesQuery` and `compare`.
- Membership/order uncertainty keeps the entity base-only or marks affected pages stale when repair is required.
- `store.repair()` retries stale page repair when UI/tests need direct control.
- Multiple repair requests in one tick share one in-flight repair.
- A repair request during active repair queues one follow-up repair.
- Full scope reset is reserved for cursor/finality gaps, unsafe schema/data changes, and retention gaps.

Page-size guidance:

- Use small page limits for normal browser screens.
- Use large limits only for bounded admin or explicit bulk workflows.
- Small pages reduce browser memory and keep optimistic/realtime reconciliation cheaper.
- Large pages reduce DB round trips when each list call has high latency.
- Do not use page size as a substitute for correct pagination and `loadMore`.

The 50,000-row local harness showed that 10,000-item pages were correct but made optimistic reconciliation slower as the loaded window grew. Smaller pages kept client reconciliation cheaper at the cost of more DB round trips.

Backend list example:

```ts
const noteListQuerySchema = t.Object({
	search: t.Optional(t.String()),
	limit: t.Number(),
	cursor: t.Optional(t.String())
});

const noteListPageSchema = t.Object({
	items: t.Array(noteSchema),
	pageCursor: t.Optional(t.String()),
	meta: t.Optional(
		t.Object({
			totalCount: t.Number()
		})
	)
});

list: method.list({
	query: noteListQuerySchema,
	output: noteListPageSchema,
	async handler({ params, query, ctx }) {
		const page = await notesRepository.list<NoteDocument>({ params, query, limit: query.limit + 1 });
		if (page.isErr()) {
			return ctx.error(ctx.syncError('internal', page.error.message, { cause: page.error }));
		}

		const limit = query.limit;
		const items = page.value.items.slice(0, limit);
		const meta = query.cursor ? undefined : await buildNoteListMeta(params, query);
		return ctx.ok({
			items,
			pageCursor: page.value.items.length > limit ? encodeNoteCursor(items[items.length - 1]) : undefined,
			meta
		});
	}
});
```

Frontend list example:

```ts
const notes = createStore<NotesManager>(
	{
		key: 'notes',
		getParams: () => ({ workspaceId }),
		getUrl: (params) => `/api/workspaces/${params.workspaceId}/notes/sync`,
		query: () => ({ status: statusFilter(), limit: 50 })
	},
	(reconcile) =>
		reconcile.defaults({
			matchesQuery(note, { query }) {
				return note.status === query.status;
			},
			compare(left, right) {
				return right.createdAt.localeCompare(left.createdAt);
			}
		})
);

await notes.hydrate();
await notes.loadMore();
```

`refresh()` loads the first page for the default query family. `loadMore()` reuses the default family cursor and adds the next page. For concurrent list families, call `store.list(query).refresh()` and `store.list(query).loadMore()`. UI code should trigger this through a shared infinite-scroll component. If a realtime item belongs outside all loaded windows, the entity can stay base-only without refetching visible lists.

## Reset Manifests

Reset envelopes carry a manifest:

```ts
interface ResetManifest {
	readonly scope: string;
	readonly reason: string;
	readonly affectedFamilies?: readonly string[];
	readonly previousCursor?: string;
	readonly nextCursor?: string;
	readonly schemaVersion?: string;
	readonly authVersion?: string;
	readonly cachePolicyVersion?: string;
}
```

Use page repair before full scope reset when safe. Use full reset for cursor/finality gaps, unsafe auth/schema/cache changes, replay failure, or retained outbox gaps.

Known reset reasons include:

- `retention_gap`
- `replay_failed`

## Recommended Implementation Path

When adding a synced domain:

1. Define params, query, input, output, and tombstone schemas as needed. Signal-only writes do not need an output schema.
2. Create a resource near the DB code.
3. Keep DB-specific logic in the resource or resource-owned dependencies.
4. Return plain `ctx.ok(output)` for normal methods.
5. Add explicit `changes`, `metrics`, `batchHandler`, or `sourceCommit` only when the domain needs it.
6. Create a manager with `key`, `resource`, `authorize`, and `scope`.
7. Export `type XManager = typeof xManager.type`.
8. Mount manager HTTP methods inside a normal domain route module.
9. Mount the shared stream handler once for the host application.
10. Configure frontend sync once with application fetch and an application-owned IndexedDB cache adapter.
11. Create frontend stores with only manager type, params, URL, query, cache policy, and optional reconcile.
12. Branch on `SyncResult` for frontend actions.
13. Run targeted conformance tests when touching synchronization behavior.
14. Run `pnpm run validate`.

## Common Mistakes

- Importing backend manager runtime code into the frontend.
- Passing a backend runtime object to `createStore`; pass only the exported manager type as a generic.
- Adding public action names for domain logic that can be ordinary `mutate`.
- Modeling embedded row collections as `get` / `data()` because the DB stores one aggregate document.
- Using action-only mutate inputs while expecting automatic optimistic row updates.
- Returning changed data from a custom side-effect endpoint instead of updating canonical synced state through `store.mutate()` / manager mutate.
- Adding separate public batch methods instead of homogeneous arrays.
- Putting DB dependencies into manager config.
- Adding cache adapters per store instead of configuring cache once.
- Opening one stream per store/manager/scope.
- Treating HTTP ACK as finality.
- Clearing pending state without authoritative envelope/reset coverage.
- Persisting optimistic pending overlays or pending metadata in IndexedDB.
- Writing broad cross-partition database queries inside `list`.
- Using full-list hydration for large collections.
- Adding defensive normalization when schemas already cover the boundary.
- Duplicating state that can be derived from data, pending, hydrating, refreshing, and error.

## Repository Map

Use this README first for the model and public API. Read the owning source when changing internals:

- Resource definition and execution: `src/server/resource.ts`
- Manager, finality, idempotency, and outbox behavior: `src/server/manager.ts`
- Shared stream multiplexing and SSE fanout: `src/server/streamMultiplexer.ts`
- Client store orchestration: `src/client/store.ts`
- Optimistic and pagination reconciliation: `src/client/optimistic.ts`
- Browser runtime, transport, and cache scheduling: `src/client/runtime.ts`
- Protocol guards and shared types: `src/shared/protocol.ts`
