# Sync Resource Usage And API

Sync Resource is a typed, server-authoritative synchronization library for applications that need cached reads,
optimistic writes, realtime updates, replay, repair, and mutation finality without coupling the client core to a UI
framework or database.

The database remains authoritative. Browser cache accelerates startup, optimistic state keeps interactions immediate,
and realtime envelopes deliver committed changes. Cursor replay and repair preserve correctness when delivery is
interrupted.

Server code is written with plain Promises (`sync-resource/server`) or with Effect v4 (`sync-resource/server/effect`);
both expose the same protocol, so one client talks to either. See [Effect Server API](#effect-server-api).

Examples in this guide use the framework-neutral JavaScript client. The same configuration and actions work through
the [Solid](./solid.md), [Vue](./vue.md), and [React](./react.md) adapters; their guides show only the reactive-read and
lifecycle translation.

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
- Worker-only code should avoid direct Sync Resource access where possible. If a worker truly needs persisted data
  access, the maximum allowed boundary is a resource, never a manager.
- Use `createStore<ManagerType>(...)` for framework-agnostic frontend state.
- Frontend imports only exported manager types, never backend runtime objects.
- There are no public resource modes such as `document`, `list`, or `event-stream`; method presence defines behavior.
- Do not expose public string dispatch APIs such as `execute('mutate', ...)`.
- All public resource, manager, and store methods are dot-callable and strongly typed.
- All non-trivial sync operations return strict `SyncResult` unions.
- Resource schemas may be `.parse(...)` validators or Standard Schema-compatible objects with `~standard.validate(...)`.
- Browser cache must use an IndexedDB-backed `CacheAdapter`; the concrete adapter belongs in the app, not the Sync Resource package.
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

Sync methods return dependency-free discriminated unions with `isOk()` and `isErr()` guards:

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
Sync Resource internals construct failed results with `err(code, message, options)` or preserve an existing
`SyncError` with `err(syncErrorValue)`. Resource handlers continue to use `ctx.error(...)`.

## Backend Resource API

Define resources with the params schema first and method factory second:

Before changing resource behavior, read `src/server/resource.ts` and `src/server/manager.ts`. The examples below are
the public shape; the source is the
authoritative guide for exact validation, self-call, batch, envelope, and error behavior.

```ts
import { and, desc, eq } from 'drizzle-orm';
import { resource } from 'sync-resource/server';

import { db, notes } from './db.js';

export const notesResource = resource(noteParamsSchema, (method) => ({
	list: method.list({
		query: notePageQuerySchema,
		output: notePageSchema,
		async handler({ params, query, ctx }) {
			const items = await db
				.select()
				.from(notes)
				.where(eq(notes.workspaceId, params.workspaceId))
				.orderBy(desc(notes.createdAt))
				.limit(query.limit);

			return ctx.ok({ items, pageCursor: buildNextNoteCursor(items) });
		}
	}),

	add: method.add({
		input: createNoteSchema,
		output: noteSchema,
		async handler({ params, input, ctx }) {
			const [note] = await db
				.insert(notes)
				.values(buildNoteDocument(params, input, ctx.actor))
				.returning();

			return ctx.ok(note);
		}
	}),

	mutate: method.mutate({
		query: noteByIdSchema,
		input: notePatchSchema,
		output: noteSchema,
		async handler({ params, query, input, ctx }) {
			const [note] = await db
				.update(notes)
				.set({ ...input, updatedBy: ctx.actor })
				.where(and(eq(notes.id, query.id), eq(notes.workspaceId, params.workspaceId)))
				.returning();
			if (!note) {
				return ctx.error(ctx.syncError('not_found', 'Note not found.'));
			}

			return ctx.ok(note);
		}
	}),

	delete: method.delete({
		query: noteByIdSchema,
		output: noteTombstoneSchema,
		async handler({ params, query, ctx }) {
			const deleted = await db
				.delete(notes)
				.where(and(eq(notes.id, query.id), eq(notes.workspaceId, params.workspaceId)))
				.returning({ id: notes.id });
			if (!deleted.length) {
				return ctx.error(ctx.syncError('not_found', 'Note not found.'));
			}

			return ctx.ok({ id: query.id, deleted: true });
		}
	})
}));
```

Resource dependencies such as the database client, clocks, and id generators belong in closures or factory parameters:

```ts
interface CreateNotesResourceDeps {
	readonly db: typeof db;
	readonly clock: Clock;
}

export function createNotesResource(deps: CreateNotesResourceDeps) {
	return resource(noteParamsSchema, (method) => ({
		mutate: method.mutate({
			query: noteByIdSchema,
			input: notePatchSchema,
			output: noteSchema,
			async handler({ params, query, input, ctx }) {
				const [note] = await deps.db
					.update(notes)
					.set({ ...input, updatedAt: deps.clock.now() })
					.where(and(eq(notes.id, query.id), eq(notes.workspaceId, params.workspaceId)))
					.returning();
				if (!note) {
					return ctx.error(ctx.syncError('not_found', 'Note not found.'));
				}

				return ctx.ok(note);
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
	readonly environment?: unknown;
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

Use `ctx.signal` for DB and network cancellation. `environment` is opaque server-owned execution state; application code
must not inspect or populate it. Use `ctx.actor` for audit/permission-aware writes. Use `ctx.ok`, `ctx.error`, and
`ctx.syncError` so plain handlers stay on the local result shape.

Resource handlers are wrapped so thrown errors become typed sync errors and trigger error hooks.

Handlers also receive `self`, a runtime sibling-method object for calling the same resource without manager auth, outbox, realtime fanout, or mutation finality. `self` auto-binds current `params`, `signal`, `meta`, `actor`, and mutation context. Use it only for internal canonical reads/writes where bypassing manager sync behavior is intentional.

## Resource Outputs And Sync Changes

The common handler return is the domain output:

```ts
return ctx.ok(note);
```

The manager can infer normal sync changes for writes from method shape and ids:

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

Read responses do not get synthetic replay cursors. A `list` or `get` read without a `syncCursor` gets the scope's latest outbox cursor when the outbox implements `latestCursor` (captured before the read handler runs); a `list` or `get` read then returns an envelope with that cursor and no changes: the page itself travels once, in the response value. A `list` read that returns explicit `changes` keeps them in its envelope. With an outbox without `latestCursor`, a read returns no cursor. A resource can also return a real persisted cursor through `syncCursor` or explicit changes that include it. Writes with state changes get manager-generated cursors and are persisted through the outbox/finality path.

The client store's cursor is the one its latest read returned, or the newest live envelope applied after that read started. A read without a cursor clears it, so a cursor restored from cache or set by a reset is never sent once a read has replaced the data it described.

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
import { manager } from 'sync-resource/server';

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
- each duplicate caller can cancel its own wait without cancelling or delaying the caller that owns the shared execution
- successful writes with authoritative changes are recorded as `finalized`
- successful writes without an envelope are recorded as `acknowledged`
- a write whose mutation id the store or caller did not supply gets a generated id; it can never match a stored or in-flight record, so the manager skips the duplicate lookup and the arguments hash, and stores `argsHash: ''` in its record

Production persistence should enforce durable uniqueness for `(scope, mutationId)` and should write source data, mutation record, and outbox envelope atomically where possible.

In-memory persistence keeps mutation records only for a bounded idempotency window. Do not rely on memory persistence for replay or mutation finality across API restarts.
The package currently ships only memory persistence. Durable production persistence is app-owned so the storage schema, partitioning, TTL, and source-write atomicity can match the backing database.

## Persistence, Source Commit, And DB Cost

The core persistence boundary is DB-agnostic:

```ts
interface OperationExecution {
	readonly signal: AbortSignal;
	readonly environment?: unknown;
}

interface ManagerOutbox {
	append(envelope: SyncEnvelope, execution?: OperationExecution): Awaitable<void>;
	readAfter(
		scope: string,
		cursor: string,
		limit: number,
		execution?: OperationExecution
	): Awaitable<ManagerOutboxRead>;
	latestCursor?(scope: string, execution?: OperationExecution): Awaitable<string | undefined>;
}

interface ManagerSyncPersistence extends ManagerOutbox {
	readMutation(
		scope: string,
		mutationId: string,
		execution?: OperationExecution
	): Awaitable<ManagerMutationRecord | undefined>;
	recordMutation(record: ManagerMutationRecord, execution?: OperationExecution): Awaitable<void>;
}
```

`OperationExecution` carries the operation `signal` plus opaque server-owned execution state. Persistence implementations
should observe the signal while reading. Finality writes that start after a successful source commit are allowed to finish
before caller interruption completes, so a committed mutation cannot lose its mutation record or outbox envelope.

Cursor contract:

- Cursors are opaque replay tokens scoped to one manager scope.
- `readAfter(scope, cursor, limit)` must return envelopes strictly after that cursor when it is retained.
- If the cursor is not retained, return `cursorFound: false` and the retained count; the stream always emits a reset for an unknown cursor, including when nothing is retained. The stream reads up to `replayLimit + 1` envelopes (manager option `replayLimit`, default 1000) and emits a reset instead of a partial replay when more than `replayLimit` are pending.
- `latestCursor(scope)` is optional and returns the cursor of the newest retained envelope of the scope. For a scope with no retained envelope it returns an origin cursor that `readAfter` accepts as the start of the scope's history until trimming may have dropped an envelope (`MemoryManagerOutbox` uses one origin per instance, so an origin from another process is unknown and resets; it also resets once the scope went idle and was pruned to its newest envelope). Return `undefined` only when the outbox cannot name a position. Every returned cursor must be one `readAfter` can find. Effect outboxes and persistence take the same optional `latestCursor(scope)` returning an Effect.
- Manager-generated cursors include process entropy to avoid cross-process token collisions, but they are not a durable ordered sequence by themselves.
- Durable persistence should make replay ordering stable across processes. If the backing store owns sequence numbers, the resource/commit path should return that persisted cursor through `syncCursor`.
- Read/list responses must not invent replay cursors that are not present in durable persistence. When a `list` or `get` read returns no `syncCursor` and the outbox implements `latestCursor`, the manager attaches the scope's latest outbox cursor, captured before the read handler runs, so replay covers commits that land between the read and the stream registration.

Use `sourceCommit` when a resource needs the manager-supplied mutation record and envelope before committing the source DB write:

```ts
return ctx.ok({
	output: note,
	changes: [change.itemUpdated({ id: note.id, value: note })],
	sourceCommit: {
		async commit({ mutationRecord, envelope }) {
			await db
				.update(notes)
				.set({ ...note, syncRecord: mutationRecord, syncEnvelope: envelope })
				.where(and(eq(notes.id, note.id), eq(notes.workspaceId, note.workspaceId)));

			return ctx.ok({ syncPersisted: true });
		}
	}
});
```

This lets a resource use DB-native transactions, batch writes, or stored procedures without forcing the core sync API
to know about that DB.

Plain `sourceCommit.commit` has only `mutationRecord` and `envelope` in its public enumerable record shape. A
non-enumerable cancellation signal remains available for existing abort-aware commit implementations; the opaque Effect
execution environment is never exposed. The Effect entrypoint uses the same private channel when lowering an Effect
commit to this plain contract.

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
import { and, eq } from 'drizzle-orm';

import { db, notes } from './db.js';

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
			const [patched] = await db
				.update(notes)
				.set(item.input)
				.where(and(eq(notes.id, item.query.id), eq(notes.workspaceId, params.workspaceId)))
				.returning();

			if (!patched) {
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
				value: { output: patched }
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
import { httpSharedSyncStream } from 'sync-resource/server';

export function handleSyncResourceStream(request: Request): Promise<Response> {
	return httpSharedSyncStream(request);
}
```

Mount the batch connect route once as well. The browser sends every registration queued in the same microtask as one
request (see [Batched registration](#batched-registration)):

```ts
import { httpSharedSyncConnect } from 'sync-resource/server';

export function handleSyncResourceConnect(request: Request): Promise<Response> {
	return httpSharedSyncConnect(request, handleApplicationRequest);
}
```

`handleApplicationRequest` is the application's own request handler. `httpSharedSyncConnect` calls it once per item
with a `POST` `Request` built from the item URL, so each item runs through the manager's normal `/connect` route with
its own params, context, and authentication.

`manager.http` has one handler per method the resource defines, plus `connect`:

```ts
export function connectNotes(request: Request, workspaceId: string): Promise<Response> {
	return notesManager.http.connect({
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

The batch route (`/api/sync-resource/connect` by default) is separate from the per-manager routes and is `POST` only.

Realtime uses one shared SSE stream plus manager `/connect` routes for logical subscriptions. There is no per-manager
SSE route.

Transport details:

- `list` reads `query` from the URL search parameter as JSON.
- `delete` can read `query` from the URL when no JSON body is present.
- Writes send JSON bodies shaped like `{ input }`, `{ query, input }`, `{ query }`, or arrays of those shapes.
- Protocol parameters such as `clientId` and `mutationId` are excluded from resource query parameters.
- `x-client-id` identifies the browser runtime for optimistic finality origin checks.
- `x-mutation-id` is generated by the store for writes.
- The manager reads `after` from the query string for replay.
- When a host passes `options.signal`, the manager composes it with `request.signal`; either signal cancels the same
  operation, including when either one is already aborted.

If a framework pre-reads the request body, its adapter must rebuild a clean `Request` before calling the manager.
Manager parsing remains the single synchronization transport boundary.

### Effect Server API

Use `sync-resource/server/effect` when resource IO and manager hooks are Effects. Its `resource` and `manager` exports
retain the same schemas and protocol behavior as `sync-resource/server`, while direct operations use the typed Effect
failure channel and caller requirements:

Effect is an optional peer dependency (`effect >=4.0.3 <5`): install it only for this explicit entry point. Core,
shared, client, and Promise-native server imports never load Effect.

```ts
import { eq } from 'drizzle-orm';
import { Effect } from 'effect';
import { syncError } from 'sync-resource/server';
import { manager, resource } from 'sync-resource/server/effect';

import { Database, notes } from './db.js';

const notesResource = resource(noteParamsSchema, (method) => ({
	list: method.list({
		query: notePageQuerySchema,
		output: notePageSchema,
		handler: ({ params, query }) =>
			Effect.gen(function* () {
				const db = yield* Database;
				const items = yield* Effect.tryPromise({
					try: () =>
						db.select().from(notes).where(eq(notes.workspaceId, params.workspaceId)).limit(query.limit),
					catch: (cause) => syncError('internal', 'Notes read failed.', { cause })
				});
				return { items };
			})
	})
}));

const program = Effect.gen(function* () {
	const notesManager = yield* manager({
		key: 'notes',
		resource: notesResource,
		authorize: (context, params) => workspaceAccess(context, params), // returns an Effect
		scope: (params) => params.workspaceId
	});

	return yield* notesManager.bind({ workspaceId }).list();
});
```

`manager(...)` is an Effect that requires the services its hooks use; `resource(...)` is a plain constructor. The
manager's `.http` handlers and `.type` work exactly as in the Promise API.

Effect resource handlers return a bare output or structured resource output. Expected failures use the Effect failure
channel rather than `SyncResult`. Their `self`, direct resource, and bound manager methods are Effects. Every method exposes
the union of the resource's handler and source-commit service requirements, so sibling `self` calls and manager-bound calls
retain the complete requirement type.

Direct resource operations provide the caller Context to handlers, batch handlers, `self` calls, and source commits.
Resource-level telemetry and `handleError` deliberately run with `Context.empty()` and require no services; they receive
the typed resource telemetry data but cannot read caller services.

Manager operations merge the caller Context with the manager's long-lived base Context for handlers, commits,
authorization, persistence, manager telemetry, and manager error hooks. The base omits the startup `Scope` and parent span
so later calls cannot retain closed scopes or stale trace parents. Context travels through opaque server-owned execution
state, never through public `meta`. A realtime `subscribe` Effect receives its own `Scope`; its finalizers run when the
last manager-owned consumer leaves, including while subscription acquisition is still pending. Detached realtime hooks
use the sanitized manager base. `manager(...)` is therefore an Effect, while `resource(...)` is a plain definition
constructor.

Effect operation options omit `signal` and the opaque execution environment. Fiber interruption owns caller cancellation,
while `timeoutMs` remains a typed kernel timeout. Interruption reaches authorization, persistence reads, handlers,
`self` calls, and source commits, and waits for their cancellation finalizers and error hooks. After a source commit
succeeds, required persistence finality finishes before interruption completes; this prevents a committed mutation from
losing its finalized record or envelope. Manager telemetry and error hooks receive typed `aborted` cancellation while the
interrupted Effect caller retains its Interrupt Cause.

A Cause becomes a typed `SyncError` failure only when it contains exactly one expected SyncError failure. Composite Causes,
defects, and unexpected interruptions retain their full Cause for direct Effect calls; HTTP still converts them to the
kernel's internal-error response. A failing detached `stream.onScopeIdle` hook is reported once through `handleError` when
provided. Failure of that terminal error hook is contained rather than recursively reported.

The same entrypoint mounts the existing Web handlers in Effect's unstable `HttpRouter`. It preserves the original request
body, headers, and abort signal; it does not parse, authorize, or otherwise reinterpret a sync request. Route Context is
threaded to Effect resource handlers.

```ts
import { Effect, Layer } from 'effect';
import { HttpRouter } from 'effect/http';
import { httpSharedSyncConnectRoute, httpSharedSyncStreamRoute, managerHttpRoutes } from 'sync-resource/server/effect';

const SyncRoutes = Layer.mergeAll(
	HttpRouter.addAll([
		httpSharedSyncStreamRoute('/api/sync-resource/stream'),
		...managerHttpRoutes({
			manager: notesManager,
			path: '/api/workspaces/:workspaceId/notes/sync',
			params: () => Effect.map(HttpRouter.params, ({ workspaceId }) => ({ workspaceId }))
		})
	]),
	httpSharedSyncConnectRoute('/api/sync-resource/connect')
);
```

`httpSharedSyncConnectRoute(path)` is a Layer that requires the `HttpRouter`. It dispatches every batched connect
through that router's full handler (`router.asHttpEffect()`), so route middleware provided to the layer and global
middleware, such as authentication, run once per item, and `params`/`context` resolve against each item's own path.
Provide the same route middleware to it that you provide to the manager routes. The dispatch reuses the request's
Effect context rather than creating a runtime.

`managerHttpRoutes` mounts only the manager methods that exist: `get`, `list`, `add`, `mutate`, `delete`, and `connect`.
The `params` and optional `context` resolvers are Effect values, so application authentication and path decoding
remain in the host router rather than being duplicated by Sync Resource.

The adapter does not create a `ManagedRuntime`, accept a `Layer`, or own another runtime lifecycle. For Web-native request
sources, Effect returns the original `Request`; server-side route interruption therefore cannot replace its signal, though
client abort still reaches the kernel. Node-native HTTP requests receive the adapter signal. While a kernel handler is
pending, route interruption cancels that Web request and waits for the kernel promise and its cancellation finalizers to
settle. Kernel-internal defects keep the plain API contract and surface as typed `internal` failures; defects raised by
Effect resource handlers are restored as defects for direct Effect calls and become internal HTTP responses at the
transport boundary.

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
POST /api/sync-resource/stream
POST /api/sync-resource/connect
POST <manager-sync-route>/connect
```

The `sync-resource` route and internal coordination namespace are compatibility-sensitive protocol state.
Changing the default stream path, cache-key prefix, browser storage keys, broadcast channel, lock name, or server
realtime channel prefix would disconnect existing clients and rolling server deployments. New applications can choose
a different stream or connect URL explicitly through `configureSync(...)`.

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
- When `getParams()` changes the store's scope, the store ends the old scope's registration and registers the new one on
  its next `hydrate()` or `connect()`.
- Reconnect re-registers active logical subscriptions with their latest cursor.
- The tab that owns the physical stream sends its first `/connect` registrations after the first stream attempt
  resolves (opened or failed), so a registration is never sent twice for one stream. Follower tabs register immediately.
  A `subscribe` call therefore resolves no earlier than the owner's first stream response.
- The owner tab relays each envelope's JSON text to follower tabs on the `sync-resource:v2:transport` BroadcastChannel.
  A follower parses and validates it like a stream frame. Tabs running a different relay version use a different channel,
  so they share no envelopes until the older tab reloads.
- A failed `/connect` keeps the logical subscription and retries registration with backoff (1s doubling to 30s) until it succeeds, the stream reconnects, or the last subscriber leaves. The error surfaces as the store error, not in `hydrate()`'s result. Its code is the one in the server's error body; a failure with no sync error body (such as a proxy answering 401 or 503) is classified by its HTTP status.
- Registrations are batched by default; see [Batched registration](#batched-registration).
- Replay/reset remains per logical scope through durable outbox cursors. An unknown cursor, or more pending envelopes than
  `replayLimit`, produces a reset instead of a partial replay.
- While a scope replays, live envelopes for it are buffered and sent after the replayed ones, in order. Overlapping
  replays of one scope keep buffering until the last one ends.
- A read response never overwrites newer realtime state: envelopes applied while a read was in flight are re-applied over
  its output in arrival order, and the cursor stays at the last of them.
- `/connect` returns the authoritative encoded scope before replay is delivered.
- Each pending `/connect` wait is independently abortable; cancelling one waiter does not cancel establishment while
  another consumer still owns it.
- Manager `stream.onScopeIdle` is only for releasing manager-owned ephemeral per-scope memory. Idle cleanup is routed
  only to the manager that owns that encoded scope. When a scope goes idle, the manager also drops its mutation records
  from memory persistence and releases the scope's realtime bus subscription; `MemoryManagerOutbox` keeps only the
  scope's newest envelope. A client exactly at that cursor replays nothing, while any older cursor and the scope origin
  reset and repair.
- Shared-stream replay failure reports through the manager error hook with method `stream.replay`, emits a
  `replay_failed` reset when possible, and closes the stream so reconnect/repair owns recovery. Physical disconnect
  cancels pending replay through the physical response stream's owned signal, clears replay buffers, and is normal
  termination rather than a reported failure.
- Ping events are ignored.
- Malformed chunk frames—including fractional indexes/counts—, relayed envelope JSON, and non-finite owner-lock expiries
  are discarded before they affect client state.
- Server streams use bounded event size, bounded backpressure, heartbeat, stale transport TTL, and per-transport/IP limits.
  Request abort, reader cancellation, oversize, backpressure, and enqueue failure all perform the same idempotent cleanup.
- Client SSE limits count UTF-8 bytes, each concurrently read stream owns its stateful decoder, and terminal protocol
  rejection aborts the old fetch owner before reconnect.
- The connection cap is per browser transport id plus IP. This limits duplicate streams from one browser session without punishing many real users behind one NAT. It is not a standalone per-IP abuse ceiling.
- Stream close/cancel clears per-stream cursor replay buffers immediately, so high-fanout connections do not retain cursor bookkeeping after disconnect while waiting for GC. The per-stream cursor window holds only replayed cursors, so live sends add no retained memory per stream.

Realtime is required, but correctness comes from cursor replay and repair. A missed SSE event is not data loss if the outbox can replay or trigger reset.

## Cross-Process Realtime

Use a manager realtime bus to publish committed envelopes to other API processes:

```ts
import { createPubSubManagerRealtimeBus } from 'sync-resource/server';

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

The transport wrapper can be Redis, NATS, Azure Service Bus, or another pub/sub system. Bus failures must not invalidate
committed writes; durable outbox replay is still the correctness backstop.

One manager-owned entry coordinates each scope. Successful shared-stream `/connect` calls retain one deduplicated lease
until that scope becomes idle. Concurrent consumers share one subscription establishment. Each waiter can cancel
independently; only the last lease leaving aborts a pending establishment or runs the acquired unsubscribe function.

The manager publishes every committed envelope to the bus and holds the subscription for connected scopes, but it passes
the bus a no-op envelope callback. A shared stream delivers the envelopes this process commits; envelopes that arrive
from the bus are not forwarded to it. Sync Resource targets a single server instance, and a client reconnecting after a
gap recovers through outbox replay and reset.

`ManagerRealtimeBus.subscribe` and `ManagerRealtimePubSubTransport.subscribe` receive an optional third
`OperationExecution` argument. Implementations must observe `execution.signal` while acquiring the subscription and stop
the underlying work when it aborts. The Effect adapter uses that signal to interrupt the acquisition fiber and close its
Scope, so even a never-settling Effect runs its finalizers when the last lease leaves.

A failed shared establishment is reported once with method `realtimeBus.subscribe` and remains retryable by the next consumer. A failed unsubscribe is
reported once with method `realtimeBus.unsubscribe` and is terminally contained. A failed best-effort publish is reported
once with method `realtimeBus.publish`; neither failure invalidates a committed write.

### Batched registration

The browser sends every registration queued in the same microtask as one `POST /api/sync-resource/connect` request. A
page boot that opens 13 stores sends one request. The body lists each registration's manager `/connect` URL, including
its `after` replay cursor:

```json
{ "connects": ["/api/notes/sync/connect?after=cursor-1", "/api/tasks/sync/connect"] }
```

The server (`httpSharedSyncConnect` or `httpSharedSyncConnectRoute`) runs each item as a `POST` through the application's
normal per-manager connect route, forwarding the request headers (`x-sync-transport-id`, `x-client-id`, cookies,
authorization) minus `content-length`, `content-type`, and `transfer-encoding`. It answers `200` with one result per
item, in request order:

```json
{
	"results": [
		{ "status": 200, "body": { "isOk": true, "isError": false, "value": { "scope": "..." } } },
		{ "status": 401, "body": {} }
	]
}
```

Each `body` is exactly what the manager's `/connect` route returned, so the client applies its normal connect handling
per item. A failed item fails and retries alone with the usual registration backoff; a failed or malformed batch
response fails and retries every item. A dispatch that throws becomes `{ status: 500, body: <internal error> }` for that item only.

Rules:

- A body is rejected with `400` unless `connects` is an array of 1 to 64 relative paths that start with a single `/`,
  stay on the request's origin, and end in `/connect`. The client sends a larger queue as several requests of at most 64.
- An item aborted before the flush is not sent. An item aborted after the flush is still sent and its result is ignored.
- The client sends each store URL on the batch route's origin as its path. A store URL on another origin connects directly.
- While any store read is in flight the transport holds queued registrations (`RuntimeTransport.holdRegistrations`, optional for custom transports) and sends them together a task after the last read ends, or after two seconds. A boot whose stores finish their reads at different times still sends one request; each read's replay cursor covers the wait.
- Registrations still wait for the owner's first stream attempt, so a boot sends one batch. Reconnect re-registrations
  batch the same way.

## Frontend Runtime Configuration

Configure sync once near app startup:

```ts
import { configureSync } from 'sync-resource/client/core';

import { IndexedDbCacheAdapter } from './syncResourceIndexedDbCache.js';

const DAY = 24 * 60 * 60 * 1000;

configureSync({
	fetch: globalFetch,
	streamUrl: '/api/sync-resource/stream',
	connectUrl: '/api/sync-resource/connect',
	cache: {
		adapter: new IndexedDbCacheAdapter('Sync Resource', 'store'),
		ttlMs: 14 * DAY
	}
});
```

Cache adapter shape (the application implements it, typically over IndexedDB):

```ts
interface CacheAdapter {
	readonly kind?: 'memory' | 'idb';
	get<TValue>(key: string): Promise<CacheItem<TValue> | undefined>; // CacheItem is { value, expiry }
	set<TValue>(key: string, value: CacheItem<TValue>): Promise<void>;
	del(key: string): Promise<void>;
	keys?(): Promise<readonly string[]>; // optional: enables the expired-snapshot sweep
}
```

Runtime options:

- `streamUrl` defaults to `/api/sync-resource/stream`.
- `connectUrl` defaults to `/api/sync-resource/connect`, the batch connect route.

Rules:

- Do not pass runtime/fetch/transport/cache adapter into individual stores.
- The app fetch/auth/session-cookie behavior belongs in one `configureSync(...)`.
- The cache adapter is app-wide.
- A snapshot is deleted when its store reads it after expiry. An adapter that implements the optional `keys()` also lets the runtime remove expired snapshots of scopes that are never opened again: one sweep, 60 seconds after `configureSync`, at most once a day across tabs and reloads (tracked by a `sync-resource:cache-sweep` entry the sweep writes through the adapter). The sweep deletes every expired entry `keys()` returns, so use an adapter store dedicated to Sync Resource. Entries with `expiry: 0` never expire.
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
import { createStore } from 'sync-resource/client/core';

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
				return note.status === query?.status;
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
import { configureSync, createStore } from 'sync-resource/client/core';

import { cacheAdapter } from './cache.js';

configureSync({
	fetch: (...args) => fetch(...args),
	streamUrl: '/api/sync-resource/stream',
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
starts realtime delivery. It resolves once the read is applied; the registration continues in the background, and the
read's replay cursor covers the time until it completes. `connect()` awaits the registration.

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

## Framework Adapters

The plain JavaScript API in this guide is the behavioral reference for every adapter. Framework adapters create the
same core store and change only reactive reads and lifecycle ownership:

| Core JavaScript | Solid         | Vue               | React            |
| --------------- | ------------- | ----------------- | ---------------- |
| `items()`       | `items()`     | `items.value`     | `useItems()`     |
| `data()`        | `data()`      | `data.value`      | `useData()`      |
| `isPending()`   | `isPending()` | `isPending.value` | `useIsPending()` |
| `list(query)`   | `list(query)` | `list(query)`     | `useList(query)` |
| explicit owner  | Solid owner   | Vue effect scope  | explicit owner   |

- [Solid adapter](./solid.md)
- [Vue adapter](./vue.md)
- [React adapter](./react.md)

Actions such as `hydrate`, `refresh`, `loadMore`, `add`, `mutate`, and `delete` keep their names and `SyncResult`
behavior. Each adapter exposes the underlying store as `store.core`. Framework peers remain optional, and importing
`sync-resource/client/core` does not load a framework.

### Custom Adapter Authoring

`sync-resource/client/core` exports `bindStoreEvents`, `StoreAdapterEventSinks`, `StoreAdapterEventSource`,
`StoreActionMethods`, and the conditional `StoreWith*` types. These keep custom adapters tied to the public store
contract without importing client internals. Live-only signals remain dynamically keyed and are subscribed through
`store.on.signal(type, callback)`.

## Store State And Events

Core store exposes:

```ts
interface StoreSnapshot<TManager> {
	readonly data: OutputOf<TManager, 'get'> | undefined;
	readonly items: readonly CollectionItem<TManager>[];
	readonly listMeta: CollectionMeta<TManager> | undefined;
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
store.on.listMeta((meta) => {});
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

`snapshot()` and coarse `subscribe(snapshot)` read the whole state. Consumers should prefer typed event slots so
unrelated UI subscriptions do not rerun.

## Cache-First Behavior

The client cache defaults to a long TTL, normally two weeks.

Rules:

- If cached data exists, display it immediately.
- If cached data exists and the store is hydrating, show the cached data plus a background refresh indicator.
- If no cached data exists and hydration is in flight, show loading UI in the content area.
- If no data exists and hydration is not in flight, show empty/no-data UI.
- Store construction is network-free.
- `hydrate()` restores cache, reads from server, persists fresh authoritative state, and starts the realtime
  registration without waiting for it. A registration failure is reported as the store error and retried by the
  transport; `hydrate()`'s result reports only cache and read failures.
- Public `hydrate()` is successful-once per store/scope lifetime: concurrent calls share the active attempt, later calls
  reuse a completed success without a read (they only resubscribe a failed registration), and failed attempts remain
  retryable. `isHydrating()` stays true through cache restore and the authoritative read.
- Reconnect catch-up forces a new authoritative hydration even after public hydration succeeded. Explicit `refresh()`
  and repair/reset reads also remain independent of the public hydration success.
- `restore()` only loads authoritative cache. In-memory pending commands are preserved, but pending commands are not restored from IndexedDB.
- `connect()` attaches realtime from the current cached cursor without forcing a server refresh.
- IndexedDB stores only last-success authoritative state. Optimistic pending commands stay memory-only.
- A snapshot holds `baseData`, the loaded `baseItems` once, and per query family (at most 8) its ids, page records (cursors, coverage,
  size, source), sort flag and meta, plus the active family and cursor. It is stored under `<scope>:view:v2`, so a
  snapshot saved in an older format sits under another key, is never read, and the store refetches.
- Cache writes use one timer, armed by the first dirty event and set to 250 ms. Later dirty events do not reset it, so bursts and continuous realtime traffic write at most once per 250 ms. Events that arrive during an in-flight write arm the next timer after the current write completes. The store passes plain values to the cache adapter; the adapter/library owns IndexedDB storage details.

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
		matchesQuery: (note, { query }) => note.status === query?.status,
		compare: (left, right) => right.createdAt.localeCompare(left.createdAt)
	})
);
```

`itemId` receives `{ params }`, `targetId` receives `{ params, query, input }`, and `matchesQuery` receives `{ params, query }`. Use params for identity when the resource method omits query because the scoped params already identify the one item.

`matchesQuery` is optional. Use it when realtime external adds can be safely checked against the current page query. If it returns false, the entity is not added to the family's visible page ids; the store keeps an entity only while a loaded family holds it.

`compare` is optional. Use it when user-visible order is domain-specific.

`addPlacement: 'sorted'` is an opt-in for authoritative acknowledgements of the current browser's `add` writes. It
uses the same `matchesQuery` and `compare` reconciliation as external realtime adds across every loaded query family,
so a partially loaded page keeps its sorted window as later realtime changes arrive. Without this option, local add
acknowledgements append to the active family's first loaded page.

`addPlacement: 'sorted'` requires `compare`; store construction throws when the option is supplied without an
ordering function. The client does not infer an order or refetch to guess placement, so provide `compare` when sorted
acknowledgement placement is needed.

With sorted placement enabled, an item that belongs inside a partially loaded window grows the loaded ids instead of
trimming the oldest id. The page's existing `cursorOut` remains the boundary for `loadMore()`, so later pagination can
continue without skipping the row that was previously at that boundary. This applies to the acknowledged local add
and to subsequent external realtime adds in the same loaded families.

For realtime external `itemAdded` on a loaded page window:

- if `matchesQuery` rejects the item, keep it out of the visible window
- if the loaded family has one complete empty page and `matchesQuery` accepts the item, insert it into that page
- if `compare` is missing, keep other non-empty entities cached but do not guess page placement
- if `compare` proves the item sorts into a non-complete loaded window, insert and trim to the current loaded capacity
  (unless `addPlacement: 'sorted'` is enabled, which preserves the existing cursor boundary by growing the loaded ids)
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
- The store keeps one normalized entity cache and up to 8 page families. A family keeps its loaded ids and items in two
  aligned arrays and records only each page's size, so `pages()` derives each page's `ids` from the family.
- The entity cache holds exactly the entities some family holds. A realtime entity that no loaded window holds, an entity
  dropped from a window, and the entities of an evicted family are released instead of retained.
- Creating a ninth family evicts the least recently used one. A family counts as used when its handle reads `items()`,
  `meta()` or `pages()`, when a page loads into it, and while it is the active family, which `items()` reads and is never
  evicted. A handle for an evicted family reads empty (`[]`, no meta) until its `refresh()` or `loadMore()` loads it
  again from the first page. Snapshots hold only the families that remain.
- A patch-only `itemUpdated` (no `value`) applies to an entity the store holds, and is ignored for one it does not hold,
  because the full item cannot be built. Send `value` when an update can move an entity into a window it was outside.
- Ids are unique across a family's pages: a page that repeats an id the family already holds, such as one a realtime add
  inserted, leaves the existing entry in place.
- Visible order is `compare` order when `compare` is set. Items that compare equal, and every item when `compare` is not
  set, keep the family's page order.
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
- Membership/order uncertainty leaves the entity out of the window or marks affected pages stale when repair is required.
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
import { and, desc, eq, lt } from 'drizzle-orm';

import { db, notes } from './db.js';

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
		const limit = query.limit;
		const rows = await db
			.select()
			.from(notes)
			.where(
				and(
					eq(notes.workspaceId, params.workspaceId),
					query.cursor ? lt(notes.createdAt, decodeNoteCursor(query.cursor)) : undefined
				)
			)
			.orderBy(desc(notes.createdAt))
			.limit(limit + 1);

		const items = rows.slice(0, limit);
		const meta = query.cursor ? undefined : await buildNoteListMeta(params, query);
		return ctx.ok({
			items,
			pageCursor: rows.length > limit ? encodeNoteCursor(items[items.length - 1]) : undefined,
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
				return note.status === query?.status;
			},
			compare(left, right) {
				return right.createdAt.localeCompare(left.createdAt);
			}
		})
);

await notes.hydrate();
await notes.loadMore();
```

`refresh()` loads the first page for the default query family. `loadMore()` reuses the default family cursor and adds the next page. For concurrent list families, call `store.list(query).refresh()` and `store.list(query).loadMore()`. UI code should trigger this through a shared infinite-scroll component. If a realtime item belongs outside all loaded windows, visible lists stay unchanged without a refetch and the store does not retain the item.

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

- `retention_gap`: the cursor is unknown to the outbox, or more envelopes are pending than `replayLimit`
- `replay_failed`: reading the outbox for replay failed
- `manual`, `auth_changed`, `schema_changed`, `cache_policy_changed`, `repair`: for resources that return `change.reset(manifest)`

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
9. Mount the shared stream handler and the batch connect handler once for the host application.
10. Configure frontend sync once with application fetch and an application-owned IndexedDB cache adapter.
11. Create frontend stores with only manager type, params, URL, query, cache policy, and optional reconcile.
12. Branch on `SyncResult` for frontend actions.
13. Run targeted conformance tests when touching synchronization behavior.

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
- Mounting the stream route but not the batch connect route.
- Treating HTTP ACK as finality.
- Clearing pending state without authoritative envelope/reset coverage.
- Persisting optimistic pending overlays or pending metadata in IndexedDB.
- Writing broad cross-partition database queries inside `list`.
- Using full-list hydration for large collections.
- Adding defensive normalization when schemas already cover the boundary.
- Duplicating state that can be derived from data, pending, hydrating, refreshing, and error.
