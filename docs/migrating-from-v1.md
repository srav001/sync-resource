# Migrating From v1 To v2

v2 keeps the v1 resource, manager, and store APIs. The client and server protocol changed in these ways that can need
action: the browser now registers realtime subscriptions through a batch route, `hydrate()` resolves earlier,
`resetSharedStreamForTests` is no longer exported, the per-manager `/events` route is gone, list read envelopes no longer
carry the page, an idle scope keeps only its newest outbox envelope, and some unused exports, options and fields are
removed. Everything else is additive or automatic.

## Checklist

1. Mount the batch connect route on the server. [Details](#1-mount-the-batch-connect-route)
2. Deploy the server before the v2 client. [Details](#1-mount-the-batch-connect-route)
3. Check code that relied on `hydrate()` also meaning "realtime is registered". [Details](#2-hydrate-resolves-when-the-read-is-applied)
4. Remove `resetSharedStreamForTests` imports. [Details](#3-resetsharedstreamfortests-is-no-longer-exported)
5. Only if you wrote your own outbox, persistence, or realtime bus: review [custom persistence](#4-custom-outbox-persistence-or-realtime-bus).
6. Only if you used `events: true`, `http.events`, or a `/events` URL: [use the shared stream](#5-replace-the-direct-events-route-with-the-shared-stream).
7. Only if code reads raw `list` responses: [read the page from `value`](#6-list-read-envelopes-no-longer-carry-pageloaded).
8. Only if you rely on replay for clients that were offline longer than the idle TTL: [review idle scopes](#7-idle-scopes-keep-only-their-newest-envelope).
9. Only if you imported the `Result` helpers: [they are removed](#8-result-helpers-are-removed).
10. Only if you used one of the exports, options or fields in [section 9](#9-removed-exports-options-and-fields): remove it.
11. Nothing else is required. Node and framework peers are unchanged; `effect` is an optional peer used only by
    `sync-resource/server/effect` ([requirements](#requirements)).

Then adopt what you want from [Optional additions](#optional-additions). Automatic behavior changes need no code and
are listed in [No code change needed](#no-code-change-needed).

## Required changes

### 1. Mount the batch connect route

**What changed.** v1 sent one `POST <manager>/connect` per store. v2 sends every registration queued in the same
microtask as one `POST /api/sync-resource/connect` (a boot that opens 13 stores sends one request).

**Why it matters.** Without the route, each registration fails with `not_found` (`Sync request failed: 404`), the
error appears as the store error, and the client retries with backoff up to 30 seconds. Reads, writes, and cache still
work; realtime does not.

**Before (v1).** Server mounted the shared stream and the per-manager routes only:

```ts
import { httpSharedSyncStream } from 'sync-resource/server';

export function handleSyncResourceStream(request: Request): Promise<Response> {
	return httpSharedSyncStream(request);
}
```

**After (v2), Promise server.** Mount the batch route once. `handleApplicationRequest` is your app's own
`(request: Request) => Promise<Response>` handler; the batch route calls it once per item with a `POST` built from the
item URL, so each item runs through your normal router, authentication, and manager `/connect` route:

```ts
import { httpSharedSyncConnect, httpSharedSyncStream } from 'sync-resource/server';

export function handleSyncResourceStream(request: Request): Promise<Response> {
	return httpSharedSyncStream(request);
}

export function handleSyncResourceConnect(request: Request): Promise<Response> {
	return httpSharedSyncConnect(request, handleApplicationRequest);
}
```

**After (v2), Effect server.** `httpSharedSyncConnectRoute(path)` is a Layer that requires the `HttpRouter`; provide the
same route middleware to it as to your manager routes:

```ts
import { Layer } from 'effect';
import { HttpRouter } from 'effect/http';
import { httpSharedSyncConnectRoute, httpSharedSyncStreamRoute } from 'sync-resource/server/effect';

const SyncRoutes = Layer.mergeAll(
	HttpRouter.addAll([httpSharedSyncStreamRoute('/api/sync-resource/stream') /* , ...managerHttpRoutes(...) */]),
	httpSharedSyncConnectRoute('/api/sync-resource/connect')
);
```

**Different path.** Pass it on the client:

```ts
configureSync({ cache: { adapter }, connectUrl: '/internal/sync/connect' });
```

Notes:

- Keep the per-manager `/connect` routes mounted; the batch route dispatches to them.
- The batch route accepts 1 to 64 same-origin paths ending in `/connect`; the client splits larger queues.
- Deploy order: ship the server with the batch route first. The per-manager routes are unchanged, so v1 clients keep
  using them. A v2 client against a server without the route loses realtime until the route exists.
- A store URL on another origin than the batch route connects directly.

### 2. `hydrate()` resolves when the read is applied

**What changed.** v1 resolved `hydrate()` after cache restore, the read, and the realtime registration, and returned an
error if the registration failed. v2 resolves once the read is applied and registers realtime in the background; the
read's replay cursor covers the gap. A registration failure is reported as the store error and retried; it is no longer
in `hydrate()`'s result. `isHydrating()` covers cache restore and the read only. A repeated `hydrate()` after success
does not read again; it only re-registers a failed registration.

**Before (v1).**

```ts
const result = await notes.hydrate();
if (result.isErr()) {
	showError(result.error); // cache, read, or realtime registration failure
}
```

**After (v2).**

```ts
const result = await notes.hydrate();
if (result.isErr()) {
	showError(result.error); // cache or read failure only
}

const stop = notes.on.error((error) => {
	if (error) {
		showRealtimeProblem(error); // includes a failed realtime registration, which is retried
	}
});

const registered = await notes.connect(); // only when you need "realtime is registered"
```

### 3. `resetSharedStreamForTests` is no longer exported

`sync-resource/server` no longer exports it. There is no public replacement.

**Before (v1).**

```ts
import { resetSharedStreamForTests } from 'sync-resource/server';

afterEach(() => resetSharedStreamForTests());
```

**After (v2).** Isolate tests with their own manager keys and `x-sync-transport-id` values, and cancel the stream readers
a test opens:

```ts
const response = await httpSharedSyncStream(
	new Request('http://sync.test/stream', {
		method: 'POST',
		headers: { 'x-sync-transport-id': `test-${crypto.randomUUID()}` }
	})
);
const reader = response.body!.getReader();
// ... assertions ...
await reader.cancel();
```

### 4. Custom outbox, persistence, or realtime bus

Skip this if you use `MemoryManagerOutbox` and `MemoryManagerRealtimeBus`.

**Replay now resets instead of guessing.** v1 reset only when an unknown cursor met a non-empty scope, and silently sent
the first `replayLimit` envelopes when more were pending. v2 resets for every unknown cursor (including an empty scope)
and whenever more than `replayLimit` envelopes are pending; the stream reads `replayLimit + 1` envelopes to detect
that. Your `readAfter` must report `cursorFound` accurately and honor `limit`:

```ts
const outbox: ManagerOutbox = {
	append: (envelope) => db.append(envelope),
	async readAfter(scope, cursor, limit) {
		const page = await db.after(scope, cursor, limit); // strictly after `cursor`, at most `limit`
		return { envelopes: page.rows, cursorFound: page.found, retainedEnvelopeCount: page.retained };
	}
};
```

**Generated mutation ids.** A write whose mutation id was not supplied (no `x-mutation-id`, no `mutationId` option) gets a
generated id that cannot match an earlier record, so the manager skips the duplicate lookup and no longer hashes its
arguments. The record passed to `recordMutation` has `argsHash: ''` for those writes. If your persistence stores or
indexes `argsHash`, accept the empty string. A supplied id keeps its stable arguments hash and duplicate checks.

**Realtime bus callback.** The `onEnvelope` callback the manager passes to `realtimeBus.subscribe` is a no-op. Keep
implementing `subscribe` (the manager calls it when a client connects and releases it when the scope is idle), but do not
rely on the callback receiving envelopes.

**Optional `execution` argument.** `append`, `readAfter`, `readMutation`, `recordMutation`, `latestCursor`, the
`authorize` hook (third argument), and `realtimeBus.subscribe` / pub-sub `subscribe` now receive
`execution: { signal: AbortSignal }`. Existing two-argument implementations keep compiling and running. Observe the signal
to cancel work:

```ts
// Before (v1)
authorize: (context, params) => canRead(context, params);
// After (v2), optional
authorize: (context, params, { signal }) => canRead(context, params, signal);
```

### 5. Replace the direct `/events` route with the shared stream

**What changed.** The per-manager direct SSE route is removed: the `events: true` option of `managerHttpRoutes`, the
`http.events` handler, and `GET <manager>/events` no longer exist. The browser runtime never used it; it was a
diagnostic stream. Realtime uses the shared stream plus each manager's `/connect` route. Error-hook method labels
changed with it: a failed bus subscribe is reported as `realtimeBus.subscribe` and a failed shared-stream replay as
`stream.replay` (both were `events`).

**Before.**

```ts
managerHttpRoutes({ manager: notes, path: '/api/notes', params, events: true }); // also mounted GET /api/notes/events
const response = await notes.http.events!({ request, params });
```

**After.** Remove `events: true` and the `http.events` call. To read envelopes outside the browser runtime, open the
shared stream and register the scope through `connect`; `after` replays from a cursor:

```ts
const headers = { 'x-sync-transport-id': transportId };
const stream = await httpSharedSyncStream(new Request(streamUrl, { method: 'POST', headers }));
await notes.http.connect!({
	request: new Request(`${connectUrl}?after=${cursor}`, { method: 'POST', headers }),
	params
});
// stream.body now carries hello, ping, sync, and sync-chunk events
```

`connect` still acquires `realtimeBus.subscribe` for the scope and holds it until the scope is idle. The manager passes
the bus a no-op envelope callback, so bus envelopes are not forwarded to the shared stream; run one server instance.

### 6. List read envelopes no longer carry `pageLoaded`

**What changed.** The `envelope` of a `list` read response is `{ managerKey, scope, cursor, changes: [] }`, like a `get`
read. It used to repeat the page as a `pageLoaded` change next to `value`, which doubled the response size. The cursor
is unchanged (the resource's `syncCursor`, then `output.syncCursor`, then the outbox's latest cursor), and the store only
reads `envelope.cursor` from a read. List read envelopes no longer include `sourceClientId` or `metrics`. A `list` handler
that returns explicit `changes` still gets them in its envelope. This only matters to code that reads raw responses.

**Before.**

```ts
const body = await response.json();
const items = body.envelope.changes[0].items;
```

**After.**

```ts
const body = await response.json();
const items = body.value.items;
```

### 7. Idle scopes keep only their newest envelope

**What changed.** When a scope has had no connected stream for `stream.idleTtlMs` (5 minutes by default),
`MemoryManagerOutbox` now keeps only that scope's newest envelope, on top of the mutation records it already dropped. A
manager without an `outbox` option uses a `MemoryManagerOutbox`, so it behaves the same. A client reconnecting with
exactly that cursor resumes without a reset; every older cursor, and the scope's origin cursor, reset and repair.

**Why it matters.** Memory per idle scope is bounded to one envelope. A client that was offline longer than the idle TTL
repairs through a reset instead of replaying.

**Action.** None for the browser client, which repairs automatically. If you need replay for longer-offline clients,
raise `stream.idleTtlMs` or use a durable outbox; custom outboxes are not pruned by the manager.

### 8. `Result` helpers are removed

`Result`, `map`, `mapError`, `andThen`, `trySync`, `tryPromise`, `isOk`, `isError`, `InferOk` and `InferErr` are no
longer exported from `sync-resource/client/core` or `sync-resource/server`. Sync Resource does not use them anymore.

Still exported: `ok`, `err` and the `SyncResult` type. Every `SyncResult` value keeps its `isOk()` and `isErr()` methods.

**Before (v1).**

```ts
import { isOk, Result } from 'sync-resource/client/core';
```

**After (v2).** Remove these imports, and replace the calls with your own code or with `ok`, `err` and the `isOk()` /
`isErr()` methods.

### 9. Removed exports, options and fields

Sync Resource no longer uses any of these. Remove them from your code.

- **Client core:** `createRepairScheduler`, `planRepair`, `noRepairNeeded`, `RepairPlan` and `RepairScheduler`. The store
  repairs itself after a reset or a stale page, and `repair()` is the manual call. The types `StoreActionMethodName`,
  `ParamsOf` and `PageOutputMeta` are no longer exported.
- **Store `unsubscribe(callback)`:** removed. Call the function that `subscribe(callback)` returns.
- **`PageState`:** `coverage`, `repairNeeded` and `lastSyncCursor` are removed, and `source` is `'network' | 'realtime'`.
  `stale` is the one staleness flag.
- **`CacheAdapter.kind`:** removed; a class that declares `kind` keeps compiling, but an object literal typed as
  `CacheAdapter` must drop it.
- **`RuntimeTransport.subscribe`:** the `url` and `streamUrl` options are removed, and `connectUrl`, `managerKey` and
  `scope` are required. The direct per-manager SSE subscription they served is gone with the `/events` route. The
  `streamUrl` option of `configureSync` is unchanged.
- **Solid:** `SolidStoreListHandle` is removed; use `StoreListHandle`. `SolidClientStore` is the core store type without
  `on` and `subscribe`, plus `core`; every member it had is still there.
- **Server:** `parseSchema`, `registerSharedStreamManager`, `registerSharedStreamSubscription`,
  `publishSharedStreamEnvelope`, `getSharedStreamConfig`, `getRegisteredSharedStreamManagerKeys`, and the types
  `ManagerOptions` and `ManagerType`. `GetMethodInput` and `ListMethodInput` are one `ReadMethodInput`.
  `GetResourceArgs`, `ListResourceArgs` and `DeleteResourceArgs` are no longer exported.
- **`ManagerSyncPersistence`, including the Effect persistence:** `append` is removed. The manager never called it;
  `recordMutation` stores the record and appends its envelope. The `outbox` option still takes `append`.
- **Shared stream:** the transport id is read only from the `x-sync-transport-id` header; the `transportId` query
  parameter is gone. The browser client always sent the header.
- **Shared:** `isResetManifest`, `isSyncChange`, `isSyncErrorCode`, `isSyncErrorRecovery`, `isSyncProtocolError` and
  `isSyncSignal`. `isSyncEnvelope` and `isSyncHttpResult` remain.
- **Every entry point:** the `SyncRecoveryMetadata` type is `SyncErrorRecovery`. `normalizeSyncError` has no
  `fallbackMessage` argument; an `Error` with an empty message becomes `Sync operation failed.`.

**Before.**

```ts
const persistence = { append, readAfter, readMutation, recordMutation };
const stale = page.stale || page.repairNeeded;
```

**After.**

```ts
const persistence = { readAfter, readMutation, recordMutation };
const stale = page.stale;
```

## Requirements

- Node `>=22.18.0` (unchanged).
- `effect` `>=4.0.3 <5`, optional peer. Install it only to import `sync-resource/server/effect`. The Promise server,
  shared, and client entry points never load it. Effect v3 is not supported.
- `react`, `solid-js`, `vue` peers: unchanged and optional.
- New public subpath: `sync-resource/server/effect`. New exports from `sync-resource/server`: `httpSharedSyncConnect`,
  and the `OperationExecution` type.

## No code change needed

- A restarted server or trimmed outbox now sends a reset for an unknown cursor; the client repairs automatically.
- Reads carry a replay cursor when the outbox implements `latestCursor` (`MemoryManagerOutbox` does); the read, not a
  cursor restored from cache, defines the store cursor.
- A live envelope that arrives while a read is in flight is re-applied over the read response.
- A failed realtime registration retries with backoff (1 s doubling to 30 s) until it succeeds or the last subscriber leaves.
- The tab that owns the stream registers after its first stream attempt, so a registration is not sent twice.
- A store whose `getParams()` scope changes ends the old registration and registers the new scope on its next
  `hydrate()` or `connect()`.
- Live envelopes for a scope are sent after its replayed envelopes, in order.
- The client discards malformed SSE chunk frames and counts SSE size limits in UTF-8 bytes.
- Effect manager routes answer JSON responses as buffered bodies with the same status and headers; only the shared stream
  stays a stream.
- The cache snapshot format changed, so old browser caches are refetched once.
- Adapters (`sync-resource/client/react`, `solid`, `vue`) are unchanged, except the Solid type names in [section 9](#9-removed-exports-options-and-fields).
- TypeScript signatures that only widened (`Validator.parse`, `OperationMeta`, telemetry contexts now extend
  `OperationExecution`) need no change.
- `GetMethodDefinition`, `ListMethodDefinition`, `AddMethodDefinition`, `MutateMethodDefinition` and `DeleteMethodDefinition`
  are the matching method input type plus `kind`. They describe the same values.
- `manager.http` is typed from the resource: each method the resource defines, plus `connect`, is a required handler,
  so existing `?.` and `!` calls keep working and can be dropped. Methods the resource does not define are not on the type.
  Handler arguments are checked strictly, so a `managerHttpRoutes` `context` callback must produce the manager's context
  type: `Effect.undefined`, not `Effect.void`, for a manager without context.

## Optional additions

### `latestCursor` on a custom outbox

Without it, reads of a custom outbox return no cursor (as in v1). With it, a `list` or `get` read carries the scope's
latest cursor, captured before the handler runs, so replay covers commits that land between the read and the realtime
registration. Return the newest cursor, or an origin cursor `readAfter` accepts for an empty scope:

```ts
const outbox: ManagerOutbox = {
	// append, readAfter as above
	async latestCursor(scope) {
		return (await db.newestCursor(scope)) ?? (await db.scopeOrigin(scope));
	}
};
```

Every returned cursor must be one `readAfter` can find. Return `undefined` when you cannot name a position.

### Cache adapter `keys()`

Add `keys()` to your `CacheAdapter` and the runtime removes expired snapshots of scopes that are never opened again: one
sweep 60 seconds after `configureSync`, at most once a day (it writes a `sync-resource:cache-sweep` entry through the
adapter). It deletes every expired entry `keys()` returns, so use a store dedicated to Sync Resource.

```ts
const adapter: CacheAdapter = {
	get: (key) => idb.get(key),
	set: (key, item) => idb.set(key, item),
	del: (key) => idb.delete(key),
	keys: () => idb.keys() // new, optional
};
```

### Effect server boundary

`sync-resource/server/effect` mirrors `resource` and `manager` with Effect handlers, typed failures, services, and
interruption. The client and HTTP protocol are identical, so you can move one manager at a time.

**Before (Promise).**

```ts
import { eq } from 'drizzle-orm';
import { manager, resource } from 'sync-resource/server';

import { db, notes } from './db.js';

const notesResource = resource(paramsSchema, (method) => ({
	mutate: method.mutate({
		query: querySchema,
		input: noteSchema,
		output: noteSchema,
		async handler({ query, input, ctx }) {
			const [note] = await db.select().from(notes).where(eq(notes.id, query.id)).limit(1);
			if (!note) {
				return ctx.error(ctx.syncError('not_found', 'Note not found.'));
			}
			return ctx.ok({ ...note, ...input });
		}
	})
}));

const notesManager = manager({
	key: 'notes',
	resource: notesResource,
	authorize: (context, params) => canReadWorkspace(context, params),
	scope: (params) => params.workspaceId
});
```

**After (Effect).** Return the output (or `Effect.fail(syncError(...))`) instead of `ctx.ok`/`ctx.error`; `authorize` returns
an Effect; `manager(...)` is itself an Effect:

```ts
import { eq } from 'drizzle-orm';
import { Effect } from 'effect';
import { syncError } from 'sync-resource/server';
import { manager, resource } from 'sync-resource/server/effect';

import { db, notes } from './db.js';

const notesResource = resource(paramsSchema, (method) => ({
	mutate: method.mutate({
		query: querySchema,
		input: noteSchema,
		output: noteSchema,
		handler: ({ query, input }) =>
			Effect.gen(function* () {
				const [note] = yield* Effect.tryPromise({
					try: () => db.select().from(notes).where(eq(notes.id, query.id)).limit(1),
					catch: (cause) => syncError('internal', 'Note read failed.', { cause })
				});
				if (!note) {
					return yield* Effect.fail(syncError('not_found', 'Note not found.'));
				}
				return { ...note, ...input };
			})
	})
}));

const program = Effect.gen(function* () {
	const notesManager = yield* manager({
		key: 'notes',
		resource: notesResource,
		authorize: (context, params) => canReadWorkspace(context, params), // returns an Effect
		scope: (params) => params.workspaceId
	});
	return notesManager;
});
```

Mounting uses `httpSharedSyncStreamRoute`, `httpSharedSyncConnectRoute`, and `managerHttpRoutes` on Effect's
`HttpRouter`; see [Effect Server API](./usage.md#effect-server-api).

### `addPlacement: 'sorted'`

Opt in to sorted placement of the current browser's acknowledged `add` writes, using the same `matchesQuery` and
`compare` as realtime adds. It requires `compare`; store construction throws without it.

```ts
reconcile.defaults({
	compare: (left, right) => right.createdAt.localeCompare(left.createdAt),
	addPlacement: 'sorted'
});
```

### `holdRegistrations` and `onRegistered` on a custom transport

Only for apps that pass their own `transport` to `createRuntime`. Implement the optional
`holdRegistrations(): () => void` and the store calls it while a read is in flight and releases it after, so queued
registrations go out together. Call the optional `onRegistered` subscribe callback when a background retry registers a
subscription, so the store clears that registration error. Without it, nothing changes; `connectUrl` applies only to the built-in transport.
