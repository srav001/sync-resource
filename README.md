# Sync Resource

Typed, server-authoritative synchronization: cached reads, optimistic writes, realtime updates, replay, repair, and
mutation finality.

> **Coding agents:** read [AGENTS.md](./AGENTS.md) first. It routes you to the correct usage guide and source layer. In
> an installed project it is at `node_modules/sync-resource/AGENTS.md`, beside this README and `docs/`.

- **Effect-native server.** `sync-resource/server/effect` takes Effect v4 handlers, typed failures, services, and
  interruption, and mounts on Effect's `HttpRouter`.
- **Plain Promises, fully supported.** `sync-resource/server` and the client use ordinary `async` functions and strict
  `SyncResult` values. Effect is optional and never loaded unless you import it.
- **Framework-neutral client** with Solid, Vue, and React adapters.
- **Database stays authoritative.** Browser cache makes startup instant, optimistic state keeps interactions
  immediate, cursor replay plus repair preserve correctness when realtime delivery is interrupted.

Upgrading from v1? See [docs/migrating-from-v1.md](./docs/migrating-from-v1.md).

## Install

```bash
npm install sync-resource            # Promise API, client core
npm install sync-resource effect     # plus the Effect server API (effect >=4.0.3 <5)
npm install sync-resource react      # or vue, or solid-js, for a framework adapter
```

Works with npm, pnpm, Bun, or any compatible package manager. Node 22.18 or newer.

## Server: Promise or Effect

The same resource and manager, written both ways. Both expose the same HTTP handlers and the same type for the client.

**Promise**

```ts
import { eq } from 'drizzle-orm';
import { httpSharedSyncConnect, httpSharedSyncStream, manager, resource } from 'sync-resource/server';

import { db, notes } from './db.js';

const notesResource = resource(paramsSchema, (method) => ({
	list: method.list({
		output: pageSchema,
		async handler({ params, ctx }) {
			const items = await db.select().from(notes).where(eq(notes.workspaceId, params.workspaceId));
			return ctx.ok({ items });
		}
	})
}));

export const notesManager = manager({
	key: 'notes',
	resource: notesResource,
	authorize: (context, params) => canReadWorkspace(context, params),
	scope: (params) => params.workspaceId
});
export type NotesManager = typeof notesManager.type;

// Mount once per app: the shared stream and the batch connect route.
httpSharedSyncStream(request);
httpSharedSyncConnect(request, handleApplicationRequest);
// Mount per manager: one handler per method the resource defines, plus .connect.
notesManager.http.list({ request, params });
notesManager.http.connect({ request, params });
```

**Effect**

```ts
import { eq } from 'drizzle-orm';
import { Effect, Layer } from 'effect';
import { HttpRouter } from 'effect/http';
import { syncError } from 'sync-resource/server';
import {
	httpSharedSyncConnectRoute,
	httpSharedSyncStreamRoute,
	manager,
	managerHttpRoutes,
	resource
} from 'sync-resource/server/effect';

import { db, notes } from './db.js';

const notesResource = resource(paramsSchema, (method) => ({
	list: method.list({
		output: pageSchema,
		handler: ({ params }) =>
			Effect.tryPromise({
				try: () => db.select().from(notes).where(eq(notes.workspaceId, params.workspaceId)),
				catch: (cause) => syncError('internal', 'Notes read failed.', { cause })
			}).pipe(Effect.map((items) => ({ items })))
	})
}));

const routes = Effect.gen(function* () {
	const notesManager = yield* manager({
		key: 'notes',
		resource: notesResource,
		authorize: (context, params) => canReadWorkspace(context, params), // returns an Effect
		scope: (params) => params.workspaceId
	});

	return Layer.mergeAll(
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
});
```

## Client

The client is Promise-based and returns `SyncResult` values instead of throwing, so it works in any runtime and wraps
into Effect in one line.

**Promise**

```ts
import { configureSync, createStore } from 'sync-resource/client/core';

import type { NotesManager } from './notesManager.js';

configureSync({ cache: { adapter: cacheAdapter } });

const notes = createStore<NotesManager>({
	key: 'notes',
	getParams: () => ({ workspaceId: 'workspace-123' }),
	getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
	query: () => undefined
});

notes.on.items(renderNotes);

const result = await notes.hydrate();
if (result.isErr()) {
	console.error(result.error);
}
```

**Effect**

```ts
import { Effect } from 'effect';

const hydrate = Effect.promise(() => notes.hydrate()).pipe(
	Effect.flatMap((result) => (result.isOk() ? Effect.void : Effect.fail(result.error)))
);
```

Store construction is network-free. `hydrate()` restores cached state, reads from the server, and resolves once the read
is applied; realtime registration continues in the background. The application owns its concrete cache adapter.

## Documentation

- [Complete usage and API](./docs/usage.md): resources, managers, the Effect server API, client stores, protocol,
  replay, repair, pagination, reconciliation, and finality
- [Solid](./docs/solid.md), [Vue](./docs/vue.md), [React](./docs/react.md): adapter mapping and store ownership
- [Migrating from v1](./docs/migrating-from-v1.md)

## Public Entry Points

| Import                        | Purpose                                                |
| ----------------------------- | ------------------------------------------------------ |
| `sync-resource/server`        | Resources, managers, streams, and persistence          |
| `sync-resource/server/effect` | Effect v4 resources, managers, and `HttpRouter` routes |
| `sync-resource/client/core`   | Framework-neutral runtime and stores                   |
| `sync-resource/client/solid`  | Solid accessors and owner cleanup                      |
| `sync-resource/client/vue`    | Vue refs and effect-scope cleanup                      |
| `sync-resource/client/react`  | React hooks and explicit store lifecycle               |
| `sync-resource/shared`        | Protocol, schemas, errors, and shared types            |

## Guarantees

- Synchronized writes flow through managers so persistence, outbox replay, realtime fanout, and finality stay aligned.
- HTTP acknowledgement is not mutation finality; authoritative envelope or reset coverage settles a write.
- Realtime is not the sole correctness mechanism; cursor replay and repair preserve convergence.
- Framework adapters do not duplicate synchronization logic from the core store.
- IndexedDB and other concrete cache implementations remain application-owned adapters.

## Development

Requirements: Node 22.18 or newer and pnpm 11.1.2.

```bash
pnpm install
pnpm run validate
```

The required validation runs formatting checks, Oxlint, type-aware checks, Vitest, and the package build. Run
`vp test --coverage` when manually auditing coverage.

---

Designed by humans ❤️. Built and documented using agents 🤖.
