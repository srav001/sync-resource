# Solid Adapter

Read [Complete usage and API](./usage.md) first. The Solid adapter creates the same client store and preserves the same
configuration, actions, `SyncResult` values, cache behavior, optimistic updates, replay, repair, and finality. It only
translates reactive reads into Solid accessors and attaches disposal to the Solid owner.

## Install And Create

```bash
npm install sync-resource solid-js
```

```ts
import { createSolidStore } from 'sync-resource/client/solid';
import type { NotesManager } from './notesManager.js';

const notes = createSolidStore<NotesManager>(config);
```

Pass the same optional reconcile factory shown in the [core usage guide](./usage.md#reconcile-defaults) as the second
argument.

## API Translation

| Core JavaScript        | Solid                  |
| ---------------------- | ---------------------- |
| `store.snapshot()`     | `store.snapshot()`     |
| `store.data()`         | `store.data()`         |
| `store.items()`        | `store.items()`        |
| `store.listMeta()`     | `store.listMeta()`     |
| `store.pages()`        | `store.pages()`        |
| `store.pending()`      | `store.pending()`      |
| `store.error()`        | `store.error()`        |
| `store.isHydrating()`  | `store.isHydrating()`  |
| `store.isRefreshing()` | `store.isRefreshing()` |
| `store.isPending()`    | `store.isPending()`    |
| `store.list(query)`    | `store.list(query)`    |

These reads are Solid accessors. List-handle `items()`, `meta()`, and `pages()` are accessors too.

Actions remain ordinary methods:

```ts
await notes.hydrate();
await notes.refresh();
await notes.loadMore();
await notes.add(args);
await notes.mutate(args);
await notes.delete(args);
```

Only methods defined by the manager type are exposed.

## Ownership

Create the store inside a component, context provider, or `createRoot`. The adapter registers Solid cleanup and
automatically disposes the core store with that owner. Manual `dispose()` remains idempotent.

A product-neutral `WorkspaceSyncProvider` can own one registry for an `$accountId/$workspaceId` route:

- Create each workspace-singleton store on first use under the provider's Solid owner.
- Reuse the Context value across child-route remounts; repeated `hydrate()` calls share or reuse the store's attempt.
- Dispose and clear workspace-owned references when the route changes. This does not delete persisted cache.

Keep entity-keyed, temporary session, account-wide, and global stores in their own lifecycle boundaries.

## Core Access

Use `notes.core` for typed signals and advanced subscriptions:

```ts
const unsubscribe = notes.core.on.signal('presence-changed', handlePresence);
onCleanup(unsubscribe);
```

Do not duplicate synchronization logic in Solid components.
