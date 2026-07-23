# Vue Adapter

Read [Complete usage and API](./usage.md) first. The Vue adapter creates the same client store and preserves the same
configuration, actions, `SyncResult` values, cache behavior, optimistic updates, replay, repair, and finality. It only
translates reactive reads into Vue refs and attaches disposal to an active Vue effect scope.

## Create

Create the store synchronously inside `setup()` or a composable called by `setup()`:

```ts
import { createVueStore } from 'sync-resource/client/vue';
import type { NotesManager } from './notesManager.js';

const notes = createVueStore<NotesManager>(config);
```

Pass the same optional reconcile factory shown in the [core usage guide](./usage.md#reconcile-defaults) as the second
argument.

## API Translation

| Core JavaScript        | Vue                        |
| ---------------------- | -------------------------- |
| `store.snapshot()`     | `store.snapshot.value`     |
| `store.data()`         | `store.data.value`         |
| `store.items()`        | `store.items.value`        |
| `store.listMeta()`     | `store.listMeta.value`     |
| `store.pages()`        | `store.pages.value`        |
| `store.pending()`      | `store.pending.value`      |
| `store.error()`        | `store.error.value`        |
| `store.isHydrating()`  | `store.isHydrating.value`  |
| `store.isRefreshing()` | `store.isRefreshing.value` |
| `store.isPending()`    | `store.isPending.value`    |
| `store.list(query)`    | `store.list(query)`        |

State uses readonly shallow refs; derived values and list-handle fields use computed refs. Domain objects are not
deep-proxied. Vue additionally exposes `store.disposed.value`.

Actions keep the core method names and `SyncResult` behavior. Only methods defined by the manager type are exposed.

## Ownership

The adapter detects the active Vue effect scope and registers `onScopeDispose(store.dispose)`. Creation after an
asynchronous boundary or outside a scope requires manual `dispose()`.

A product-neutral `WorkspaceSyncProvider` can own one effect scope and registry for an
`$accountId/$workspaceId` route:

- Create stores synchronously in that scope and expose stable values through `provide`/`inject`.
- Let repeated child mounts call `hydrate()` without duplicating hydration flags or retry policy.
- Stop the scope and clear references when the workspace changes. This does not delete persisted cache.

Keep entity, temporary session, account-wide, and global stores outside that workspace registry.

## Core Access

Use `notes.core` for typed signals and advanced subscriptions:

```ts
const unsubscribe = notes.core.on.signal('presence-changed', handlePresence);
onScopeDispose(unsubscribe);
```

Do not duplicate synchronization logic in Vue components.
