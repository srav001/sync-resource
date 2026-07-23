# React Adapter

Read [Complete usage and API](./usage.md) first. The React adapter creates the same client store and preserves the same
configuration, actions, `SyncResult` values, cache behavior, optimistic updates, replay, repair, and finality. It only
translates reactive reads into `useSyncExternalStore` hooks.

## Install And Create

```bash
npm install sync-resource react
```

Create the store at a stable application or provider boundary, not during component rendering:

```ts
import { createReactStore } from 'sync-resource/client/react';
import type { NotesManager } from './notesManager.js';

const notes = createReactStore<NotesManager>(config);
```

Pass the same optional reconcile factory shown in the [core usage guide](./usage.md#reconcile-defaults) as the second
argument.

## API Translation

| Core JavaScript        | React                   |
| ---------------------- | ----------------------- |
| `store.snapshot()`     | `store.useSnapshot()`   |
| `store.data()`         | `store.useData()`       |
| `store.items()`        | `store.useItems()`      |
| `store.listMeta()`     | `store.useListMeta()`   |
| `store.pages()`        | `store.usePages()`      |
| `store.pending()`      | `store.usePending()`    |
| `store.error()`        | `store.useError()`      |
| `store.isHydrating()`  | `store.useHydrating()`  |
| `store.isRefreshing()` | `store.useRefreshing()` |
| `store.isPending()`    | `store.useIsPending()`  |
| `store.list(query)`    | `store.useList(query)`  |

Call these hooks only from components or custom hooks. React additionally exposes `store.useDisposed()`.

Actions keep the core method names and `SyncResult` behavior. Only methods defined by the manager type are exposed.

## Ownership

React does not expose a synchronous parent ownership scope to an arbitrary store factory. The boundary that creates the
store must dispose it explicitly; an individual subscribing component must not dispose a shared store. Avoid naïve
effect cleanup because React Strict Mode may run development cleanup/setup cycles without creating a new store.

A product-neutral `WorkspaceSyncProvider` can distribute one stable registry for an
`$accountId/$workspaceId` route:

- Let the route/application lifecycle controller create and dispose the registry.
- Let repeated child mounts call `hydrate()` without duplicating hydration flags or retry policy in React state.
- Clear references when the workspace changes. This does not delete persisted cache.

Keep entity, temporary session, account-wide, and global stores outside that workspace registry.

## Core Access

Use `notes.core` for typed signals and advanced subscriptions. Subscribe in an effect and return the unsubscribe
function:

```ts
useEffect(() => notes.core.on.signal('presence-changed', handlePresence), []);
```

Do not duplicate synchronization logic in React components.
