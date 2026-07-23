# Solid Adapter

The Solid adapter exposes a Sync Resource client store as Solid accessors. It uses the same framework-neutral core
store as every other adapter, so cache, transport, optimistic updates, replay, repair, and mutation finality behave
identically.

## Install

```bash
npm install sync-resource solid-js
```

Import the adapter from its dedicated entry point:

```ts
import { createSolidStore } from 'sync-resource/client/solid';
```

Importing `sync-resource/client/core` does not load Solid.

## Create A Store

Create the store inside a Solid owner, such as a component, context provider, or `createRoot`:

```tsx
import { For, Show } from 'solid-js';
import { createSolidStore } from 'sync-resource/client/solid';
import type { NotesManager } from './notesManager.js';

export function Notes(props: { workspaceId: string }) {
	const notes = createSolidStore<NotesManager>({
		key: 'notes',
		getParams: () => ({ workspaceId: props.workspaceId }),
		getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
		query: () => ({ status: 'active', limit: 50 })
	});

	notes.hydrate().catch((error: unknown) => {
		console.error(error);
	});

	return (
		<Show when={!notes.isHydrating()} fallback={<p>Loading…</p>}>
			<For each={notes.items()}>{(note) => <p>{note.title}</p>}</For>
		</Show>
	);
}
```

The manager type determines which read and action methods exist. A list manager exposes `items`, `list`, `refresh`,
and `loadMore`; a get manager exposes `data` and `get`; write methods appear only when the manager defines them.

### Custom Reconciliation

Normal collections do not need explicit reconciliation when item and target identity can be inferred. Pass a reconcile
factory as the second argument when the domain needs custom identity, query membership, or ordering:

```ts
const notes = createSolidStore<CustomNotesManager>(
	{
		key: 'notes',
		getParams: () => ({ workspaceId }),
		getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
		query: () => ({ status: statusFilter(), limit: 50 })
	},
	(reconcile) =>
		reconcile.defaults({
			itemId: (note) => note.noteKey,
			targetId: ({ query }) => query.noteKey,
			matchesQuery: (note, { query }) => note.status === query.status,
			compare: (left, right) => right.createdAt.localeCompare(left.createdAt)
		})
);
```

Keep reconciliation in the store factory rather than rebuilding membership or ordering logic in components.

## Reactive State

Solid state is exposed through accessors:

```ts
notes.snapshot();
notes.items();
notes.listMeta();
notes.pages();
notes.pending();
notes.error();
notes.isHydrating();
notes.isRefreshing();
notes.isPending();
```

The adapter subscribes to individual core event slots. Reading `items()` does not make that accessor rerun for an
unrelated error or hydration-state change.

## Hydration And Retry

`hydrate()` restores cached authoritative state, reads current server state, persists it, and connects realtime.
Concurrent calls on one store share the active attempt. After the first successful hydration, later public calls reuse
that success without repeating IO; a failed attempt remains retryable.

It is therefore safe for a route-remountable consumer to call `hydrate()` each time it mounts:

```ts
async function hydrateNotes(): Promise<void> {
	const result = await notes.hydrate();
	if (result.isErr()) {
		setHydrationError(result.error.message);
	}
}

hydrateNotes().catch((error: unknown) => {
	console.error(error);
});
```

Use `isHydrating()` for the full cache/read/connect operation. `isRefreshing()` reports repair-driven authoritative
refreshes. Cached items may already be visible while hydration continues.

## Query Families And Actions

The configured `query()` is the default list family. Use a list handle when the UI displays another family:

```ts
const archived = notes.list({ status: 'archived', limit: 50 });

archived.items();
archived.meta();
archived.pages();
```

`refresh()` loads the first page for its query family. `loadMore()` reuses the family's stored cursor and appends the
next page. UI code does not pass cursors, page indexes, or loaded item ids.

Branch on pagination results just like write results:

```tsx
const [loadingMore, setLoadingMore] = createSignal(false);
const [loadMoreError, setLoadMoreError] = createSignal<string>();

async function loadMoreItems(): Promise<void> {
	if (loadingMore()) {
		return;
	}

	setLoadingMore(true);
	try {
		const result = await notes.loadMore();
		if (result.isErr()) {
			setLoadMoreError(result.error.message);
		}
	} finally {
		setLoadingMore(false);
	}
}

function requestMoreItems(): void {
	loadMoreItems().catch((error: unknown) => {
		console.error(error);
	});
}

<button disabled={loadingMore()} onClick={requestMoreItems}>
	Load more
</button>;
```

Actions keep the core names and typed `SyncResult` return values:

```ts
const result = await notes.mutate({
	query: { id: noteId },
	input: { title: 'Renamed' }
});

if (result.isErr()) {
	console.error(result.error);
}
```

## Ownership And Cleanup

The adapter registers `onCleanup(store.dispose)` with the current Solid owner. Disposing that owner disposes the core
store, closes its subscriptions, and removes adapter listeners. Manual `dispose()` is also available and is safe to
call more than once.

Create a separate store for each independent sync lifetime. Put a store in context when child components should share
one connection and cache view; do not create duplicate stores in every consumer.

### Workspace Route Store Ownership

`WorkspaceSyncProvider` can be the top cache boundary for one `$accountId/$workspaceId` route. Create each
workspace-singleton store on first use under that Solid owner, then reuse the same Context value across child-route
provider remounts.

- Store-specific factories own `createSolidStore`, hydration invocation, derived state, and the final Context value.
- Route-remountable consumers call public `store.hydrate()` on each mount. The store shares one active attempt, reuses
  a completed success for its scope lifetime, and remains retryable after failure.
- The workspace registry owns only Context-value references and their lifetime. It does not duplicate hydration state,
  success latches, retry policy, or `isHydrating()`.
- Leaving or switching workspaces disposes workspace-owned Solid resources and clears their in-memory references.
- Clearing the in-memory registry does not delete authoritative state held by the configured cache adapter.

Use this pattern only for values that are singletons within one workspace route. Keep entity-keyed stores, temporary
session state, account-wide stores, global stores, and ordinary query resources in their correct lifecycle boundaries.

## Signals And Core Access

Live-only signals remain on the framework-neutral core:

```ts
const unsubscribe = notes.core.on.signal('presence-changed', (payload) => {
	console.log(payload);
});

onCleanup(unsubscribe);
```

Use `notes.core` for typed signals or advanced subscriptions. Do not duplicate synchronization logic in Solid
components.

See the [README](../README.md) for runtime configuration, manager definitions, reconciliation, pagination, and
finality.
