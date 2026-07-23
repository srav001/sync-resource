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

## Query Families And Actions

The configured `query()` is the default list family. Use a list handle when the UI displays another family:

```ts
const archived = notes.list({ status: 'archived', limit: 50 });

archived.items();
archived.meta();
archived.pages();
await archived.refresh();
await archived.loadMore();
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
