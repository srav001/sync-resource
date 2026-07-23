# React Adapter

The React adapter exposes a Sync Resource client store through `useSyncExternalStore` hooks. It uses the same
framework-neutral core store as every other adapter, so cache, transport, optimistic updates, replay, repair, and
mutation finality behave identically.

## Install

```bash
npm install sync-resource react
```

Import the adapter from its dedicated entry point:

```ts
import { createReactStore } from 'sync-resource/client/react';
```

Importing `sync-resource/client/core` does not load React.

## Create A Store

Create the store at a stable application or provider boundary, not during component rendering:

```tsx
import { createReactStore } from 'sync-resource/client/react';
import type { NotesManager } from './notesManager.js';

export const notes = createReactStore<NotesManager>({
	key: 'notes',
	getParams: () => ({ workspaceId: currentWorkspaceId() }),
	getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
	query: () => ({ status: currentStatus(), limit: 50 })
});

export function Notes() {
	const items = notes.useItems();
	const hydrating = notes.useHydrating();

	if (hydrating) {
		return <p>Loading…</p>;
	}

	return items.map((note) => <p key={note.id}>{note.title}</p>);
}
```

Hydrate from the code that starts the owning application boundary:

```ts
const result = await notes.hydrate();
if (result.isErr()) {
	console.error(result.error);
}
```

The manager type determines which hooks and action methods exist. A list manager exposes list hooks and pagination
actions; a get manager exposes `useData` and `get`; write methods appear only when the manager defines them.

## Reactive State

Call adapter hooks only from React components or custom hooks:

```ts
notes.useSnapshot();
notes.useItems();
notes.useListMeta();
notes.usePages();
notes.usePending();
notes.useError();
notes.useHydrating();
notes.useRefreshing();
notes.useIsPending();
notes.useDisposed();
```

Each hook subscribes to the corresponding core event slot through a stable external-store boundary. Unrelated state
changes do not notify a field hook.

## Query Families And Actions

Use `useList(query)` when a component displays a non-default query family:

```tsx
function ArchivedNotes() {
	const archived = notes.useList({ status: 'archived', limit: 50 });

	return (
		<>
			{archived.items.map((note) => (
				<p key={note.id}>{note.title}</p>
			))}
		</>
	);
}
```

Call `await archived.refresh()` or `await archived.loadMore()` from an event handler and branch on the returned
`SyncResult`.

Actions are ordinary typed methods and return `SyncResult`:

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

React does not expose a synchronous parent ownership scope to an arbitrary store factory. The adapter therefore does
not guess ownership or dispose the store when an individual subscribing component unmounts. That store may still be
shared by other components.

The boundary that created the store must call `store.dispose()` when the whole boundary ends. Manual disposal is safe
to call more than once. Avoid creating the store directly in a component render or attaching naïve effect cleanup:
React Strict Mode may run development cleanup and setup cycles without creating a new store instance.

For a single application lifetime, a module-owned store can be disposed by the application bootstrap teardown. For
shorter lifetimes, own the store in an application-specific provider whose construction and Strict Mode lifecycle are
explicitly controlled.

## Server Rendering

The hooks provide stable server snapshots through `useSyncExternalStore`, so initial rendering can read the current
in-memory state. Do not start hydration, transport connections, or other browser effects during server rendering.
Start those operations from the client-owned application boundary.

## Signals And Core Access

Live-only signals remain on the framework-neutral core. Subscribe from an effect and return the unsubscribe function:

```tsx
import { useEffect } from 'react';

function PresenceListener() {
	useEffect(
		() =>
			notes.core.on.signal('presence-changed', (payload) => {
				console.log(payload);
			}),
		[]
	);

	return null;
}
```

Use `notes.core` for typed signals or advanced subscriptions. Do not duplicate synchronization logic in React
components.

See the [README](../README.md) for runtime configuration, manager definitions, reconciliation, pagination, and
finality.
