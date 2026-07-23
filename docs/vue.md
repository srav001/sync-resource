# Vue Adapter

The Vue adapter exposes a Sync Resource client store as shallow refs and computed refs. It uses the same
framework-neutral core store as every other adapter, so cache, transport, optimistic updates, replay, repair, and
mutation finality behave identically.

## Install

```bash
npm install sync-resource vue
```

Import the adapter from its dedicated entry point:

```ts
import { createVueStore } from 'sync-resource/client/vue';
```

Importing `sync-resource/client/core` does not load Vue.

## Create A Store

Create the store synchronously inside `setup()` or a composable called by `setup()`:

```vue
<script setup lang="ts">
import { onMounted } from 'vue';
import { createVueStore } from 'sync-resource/client/vue';
import type { NotesManager } from './notesManager.js';

const props = defineProps<{ workspaceId: string }>();

const notes = createVueStore<NotesManager>({
	key: 'notes',
	getParams: () => ({ workspaceId: props.workspaceId }),
	getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
	query: () => ({ status: 'active', limit: 50 })
});

onMounted(async () => {
	const result = await notes.hydrate();
	if (result.isErr()) {
		console.error(result.error);
	}
});
</script>

<template>
	<p v-if="notes.isHydrating.value">Loading…</p>
	<p v-for="note in notes.items.value" v-else :key="note.id">
		{{ note.title }}
	</p>
</template>
```

The manager type determines which read and action methods exist. A list manager exposes `items`, `list`, `refresh`,
and `loadMore`; a get manager exposes `data` and `get`; write methods appear only when the manager defines them.

## Reactive State

State fields are readonly shallow refs, while derived fields and snapshots are computed refs:

```ts
notes.snapshot.value;
notes.items.value;
notes.listMeta.value;
notes.pages.value;
notes.pending.value;
notes.error.value;
notes.isHydrating.value;
notes.isRefreshing.value;
notes.isPending.value;
notes.disposed.value;
```

Domain objects and arrays are not deep-proxied. Sync Resource replaces affected top-level values when state changes,
preserving the core store's allocation and identity behavior.

## Query Families And Actions

The configured `query()` is the default list family. Use a list handle when the UI displays another family:

```ts
const archived = notes.list({ status: 'archived', limit: 50 });

archived.items.value;
archived.meta.value;
archived.pages.value;
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

Vue runs `setup()` and composables within the component's active effect scope. The adapter detects that parent scope
and registers `onScopeDispose(store.dispose)`, so unmounting the owner automatically disposes the core store and its
subscriptions.

Scope detection is synchronous. If the store is created after an asynchronous boundary or outside an active effect
scope, its owner must call `store.dispose()` explicitly. Manual disposal is safe to call more than once.

Put a store in `provide`/`inject` when descendants should share one sync lifetime. Do not create duplicate stores in
every consumer.

## Signals And Core Access

Live-only signals remain on the framework-neutral core:

```ts
import { onScopeDispose } from 'vue';

const unsubscribe = notes.core.on.signal('presence-changed', (payload) => {
	console.log(payload);
});

onScopeDispose(unsubscribe);
```

Use `notes.core` for typed signals or advanced subscriptions. Do not duplicate synchronization logic in Vue
components.

See the [README](../README.md) for runtime configuration, manager definitions, reconciliation, pagination, and
finality.
