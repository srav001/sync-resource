# Sync Resource

Sync Resource is a typed, server-authoritative synchronization library for cached reads, optimistic writes, realtime
updates, replay, repair, and mutation finality.

The database remains authoritative. Browser cache accelerates startup, optimistic state keeps interactions immediate,
and cursor replay plus repair preserve correctness when realtime delivery is interrupted.

## Install

```bash
npm install sync-resource
```

The core client is framework-neutral. Install only the optional framework peer you use:

```bash
npm install sync-resource solid-js
npm install sync-resource vue
npm install sync-resource react
```

## Quick Start

```ts
import { configureSync, createStore } from 'sync-resource/client/core';
import { cacheAdapter } from './cache.js';
import type { NotesManager } from './notesManager.js';

configureSync({
	streamUrl: '/api/live-resource/stream',
	cache: { adapter: cacheAdapter }
});

const notes = createStore<NotesManager>({
	key: 'notes',
	getParams: () => ({ workspaceId: 'workspace-123' }),
	getUrl: ({ workspaceId }) => `/api/workspaces/${workspaceId}/notes/sync`,
	query: () => ({ status: 'active', limit: 50 })
});

const unsubscribe = notes.on.items((items) => {
	renderNotes(items);
});

const result = await notes.hydrate();
if (result.isErr()) {
	console.error(result.error);
}
```

Store construction is network-free. `hydrate()` restores cached authoritative state, refreshes from the server, and
connects realtime delivery. The application owns its concrete cache adapter.

## Documentation

- [Complete usage and API](./docs/usage.md) — resources, managers, client stores, pure JavaScript examples, protocol,
  replay, repair, pagination, reconciliation, and finality
- [Solid](./docs/solid.md) — accessor mapping and automatic owner cleanup
- [Vue](./docs/vue.md) — shallow-ref mapping and effect-scope cleanup
- [React](./docs/react.md) — hook mapping and explicit store ownership

The framework adapters create the same core store. They change only how state becomes reactive and how the store
lifetime is owned; cache, transport, writes, replay, repair, and finality work the same way.

## Public Entry Points

| Import                       | Purpose                                       |
| ---------------------------- | --------------------------------------------- |
| `sync-resource/server`       | Resources, managers, streams, and persistence |
| `sync-resource/client/core`  | Framework-neutral runtime and stores          |
| `sync-resource/client/solid` | Solid accessors and owner cleanup             |
| `sync-resource/client/vue`   | Vue refs and effect-scope cleanup             |
| `sync-resource/client/react` | React hooks and explicit store lifecycle      |
| `sync-resource/shared`       | Protocol, schemas, errors, and shared types   |

## Guarantees

- Synchronized writes flow through managers so persistence, outbox replay, realtime fanout, and finality stay aligned.
- HTTP acknowledgement is not mutation finality; authoritative envelope or reset coverage settles a write.
- Realtime is not the sole correctness mechanism; cursor replay and repair preserve convergence.
- Framework adapters do not duplicate synchronization logic from the core store.
- IndexedDB and other concrete cache implementations remain application-owned adapters.

## Status

The package builds ESM JavaScript, TypeScript declarations, and source maps for every public entry point. Framework
adapters have focused lifecycle and typing tests. Comprehensive protocol, replay, finality, and repair characterization
remains the next reliability milestone.

## Development

Requirements: Node 24 and pnpm 11.1.2.

```bash
pnpm install
pnpm run validate
```

Vite+ runs Oxfmt, Oxlint, type-aware checks, Vitest, and the package build.
