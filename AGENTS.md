# Repository Instructions

Read `README.md` before planning, reviewing, or changing this repository. It is the authoritative documentation for
the public model, protocol, APIs, lifecycle, examples, and runtime behavior.

## What This Repository Is

Live Resource is a TypeScript synchronization library. It provides:

- typed backend resources and managers
- HTTP and shared SSE transport primitives
- outbox replay, mutation idempotency, and finality
- a framework-neutral cached and optimistic client store

The database is authoritative. Client cache and optimistic state improve responsiveness; realtime delivery, replay,
and repair provide convergence.

## Start Here

1. Read `README.md` for the complete public contract.
2. Identify the owning layer using the repository map below.
3. Read the public entrypoint and implementation files for that layer.
4. Preserve existing public exports and behavior unless the task explicitly changes them.
5. Run `pnpm run validate` before finishing.

## Repository Map

### Shared protocol

- `src/shared/protocol.ts` — envelopes, changes, signals, errors, guards, and wire shapes
- `src/shared/result.ts` — dependency-free strict result implementation
- `src/shared/schema.ts` — validator and Standard Schema support
- `src/shared/sse.ts` — SSE encoding, chunking, and decoding contracts
- `src/shared/stableJson.ts` — stable serialization used by identity and idempotency
- `src/shared/index.ts` — intentional public shared entrypoint

### Server

- `src/server/resource.ts` — typed resource definitions, validation, direct execution, batching, and commits
- `src/server/manager.ts` — authorization, scope, HTTP methods, idempotency, persistence, replay, and finality
- `src/server/streamMultiplexer.ts` — one physical SSE stream with logical manager/scope subscriptions
- `src/server/realtime.ts` — optional cross-process realtime bus
- `src/server/changes.ts` — change constructors
- `src/server/index.ts` — intentional public server entrypoint

### Client

- `src/client/runtime.ts` — global configuration, transport ownership, fetch, reconnect, and cache configuration
- `src/client/store.ts` — hydration, reads, writes, pending commands, events, and lifecycle
- `src/client/optimistic.ts` — entity/page state, optimistic patches, rebase, and query-family reconciliation
- `src/client/repair.ts` — repair planning and scheduling
- `src/client/core.ts` — intentional framework-neutral public entrypoint

## Public Package Boundaries

The package name is `live-resource`. Preserve these subpaths unless an explicit public-API migration is
approved:

- `live-resource/server`
- `live-resource/client/core`
- `live-resource/shared`

The core client must remain framework-neutral. Database drivers, web frameworks, application stores, domain models,
and concrete IndexedDB implementations do not belong in this package.

## Architecture Rules

- Resources own schemas, validation, domain/database IO, and output shaping.
- Managers own authorization, scope, transport exposure, realtime fanout, outbox replay, idempotency, and finality.
- Client stores own cache, hydration, optimistic state, rollback/rebase, realtime application, and repair.
- HTTP success is not mutation finality; authoritative envelope or reset coverage is finality.
- Realtime delivery is not the sole correctness mechanism; cursor replay and repair must remain valid.
- Browser cache adapters are application-owned. The package stores authoritative snapshots through `CacheAdapter` and
  does not implement IndexedDB directly.
- Use `list` for independently rendered or updated records. Use `get` only when the complete scoped value is one unit.
- Do not add public string dispatch APIs, separate batch method names, or framework logic to the core.

## Coding Rules

- Use strict TypeScript, explicit control flow, descriptive names, and direct contracts.
- Prefer interfaces for object shapes and `import type` for type-only imports.
- Avoid `any`, unsafe assertions, unnecessary normalization, duplicate validation, and defensive abstractions without a
  demonstrated input boundary or bug.
- Use `SyncResult` for non-trivial operations. Narrow with `isOk()`, `isErr()`, or `status` before reading values.
- Do not use `void` to silence promises. Await required work or attach an explicit terminal rejection handler.
- Avoid barrel files except the three intentional package entrypoints.
- Keep code formatted with tabs, width 4, single quotes, semicolons, no trailing commas, and a 120-column print width.
- Keep persistent keys, channels, examples, and documentation product-neutral.

## Bug Fixes

Before fixing a bug, identify why the architecture allowed the entire bug class. Prefer removing that structural
condition over adding a symptom guard. Use a local guard only when the structural fix belongs in a separate explicit
change or is proven infeasible, and document the deferred root cause.

## Commands

```bash
pnpm install
pnpm run typecheck
pnpm run lint
pnpm run format
pnpm run build
pnpm run validate
```

Vite+ runs Oxfmt, Oxlint, and type-aware checks. `pnpm run validate` is the required final static validation command.
It also builds the publishable ESM and declaration output into `dist`. Never edit `dist` by hand.

## Publishing

The npm package is public and unscoped as `live-resource`. Before publishing:

1. Run `pnpm install --frozen-lockfile`.
2. Run `pnpm run validate`.
3. Run `npm pack --dry-run` and inspect every included path.
4. Verify the intended version and npm account/scope access.
5. Run `npm publish` only after the package contents and registry state are confirmed.

Published files are limited by the `files` field in `package.json`. Public exports must point to compiled JavaScript
and matching declarations under `dist`.

## Tests And Behavioral Changes

The repository does not yet have a maintained characterization suite. Do not infer behavioral safety from typechecking
alone. When changing protocol, resource, manager, store, replay, repair, cache, finality, or adapter behavior, add or
update black-box tests that exercise public exports.

Priority characterization path:

```txt
hydrate -> optimistic write -> authoritative finality -> disconnect -> cursor replay -> reset
```

## Runtime Assumptions

- TypeScript target: ES2022
- Development runtime: Node 24
- Package manager: pnpm 11.1.2
- Server runtime: global Web Request/Response, streams, encoders, abort APIs, timers, and Web Crypto
- Browser runtime: fetch streams, Web Crypto, local storage, and optional Web Locks/BroadcastChannel support

Renaming browser storage, lock, or broadcast-channel keys breaks coordination with already-open tabs. Treat these keys
as compatibility-sensitive protocol state.

## Documentation

`README.md` is the only public behavior and usage reference. Update it whenever public behavior, APIs, examples,
runtime assumptions, or repository navigation changes. Keep `AGENTS.md` focused on contributor workflow and rules;
reference the README instead of creating another standalone architecture document.
