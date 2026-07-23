import { createElement } from 'react';
import { renderToString } from 'react-dom/server';
import { createComputed, createRoot } from 'solid-js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';
import { effectScope, watchEffect } from 'vue';

import {
	resetSyncConfiguration,
	type ReconcileBuilder,
	type ReconcileConfig,
	type StoreConfig,
	type SyncError
} from '../src/client/core.ts';
import { createReactStore } from '../src/client/react.ts';
import { createSolidStore } from '../src/client/solid.ts';
import { createVueStore } from '../src/client/vue.ts';
import { resetSharedStreamForTests } from '../src/server/index.ts';
import { createSyncTestSystem, type Note } from './fixtures/syncSystem.ts';

vi.mock('solid-js', async () => import(['solid-js/dist', 'solid.js'].join('/')));

type NotesManager = ReturnType<typeof createSyncTestSystem>['manager']['type'];

interface ResultState {
	readonly status: 'ok' | 'error';
}

interface ReactiveAdapterHarness {
	items(): readonly Note[];
	listItems(): readonly Note[];
	pending(): boolean;
	error(): SyncError | null;
	hydrating(): boolean;
	disposed(): boolean;
	observed(): readonly string[];
	hydrate(): Promise<ResultState>;
	add(note: Note): Promise<ResultState>;
	mutate(id: string, title: string): Promise<ResultState>;
	disposeOwner(): void;
}

const storeConfig: StoreConfig<NotesManager> = {
	key: 'notes',
	getParams: () => ({ workspaceId: 'workspace-1' }),
	getUrl: ({ workspaceId }) => `http://sync.test/workspaces/${workspaceId}/notes`,
	query: () => ({ limit: 20 })
};

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('framework adapter behavior', () => {
	it('keeps Solid accessors and list handles reactive through the full store lifecycle', async () => {
		const system = createSyncTestSystem();
		const harness = createSolidHarness();

		await exerciseReactiveAdapter(system, harness);
	});

	it('keeps Vue refs and list handles reactive through the full store lifecycle', async () => {
		const system = createSyncTestSystem();
		const harness = createVueHarness();

		await exerciseReactiveAdapter(system, harness);
	});

	it('passes React actions and lifecycle through to the real core and characterizes SSR snapshots', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = createReactStore<NotesManager>(storeConfig, createReconcile);

		function NotesView() {
			const items = store.useItems();
			const pending = store.useIsPending();
			const error = store.useError();
			const disposed = store.useDisposed();
			const list = store.useList({ limit: 20 });
			return createElement(
				'output',
				null,
				`${itemText(items)}|${itemText(list.items)}|${String(pending)}|${error?.message ?? 'none'}|${String(disposed)}`
			);
		}

		try {
			expect(renderToString(createElement(NotesView))).toBe('<output>||false|none|false</output>');
			expect((await store.hydrate()).status).toBe('ok');
			expect(store.core.items()).toEqual([{ id: 'note-1', title: 'Initial' }]);

			const commit = system.database.pauseNextCommit();
			const add = store.add({ input: { id: 'note-2', title: 'Optimistic' } });
			await Promise.resolve();
			expect(store.core.isPending()).toBe(true);
			expect(store.core.items()).toContainEqual({ id: 'note-2', title: 'Optimistic' });
			commit.resolve(undefined);
			expect((await add).status).toBe('ok');
			await system.transport.waitForEnvelopeCount(1);
			expect(store.core.isPending()).toBe(false);

			const rendered = renderToString(createElement(NotesView));
			// DIVERGENCE: lazy React sources retain their construction snapshot during SSR, while useList reads current core state.
			expect(rendered).toBe('<output>|note-1:Initial,note-2:Optimistic|false|none|false</output>');

			store.dispose();
			expect(store.core.snapshot().disposed).toBe(true);
			expect((await store.hydrate()).status).toBe('error');
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});
});

async function exerciseReactiveAdapter(
	system: ReturnType<typeof createSyncTestSystem>,
	harness: ReactiveAdapterHarness
): Promise<void> {
	system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);

	try {
		expect(harness.items()).toEqual([]);
		expect(harness.listItems()).toEqual([]);
		const hydration = harness.hydrate();
		expect(harness.hydrating()).toBe(true);
		expect((await hydration).status).toBe('ok');
		expectView(harness, 'note-1:Initial', false, 'none');

		const commit = system.database.pauseNextCommit();
		const add = harness.add({ id: 'note-2', title: 'Optimistic' });
		await Promise.resolve();
		expectView(harness, 'note-1:Initial,note-2:Optimistic', true, 'none');
		commit.resolve(undefined);
		expect((await add).status).toBe('ok');
		await system.transport.waitForEnvelopeCount(1);
		expectView(harness, 'note-1:Initial,note-2:Optimistic', false, 'none');
		expect(system.database.read('workspace-1', 'note-2')).toEqual({
			id: 'note-2',
			title: 'Optimistic'
		});

		const remote = await system.manager
			.bind({ workspaceId: 'workspace-1' })
			.add({ input: { id: 'note-3', title: 'Realtime' } }, { mutationId: 'remote-mutation-1' });
		expect(remote.status).toBe('ok');
		await system.transport.waitForEnvelopeCount(2);
		expectView(harness, 'note-1:Initial,note-2:Optimistic,note-3:Realtime', false, 'none');

		system.database.failNextCommit('database unavailable');
		expect((await harness.mutate('note-1', 'Temporary')).status).toBe('error');
		expectView(harness, 'note-1:Initial,note-2:Optimistic,note-3:Realtime', false, 'database unavailable');

		harness.disposeOwner();
		expect(harness.disposed()).toBe(true);
		expect((await harness.hydrate()).status).toBe('error');
	} finally {
		harness.disposeOwner();
		await system.transport.close();
	}
}

function createSolidHarness(): ReactiveAdapterHarness {
	let disposeOwner = () => {};
	let store!: ReturnType<typeof createSolidStore<NotesManager>>;
	let list!: ReturnType<typeof store.list>;
	const observations: string[] = [];

	createRoot((dispose) => {
		disposeOwner = dispose;
		store = createSolidStore<NotesManager>(storeConfig, createReconcile);
		list = store.list({ limit: 20 });
		createComputed(() => {
			observations.push(
				viewText(store.items(), list.items(), store.isPending(), store.error(), store.snapshot().disposed)
			);
		});
	});

	return {
		items: () => store.items(),
		listItems: () => list.items(),
		pending: () => store.isPending(),
		error: () => store.error(),
		hydrating: () => store.isHydrating(),
		disposed: () => store.snapshot().disposed,
		observed: () => observations,
		hydrate: () => store.hydrate(),
		add: (note) => store.add({ input: note }),
		mutate: (id, title) => store.mutate({ query: { id }, input: { title } }),
		disposeOwner
	};
}

function createVueHarness(): ReactiveAdapterHarness {
	const scope = effectScope();
	const observations: string[] = [];
	const store = scope.run(() => {
		const nextStore = createVueStore<NotesManager>(storeConfig, createReconcile);
		const list = nextStore.list({ limit: 20 });
		watchEffect(
			() => {
				observations.push(
					viewText(
						nextStore.items.value,
						list.items.value,
						nextStore.isPending.value,
						nextStore.error.value,
						nextStore.disposed.value
					)
				);
			},
			{ flush: 'sync' }
		);
		return { nextStore, list };
	});
	if (!store) {
		throw new Error('Vue effect scope did not create the test store.');
	}

	return {
		items: () => store.nextStore.items.value,
		listItems: () => store.list.items.value,
		pending: () => store.nextStore.isPending.value,
		error: () => store.nextStore.error.value,
		hydrating: () => store.nextStore.isHydrating.value,
		disposed: () => store.nextStore.disposed.value,
		observed: () => observations,
		hydrate: () => store.nextStore.hydrate(),
		add: (note) => store.nextStore.add({ input: note }),
		mutate: (id, title) => store.nextStore.mutate({ query: { id }, input: { title } }),
		disposeOwner: () => scope.stop()
	};
}

function createReconcile(builder: ReconcileBuilder<NotesManager>): ReconcileConfig {
	return builder.defaults({
		matchesQuery: () => true,
		compare: (left, right) => left.id.localeCompare(right.id)
	});
}

function expectView(harness: ReactiveAdapterHarness, items: string, pending: boolean, errorMessage: string): void {
	expect(itemText(harness.items())).toBe(items);
	expect(itemText(harness.listItems())).toBe(items);
	expect(harness.pending()).toBe(pending);
	expect(harness.error()?.message ?? 'none').toBe(errorMessage);
	expect(harness.observed().at(-1)).toBe(
		viewText(harness.items(), harness.listItems(), pending, harness.error(), false)
	);
}

function viewText(
	items: readonly Note[],
	listItems: readonly Note[],
	pending: boolean,
	error: SyncError | null,
	disposed: boolean
): string {
	return `${itemText(items)}|${itemText(listItems)}|${String(pending)}|${error?.message ?? 'none'}|${String(disposed)}`;
}

function itemText(items: readonly Note[]): string {
	return items.map((note) => `${note.id}:${note.title}`).join(',');
}
