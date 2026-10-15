import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport,
	type SyncEnvelope
} from '../../src/client/core.ts';
import {
	applyEnvelope,
	applyPageOutput,
	createOptimisticState,
	type OptimisticReconcile
} from '../../src/client/optimistic.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface Note {
	readonly id: string;
	readonly bucket: number;
	readonly rank: number;
	readonly title: string;
}

interface NotesManager extends ManagerTypeShape {
	readonly key: 'family-eviction';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<{ readonly bucket: number }, never, { readonly items: readonly Note[] }>;
		readonly mutate: MethodType<{ readonly id: string }, { readonly $set: Partial<Note> }, Note>;
	};
}

class ManualTransport implements RuntimeTransport {
	private onEnvelope: ((envelope: SyncEnvelope) => void) | undefined;

	async subscribe(options: Parameters<RuntimeTransport['subscribe']>[0]) {
		this.onEnvelope = options.onEnvelope;
		return ok(() => {});
	}

	emit(envelope: SyncEnvelope): void {
		this.onEnvelope?.(envelope);
	}
}

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

const compare = (left: Note, right: Note) => right.rank - left.rank;
const matchesQuery = (item: Note, context: { readonly query?: { readonly bucket: number } }) =>
	item.bucket === context.query?.bucket;

afterEach(() => {
	resetSyncConfiguration();
});

describe('query family eviction', () => {
	it('evicts the least recently used family past eight, never the active one, and reloads on refresh', async () => {
		const model = Array.from({ length: 12 }, (_, bucket) => [note(bucket, 0), note(bucket, 1)]).flat();
		const { store, transport } = createNotesStore(async (input) =>
			syncResponse({ items: model.filter((item) => item.bucket === readBucket(input)) })
		);
		try {
			await store.refresh();
			await store.connect();
			for (let bucket = 1; bucket <= 9; bucket += 1) {
				await store.list({ bucket }).refresh();
			}
			// Ten families were loaded: the two oldest that are not active are gone.
			for (const bucket of [1, 2]) {
				const handle = store.list({ bucket });
				expect(handle.items()).toEqual([]);
				expect(handle.pages()).toEqual([]);
				expect(handle.meta()).toBeUndefined();
			}
			expect(ids(store.items())).toEqual(['n0-0', 'n0-1']);
			for (let bucket = 3; bucket <= 9; bucket += 1) {
				expect(store.list({ bucket }).items()).toHaveLength(2);
			}

			// Reading a family makes it recent: 3 survives the next eviction, 4 does not.
			store.list({ bucket: 3 }).items();
			await store.list({ bucket: 10 }).refresh();
			expect(store.list({ bucket: 4 }).items()).toEqual([]);
			expect(store.list({ bucket: 3 }).items()).toHaveLength(2);

			// A live change reaches the families that remain and ignores the evicted one.
			const added = { ...note(3, 5), rank: 2000 };
			model.push(added);
			transport.emit(envelope('add-1', { type: 'itemAdded', id: added.id, value: added }));
			expect(ids(store.list({ bucket: 3 }).items())).toEqual(['n3-5', 'n3-0', 'n3-1']);
			expect(store.list({ bucket: 4 }).items()).toEqual([]);

			// refresh() loads an evicted family again, equal to what a fresh read returns.
			const reloaded = await store.list({ bucket: 1 }).refresh();
			expect(reloaded.isOk()).toBe(true);
			expect(ids(store.list({ bucket: 1 }).items())).toEqual(['n1-0', 'n1-1']);
			expect(store.list({ bucket: 1 }).pages()).toHaveLength(1);
			expect(ids(store.items())).toEqual(['n0-0', 'n0-1']);
		} finally {
			store.dispose();
		}
	});

	it('keeps only items some family holds once families exist', () => {
		const reconcile: OptimisticReconcile<Note, { readonly bucket: number }> = {
			itemId: (item) => item.id,
			targetId: () => undefined,
			matchesQuery: (item, query) => item.bucket === query.bucket,
			compare
		};
		const state = createOptimisticState<Note, unknown, unknown, unknown>();
		const pending = new Map();
		const query = { bucket: 0 };
		applyEnvelope(state, envelope('live-0', added(note(7, 0))), reconcile, pending, query);
		expect(state.baseItems.size).toBe(1);

		applyPageOutput(state, { items: [note(0, 0), note(0, 1)] }, query, reconcile);
		expect(state.baseItems.size).toBe(2);

		const orphans = Array.from({ length: 50 }, (_, index) => added(note(7, index + 1)));
		applyEnvelope(state, envelope('live-1', ...orphans), reconcile, pending, query);
		applyEnvelope(state, envelope('live-2', added(note(0, 9))), reconcile, pending, query);
		expect([...state.baseItems.keys()].sort()).toEqual(['n0-0', 'n0-1', 'n0-9']);

		for (let bucket = 1; bucket <= 9; bucket += 1) {
			applyPageOutput(state, { items: [note(bucket, 0)] }, { bucket }, reconcile, false);
		}
		expect(state.families.size).toBe(8);
		expect(state.baseItems.size).toBe(3 + 7);
		expect(state.baseItems.has('n1-0')).toBe(false);
	});

	it('skips a patch-only update for an id the store does not hold', async () => {
		const { store, transport } = createNotesStore(async () => syncResponse({ items: [note(0, 0)] }));
		try {
			await store.refresh();
			await store.connect();
			transport.emit(envelope('patch-1', patched('ghost', 'x')));
			expect(ids(store.items())).toEqual(['n0-0']);
			transport.emit(envelope('patch-2', patched('n0-0', 'x')));
			expect(store.items().map((item) => item.title)).toEqual(['x']);
		} finally {
			store.dispose();
		}
	});

	it('renders and settles a pending write over a family that was evicted and reloaded', async () => {
		const mutation = new Deferred<Response>();
		const model = Array.from({ length: 10 }, (_, bucket) => note(bucket, 0));
		let mutationId = '';
		const { store } = createNotesStore(async (input, init) => {
			if (init?.method === 'POST') {
				mutationId = new Headers(init.headers).get('x-mutation-id') ?? '';
				return mutation.promise;
			}
			return syncResponse({ items: model.filter((item) => item.bucket === readBucket(input)) });
		});
		try {
			await store.refresh();
			await store.list({ bucket: 1 }).refresh();
			const write = store.mutate({ query: { id: 'n1-0' }, input: { $set: { title: 'pending' } } });
			await Promise.resolve();
			expect(titles(store.list({ bucket: 1 }).items())).toEqual(['pending']);
			expect(store.pending()).toHaveLength(1);

			for (let bucket = 2; bucket <= 9; bucket += 1) {
				await store.list({ bucket }).refresh();
			}
			expect(store.list({ bucket: 1 }).items()).toEqual([]);
			expect(store.pending()).toHaveLength(1);

			await store.list({ bucket: 1 }).refresh();
			expect(titles(store.list({ bucket: 1 }).items())).toEqual(['pending']);

			const final = { ...note(1, 0), title: 'final' };
			mutation.resolve(
				syncResponse(final, {
					...envelope('ack-1', { type: 'itemUpdated', id: final.id, value: final }),
					sourceMutationId: mutationId,
					sourceClientId: 'client-local'
				})
			);
			expect((await write).isOk()).toBe(true);
			expect(titles(store.list({ bucket: 1 }).items())).toEqual(['final']);
			expect(store.pending()).toEqual([]);
		} finally {
			store.dispose();
		}
	});
});

function createNotesStore(handler: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>) {
	const transport = new ManualTransport();
	configureSync({ cache: { adapter: cache }, transport, fetch: handler });
	const store = createStore<NotesManager>(
		{
			key: 'family-eviction',
			params: { workspaceId: 'workspace-1' },
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ bucket: 0 })
		},
		(reconcile) => reconcile.defaults({ matchesQuery, compare, targetId: (context) => context.query?.id })
	);
	return { store, transport };
}

function note(bucket: number, index: number): Note {
	return { id: `n${bucket}-${index}`, bucket, rank: 1000 - index, title: `t${bucket}-${index}` };
}

function added(value: Note): SyncEnvelope['changes'][number] {
	return { type: 'itemAdded', id: value.id, value };
}

function patched(id: string, title: string): SyncEnvelope['changes'][number] {
	return { type: 'itemUpdated', id, patch: { $set: { title, bucket: 0, rank: 1 } } };
}

function envelope(cursor: string, ...changes: SyncEnvelope['changes'][number][]): SyncEnvelope {
	return { managerKey: 'family-eviction', scope: 'workspace-1', cursor, changes };
}

const ids = (items: readonly Note[]) => items.map((item) => item.id);
const titles = (items: readonly Note[]) => items.map((item) => item.title);

function readBucket(input: RequestInfo | URL): number {
	const url = new URL(input instanceof Request ? input.url : String(input));
	return (JSON.parse(url.searchParams.get('query') ?? '{}') as { bucket: number }).bucket;
}

function syncResponse(value: unknown, responseEnvelope?: SyncEnvelope): Response {
	return Response.json({
		isOk: true,
		isError: false,
		value,
		...(responseEnvelope ? { envelope: responseEnvelope } : {})
	});
}
