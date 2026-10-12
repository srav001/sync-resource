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
import { Deferred } from '../fixtures/syncSystem.ts';

interface Note {
	readonly id: string;
	readonly title: string;
}

interface RaceManager extends ManagerTypeShape {
	readonly key: 'read-live-race';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<{ readonly limit: number }, never, { readonly items: readonly Note[] }>;
	};
}

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

afterEach(() => resetSyncConfiguration());

const listResponse = (title: string, cursor: string) =>
	Response.json({
		isOk: true,
		isError: false,
		value: { items: [{ id: 'n1', title }] },
		envelope: { managerKey: 'read-live-race', scope: 'workspace-1', cursor, changes: [] }
	});

const update = (cursor: string, title: string): SyncEnvelope => ({
	managerKey: 'read-live-race',
	scope: 'workspace-1',
	cursor,
	changes: [{ type: 'itemUpdated', id: 'n1', patch: {}, value: { id: 'n1', title } }]
});

describe('reads racing live envelopes', () => {
	it('keeps a live envelope and its cursor when an older read response lands after it', async () => {
		let emit!: (envelope: SyncEnvelope) => void;
		let getCursor!: () => string | undefined;
		const transport: RuntimeTransport = {
			async subscribe(options) {
				emit = options.onEnvelope;
				getCursor = options.getCursor!;
				return ok(() => {});
			}
		};
		const staleRead = new Deferred<Response>();
		let calls = 0;
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => {
				calls += 1;
				return calls === 1 ? listResponse('A', 'c1') : staleRead.promise;
			}
		});
		const store = createStore<RaceManager>({
			key: 'read-live-race',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ limit: 10 })
		});
		await store.hydrate();

		const refreshing = store.refresh();
		emit(update('c2', 'B'));
		staleRead.resolve(listResponse('A', 'c1'));
		await refreshing;

		expect(store.items()[0]?.title).toBe('B');
		expect(getCursor()).toBe('c2');
	});

	it('uses the read cursor when no live envelope arrived during the read', async () => {
		let getCursor!: () => string | undefined;
		const transport: RuntimeTransport = {
			async subscribe(options) {
				getCursor = options.getCursor!;
				return ok(() => {});
			}
		};
		let calls = 0;
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => listResponse('A', `c${++calls}`)
		});
		const store = createStore<RaceManager>({
			key: 'read-live-race',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ limit: 10 })
		});
		await store.hydrate();
		await store.refresh();

		expect(getCursor()).toBe('c2');
	});
	it('lets a read without a cursor replace the cursor an earlier read set', async () => {
		let getCursor!: () => string | undefined;
		const transport: RuntimeTransport = {
			async subscribe(options) {
				getCursor = options.getCursor!;
				return ok(() => {});
			}
		};
		let calls = 0;
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => {
				calls += 1;
				return calls === 1
					? listResponse('A', 'stale')
					: Response.json({ isOk: true, isError: false, value: { items: [{ id: 'n1', title: 'B' }] } });
			}
		});
		const store = createStore<RaceManager>({
			key: 'read-live-race',
			getParams: () => ({ workspaceId: 'workspace-1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => ({ limit: 10 })
		});
		try {
			await store.hydrate();
			expect(getCursor()).toBe('stale');
			await store.refresh();
			expect(getCursor()).toBeUndefined();
		} finally {
			store.dispose();
		}
	});
});
