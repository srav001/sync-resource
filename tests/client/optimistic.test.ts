import { afterEach, beforeEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport
} from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/index.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

interface BareAckManager extends ManagerTypeShape {
	readonly key: 'bare-ack';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			never,
			never,
			{ readonly items: readonly { readonly id: string; readonly title: string }[] }
		>;
		readonly add: MethodType<
			never,
			{ readonly id: string; readonly title: string },
			{ readonly id: string; readonly title: string }
		>;
	};
}

const bareAckCache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};
const bareAckTransport: RuntimeTransport = {
	async subscribe() {
		return ok(() => {});
	}
};

beforeEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('optimistic writes', () => {
	it('shows add, mutate, and delete optimistically before the DB commit', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		try {
			await store.hydrate();
			const addGate = system.database.pauseNextCommit();
			const add = store.add({ input: { id: 'note-2', title: 'Added' } });
			await Promise.resolve();
			expect(store.items()).toContainEqual({ id: 'note-2', title: 'Added' });
			addGate.resolve(undefined);
			expect((await add).isOk()).toBe(true);

			const mutateGate = system.database.pauseNextCommit();
			const mutate = store.mutate({ query: { id: 'note-1' }, input: { title: 'Changed' } });
			await Promise.resolve();
			expect(store.items()).toContainEqual({ id: 'note-1', title: 'Changed' });
			mutateGate.resolve(undefined);
			expect((await mutate).isOk()).toBe(true);

			const deleteGate = system.database.pauseNextCommit();
			const remove = store.delete({ query: { id: 'note-1' } });
			await Promise.resolve();
			expect(store.items().some((note) => note.id === 'note-1')).toBe(false);
			deleteGate.resolve(undefined);
			expect((await remove).isOk()).toBe(true);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('rolls back failed writes with memory recovery metadata', async () => {
		const system = createSyncTestSystem();
		system.database.seed('workspace-1', [{ id: 'note-1', title: 'Initial' }]);
		const store = system.createStore();
		try {
			await store.hydrate();
			system.database.failNextCommit('database unavailable');
			const result = await store.mutate({ query: { id: 'note-1' }, input: { title: 'Temporary' } });
			expect(result.isErr()).toBe(true);
			if (result.isErr()) {
				expect(result.error.recovery).toEqual({ restored: true, source: 'memory' });
			}
			expect(store.items()).toEqual([{ id: 'note-1', title: 'Initial' }]);
			expect(store.isPending()).toBe(false);
		} finally {
			store.dispose();
			await system.transport.close();
		}
	});

	it('characterizes bare successful acknowledgements as immediate finality', async () => {
		let listRead = true;
		configureSync({
			cache: { adapter: bareAckCache },
			transport: bareAckTransport,
			createId: (prefix) => `${prefix}-1`,
			fetch: async (_input, init) => {
				if (listRead) {
					listRead = false;
					return new Response(JSON.stringify({ isOk: true, isError: false, value: { items: [] } }));
				}
				if (typeof init?.body !== 'string') {
					throw new Error('Expected a JSON request body.');
				}
				const body = JSON.parse(init.body) as { input: { id: string; title: string } };
				return new Response(JSON.stringify({ isOk: true, isError: false, value: body.input }));
			}
		});
		const store = createStore<BareAckManager>({
			key: 'bare-ack',
			getParams: () => ({ workspaceId: 'w1' }),
			getUrl: () => 'http://sync.test/notes',
			query: () => undefined
		});
		try {
			await store.hydrate();
			const result = await store.add({ input: { id: 'note-1', title: 'Added' } });
			expect(result.isOk()).toBe(true);
			expect(store.isPending()).toBe(false);
			// DIVERGENCE: a bare HTTP acknowledgement clears pending without authoritative envelope coverage.
			expect(store.items()).toEqual([]);
		} finally {
			store.dispose();
		}
	});
});
