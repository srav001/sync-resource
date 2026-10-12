import { afterEach, describe, expect, it } from 'vite-plus/test';

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

interface ScopeManager extends ManagerTypeShape {
	readonly key: 'scope-change';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			{ readonly limit: number },
			never,
			{ readonly items: readonly { readonly id: string }[] }
		>;
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

describe('realtime subscription follows the store scope', () => {
	it('unsubscribes the old scope and subscribes the new one when params change', async () => {
		const events: string[] = [];
		const transport: RuntimeTransport = {
			async subscribe(options) {
				events.push(`subscribe ${options.connectUrl}`);
				return ok(() => {
					events.push(`unsubscribe ${options.connectUrl}`);
				});
			}
		};
		let workspaceId = 'w1';
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => Response.json({ isOk: true, isError: false, value: { items: [] } })
		});
		const store = createStore<ScopeManager>({
			key: 'scope-change',
			getParams: () => ({ workspaceId }),
			getUrl: () => `http://sync.test/${workspaceId}/notes`,
			query: () => ({ limit: 10 })
		});
		try {
			await store.hydrate();
			workspaceId = 'w2';
			await store.hydrate();
			await store.connect();

			expect(events).toEqual([
				'subscribe http://sync.test/w1/notes/connect',
				'unsubscribe http://sync.test/w1/notes/connect',
				'subscribe http://sync.test/w2/notes/connect'
			]);
		} finally {
			store.dispose();
		}
	});
});
