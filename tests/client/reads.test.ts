import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	resetSyncConfiguration,
	type ManagerTypeShape,
	type MethodType
} from '../../src/client/core.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { noopCache, noopTransport } from '../fixtures/clientRuntime.ts';

interface ReadManager extends ManagerTypeShape {
	readonly key: 'read-tests';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			{ readonly limit: number },
			never,
			{ readonly items: readonly { readonly id: string }[] }
		>;
	};
}

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

function createReadStore(fetchImpl: typeof fetch): ReturnType<typeof createStore<ReadManager>> {
	configureSync({
		fetch: fetchImpl,
		cache: { adapter: noopCache },
		transport: noopTransport,
		createId: (prefix) => `${prefix}-1`
	});
	return createStore<ReadManager>({
		key: 'read-tests',
		getParams: () => ({ workspaceId: 'w1' }),
		getUrl: () => 'http://sync.test/workspaces/w1/notes',
		query: () => ({ limit: 20 })
	});
}

describe('client reads', () => {
	it('serializes list queries and rejects malformed HTTP payloads', async () => {
		const requests: Request[] = [];
		const store = createReadStore(async (input, init) => {
			const request = new Request(input, init);
			requests.push(request);
			return new Response(JSON.stringify({ malformed: true }), { status: 200 });
		});
		const result = await store.refresh();
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('validation');
		}
		expect(new URL(requests[0]!.url).searchParams.get('query')).toBe(JSON.stringify({ limit: 20 }));
		store.dispose();
	});

	it('returns disposed for hydration started after disposal', async () => {
		const store = createReadStore(
			async () => new Response(JSON.stringify({ isError: false, value: { items: [] } }))
		);
		store.dispose();
		const result = await store.hydrate();
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('disposed');
		}
	});
});
