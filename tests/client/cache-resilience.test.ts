import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createStore,
	ok,
	resetSyncConfiguration,
	type CacheAdapter,
	type CacheItem,
	type ManagerTypeShape,
	type MethodType,
	type RuntimeTransport,
	type SyncEnvelope
} from '../../src/client/core.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface CacheManager extends ManagerTypeShape {
	readonly key: 'cache-resilience';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<
			never,
			never,
			{ readonly items: readonly { readonly id: string; readonly title: string }[] }
		>;
	};
}

class ManualTransport implements RuntimeTransport {
	private onEnvelope: ((envelope: SyncEnvelope) => void) | undefined;

	async subscribe(options: Parameters<RuntimeTransport['subscribe']>[0]) {
		this.onEnvelope = options.onEnvelope;
		return ok(() => {
			this.onEnvelope = undefined;
		});
	}

	emit(envelope: SyncEnvelope): void {
		this.onEnvelope?.(envelope);
	}
}

beforeEach(() => {
	vi.useFakeTimers();
});

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
});

function createCacheStore(): ReturnType<typeof createStore<CacheManager>> {
	return createStore<CacheManager>({
		key: 'cache-resilience',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => undefined
	});
}

describe('cache scheduling resilience', () => {
	it('enforces the 250ms maximum wait while repeated updates keep resetting the trailing delay', async () => {
		const writes: CacheItem<unknown>[] = [];
		const cache = cacheWithSet(async (value) => {
			writes.push(value);
		});
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => syncResponse({ items: [{ id: 'note-0', title: 'Zero' }] })
		});
		const store = createCacheStore();
		expect((await store.hydrate()).isOk()).toBe(true);

		for (let elapsed = 40; elapsed <= 240; elapsed += 40) {
			await vi.advanceTimersByTimeAsync(40);
			transport.emit(envelope(`cursor-${elapsed}`, `note-${elapsed}`));
		}
		expect(writes).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(9);
		expect(writes).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(1);
		expect(writes).toHaveLength(1);
		store.dispose();
	});

	it('writes again when authoritative state becomes dirty during an in-flight cache write', async () => {
		const firstWrite = new Deferred();
		const writes: CacheItem<unknown>[] = [];
		const cache = cacheWithSet(async (value) => {
			writes.push(value);
			if (writes.length === 1) {
				await firstWrite.promise;
			}
		});
		const transport = new ManualTransport();
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => syncResponse({ items: [{ id: 'note-0', title: 'Zero' }] })
		});
		const store = createCacheStore();
		await store.hydrate();
		await vi.advanceTimersByTimeAsync(50);
		expect(writes).toHaveLength(1);

		transport.emit(updateEnvelope('cursor-new', 'Updated while saving'));
		firstWrite.resolve(undefined);
		await Promise.resolve();
		await vi.advanceTimersByTimeAsync(50);
		expect(writes).toHaveLength(2);
		expect(cachedTitle(writes[1], 'note-0')).toBe('Updated while saving');
		store.dispose();
	});

	it('surfaces cache write failures without losing the authoritative in-memory state', async () => {
		const cache = cacheWithSet(async () => {
			throw new Error('cache quota exceeded');
		});
		configureSync({
			cache: { adapter: cache },
			transport: new ManualTransport(),
			fetch: async () => syncResponse({ items: [{ id: 'note-1', title: 'Authoritative' }] })
		});
		const store = createCacheStore();
		await store.hydrate();
		await vi.advanceTimersByTimeAsync(50);
		await waitFor(() => store.error() !== null);

		expect(store.error()?.message).toBe('cache quota exceeded');
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Authoritative' }]);
		store.dispose();
	});
});

function cacheWithSet(write: (value: CacheItem<unknown>) => Promise<void>): CacheAdapter {
	return {
		async get() {
			return undefined;
		},
		async set(_key, value) {
			await write(value as CacheItem<unknown>);
		},
		async del() {}
	};
}

function envelope(cursor: string, id: string): SyncEnvelope {
	return {
		managerKey: 'cache-resilience',
		scope: 'workspace-1',
		cursor,
		changes: [{ type: 'itemAdded', id, value: { id, title: id } }]
	};
}

function updateEnvelope(cursor: string, title: string): SyncEnvelope {
	return {
		managerKey: 'cache-resilience',
		scope: 'workspace-1',
		cursor,
		changes: [{ type: 'itemUpdated', id: 'note-0', value: { id: 'note-0', title } }]
	};
}

function cachedTitle(item: CacheItem<unknown> | undefined, id: string): string | undefined {
	if (!item || typeof item.value !== 'object' || item.value === null || !('items' in item.value)) {
		return undefined;
	}
	const items = (
		item.value as {
			readonly items?: readonly { readonly id?: string; readonly title?: string }[];
		}
	).items;
	return items?.find((value) => value.id === id)?.title;
}

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}

async function waitFor(condition: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await Promise.resolve();
	}
	throw new Error('Condition was not reached.');
}
