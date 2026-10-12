import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime, ok, type CacheAdapter, type CacheItem, type RuntimeTransport } from '../../src/client/core.ts';

const DAY_MS = 24 * 60 * 60 * 1000;
const transport: RuntimeTransport = {
	async subscribe() {
		return ok(() => {});
	}
};

function memoryCache(entries: Record<string, CacheItem<unknown>>, withKeys = true) {
	const store = new Map(Object.entries(entries));
	const listed: number[] = [];
	const adapter: CacheAdapter = {
		async get<TValue>(key: string) {
			return store.get(key) as CacheItem<TValue> | undefined;
		},
		async set(key, value) {
			store.set(key, value);
		},
		async del(key) {
			store.delete(key);
		},
		...(withKeys
			? {
					async keys() {
						listed.push(Date.now());
						return [...store.keys()];
					}
				}
			: {})
	};
	return { adapter, store, listed };
}

afterEach(() => {
	vi.useRealTimers();
});

describe('library cache sweep', () => {
	it('deletes expired snapshots a minute after start and keeps fresh and non-expiring ones', async () => {
		vi.useFakeTimers({ now: 10 * DAY_MS });
		const now = Date.now();
		const { adapter, store } = memoryCache({
			expired: { value: 'old', expiry: now - 1 },
			fresh: { value: 'new', expiry: now + DAY_MS },
			forever: { value: 'kept', expiry: 0 }
		});
		const runtime = createRuntime({ cache: adapter, transport });
		await vi.advanceTimersByTimeAsync(59_000);
		expect(store.has('expired')).toBe(true);
		await vi.advanceTimersByTimeAsync(1_000);
		expect([...store.keys()].sort()).toEqual(['forever', 'fresh', 'sync-resource:cache-sweep']);
		runtime.dispose();
	});

	it('sweeps at most once a day across runtimes', async () => {
		vi.useFakeTimers({ now: 10 * DAY_MS });
		const { adapter, listed } = memoryCache({});
		createRuntime({ cache: adapter, transport });
		await vi.advanceTimersByTimeAsync(60_000);
		createRuntime({ cache: adapter, transport });
		await vi.advanceTimersByTimeAsync(60_000);
		expect(listed).toHaveLength(1);
		vi.setSystemTime(Date.now() + DAY_MS - 180_000);
		createRuntime({ cache: adapter, transport });
		await vi.advanceTimersByTimeAsync(60_000);
		expect(listed).toHaveLength(1);
		vi.setSystemTime(Date.now() + DAY_MS);
		createRuntime({ cache: adapter, transport });
		await vi.advanceTimersByTimeAsync(60_000);
		expect(listed).toHaveLength(2);
	});

	it('does not sweep an adapter without keys or a disposed runtime', async () => {
		vi.useFakeTimers({ now: 10 * DAY_MS });
		const withoutKeys = memoryCache({ expired: { value: 'old', expiry: 1 } }, false);
		createRuntime({ cache: withoutKeys.adapter, transport });
		const disposed = memoryCache({ expired: { value: 'old', expiry: 1 } });
		createRuntime({ cache: disposed.adapter, transport }).dispose();
		await vi.advanceTimersByTimeAsync(60_000);
		expect(withoutKeys.store.has('expired')).toBe(true);
		expect(disposed.listed).toHaveLength(0);
		expect(disposed.store.has('expired')).toBe(true);
	});
});
