import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

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

interface RepairManager extends ManagerTypeShape {
	readonly key: 'store-repair';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<never, never, { readonly items: readonly { readonly id: string; title: string }[] }>;
		readonly mutate: MethodType<
			{ readonly id: string },
			{ readonly title: string },
			{ readonly id: string; readonly title: string }
		>;
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

beforeEach(() => {
	vi.useFakeTimers();
});

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
});

function createRepairStore() {
	return createStore<RepairManager>({
		key: 'store-repair',
		getParams: () => ({ workspaceId: 'workspace-1' }),
		getUrl: () => 'http://sync.test/notes',
		query: () => undefined
	});
}

describe('store reset and repair', () => {
	it('clears pending overlays on reset, refreshes stale pages, and becomes clean after repair', async () => {
		const transport = new ManualTransport();
		const writeResponse = new Deferred<Response>();
		let reads = 0;
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async (_input, init) => {
				if (init?.method === 'POST') {
					return writeResponse.promise;
				}
				reads += 1;
				return syncResponse({
					items: [{ id: 'note-1', title: reads === 1 ? 'Initial' : 'Repaired' }]
				});
			}
		});
		const store = createRepairStore();
		await store.hydrate();
		const write = store.mutate({ query: { id: 'note-1' }, input: { title: 'Optimistic' } });
		expect(store.pending()).toHaveLength(1);
		expect(store.items()[0]?.title).toBe('Optimistic');

		transport.emit(resetEnvelope());
		expect(store.pending()).toHaveLength(0);
		expect(store.pages()[0]).toMatchObject({ stale: true, repairNeeded: true, coverage: 'stale' });
		await vi.advanceTimersByTimeAsync(0);
		await waitFor(() => reads === 2);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Repaired' }]);
		expect(store.pages()[0]).toMatchObject({ stale: false, repairNeeded: false, coverage: 'full' });

		const readsAfterAutomaticRepair = reads;
		expect((await store.repair()).isOk()).toBe(true);
		expect(reads).toBe(readsAfterAutomaticRepair);

		writeResponse.resolve(syncErrorResponse('invalidated by reset'));
		expect((await write).isErr()).toBe(true);
		store.dispose();
	});

	it('retains the repair request after a failed automatic refresh so an explicit retry converges', async () => {
		const transport = new ManualTransport();
		let reads = 0;
		configureSync({
			cache: { adapter: cache },
			transport,
			fetch: async () => {
				reads += 1;
				if (reads === 2) {
					return syncErrorResponse('repair read failed');
				}
				return syncResponse({
					items: [{ id: 'note-1', title: reads === 1 ? 'Initial' : 'Recovered' }]
				});
			}
		});
		const store = createRepairStore();
		await store.hydrate();
		transport.emit(resetEnvelope());
		await vi.advanceTimersByTimeAsync(0);
		await waitFor(() => reads === 2 && store.error() !== null);
		expect(store.error()?.message).toBe('repair read failed');

		const retry = store.repair();
		await vi.advanceTimersByTimeAsync(0);
		expect((await retry).isOk()).toBe(true);
		expect(reads).toBe(3);
		expect(store.items()).toEqual([{ id: 'note-1', title: 'Recovered' }]);
		store.dispose();
	});
});

function resetEnvelope(): SyncEnvelope {
	const manifest = {
		scope: 'workspace-1',
		reason: 'retention_gap' as const,
		previousCursor: 'cursor-old',
		nextCursor: 'cursor-reset'
	};
	return {
		managerKey: 'store-repair',
		scope: 'workspace-1',
		cursor: 'cursor-reset',
		changes: [{ type: 'reset', manifest }],
		reset: manifest
	};
}

function syncResponse(value: unknown): Response {
	return Response.json({ isOk: true, isError: false, value });
}

function syncErrorResponse(message: string): Response {
	return Response.json({
		isOk: false,
		isError: true,
		error: { code: 'internal', message }
	});
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
