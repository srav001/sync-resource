import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRepairScheduler, noRepairNeeded, planRepair } from '../../src/client/repair.ts';
import type { PageState, ResetManifest } from '../../src/client/types.ts';
import { err, ok } from '../../src/shared/result.ts';

const page = (overrides: Partial<PageState> = {}): PageState => ({
	ids: [],
	coverage: 'full',
	complete: true,
	stale: false,
	repairNeeded: false,
	source: 'network',
	...overrides
});

afterEach(() => {
	vi.useRealTimers();
});

describe('repair planning', () => {
	it('returns none for clean pages and counts each stale condition once', () => {
		expect(planRepair([])).toEqual({ kind: 'none', stalePageCount: 0 });
		expect(
			planRepair([page(), page({ stale: true }), page({ repairNeeded: true }), page({ coverage: 'stale' })])
		).toEqual({
			kind: 'page',
			stalePageCount: 3
		});
	});

	it.each([
		['ordinary reset', { scope: 'notes:workspace-1', reason: 'manual' }],
		['repair reset', { scope: 'notes:workspace-1', reason: 'repair' }]
	] satisfies readonly [string, ResetManifest][])('plans %s as page repair', (_name, reset) => {
		expect(planRepair([page({ stale: true })], reset)).toEqual({
			kind: 'page',
			stalePageCount: 1,
			reason: reset.reason
		});
	});

	it('plans retention gaps as scope repair, including clean pages', () => {
		expect(
			planRepair([page(), page({ stale: true })], {
				scope: 'notes:workspace-1',
				reason: 'retention_gap'
			})
		).toEqual({
			kind: 'scope',
			stalePageCount: 1,
			reason: 'retention_gap'
		});
	});

	it('returns a successful result for noRepairNeeded', async () => {
		const result = await noRepairNeeded();
		expect(result.isOk()).toBe(true);
	});
});

describe('repair scheduler', () => {
	it('coalesces queued requests and runs one callback', async () => {
		vi.useFakeTimers();
		const scheduler = createRepairScheduler(10);
		let calls = 0;
		const run = async () => {
			calls += 1;
			return ok(undefined);
		};
		const first = scheduler.request(run);
		const second = scheduler.request(run);
		expect(second).toBe(first);
		await vi.advanceTimersByTimeAsync(10);
		expect((await first).isOk()).toBe(true);
		expect(calls).toBe(1);
		scheduler.dispose();
	});

	it('serializes a queued repair behind an active repair', async () => {
		vi.useFakeTimers();
		const scheduler = createRepairScheduler();
		let release!: () => void;
		const active = new Promise<void>((resolve) => {
			release = resolve;
		});
		const order: string[] = [];
		const first = scheduler.request(async () => {
			order.push('first:start');
			await active;
			order.push('first:end');
			return ok(undefined);
		});
		await vi.advanceTimersByTimeAsync(0);
		const second = scheduler.request(async () => {
			order.push('second');
			return ok(undefined);
		});
		await vi.advanceTimersByTimeAsync(0);
		expect(order).toEqual(['first:start']);
		release();
		await first;
		await vi.advanceTimersByTimeAsync(0);
		expect((await second).isOk()).toBe(true);
		expect(order).toEqual(['first:start', 'first:end', 'second']);
		scheduler.dispose();
	});

	it('normalizes thrown repair callbacks to internal errors', async () => {
		vi.useFakeTimers();
		const scheduler = createRepairScheduler();
		const resultPromise = scheduler.request(async () => {
			throw new Error('repair failed');
		});
		await vi.advanceTimersByTimeAsync(0);
		const result = await resultPromise;
		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('internal');
			expect(result.error.message).toBe('repair failed');
		}
		scheduler.dispose();
	});

	it('resolves queued work as disposed and rejects new work after disposal', async () => {
		vi.useFakeTimers();
		const scheduler = createRepairScheduler(100);
		const queued = scheduler.request(async () => ok(undefined));
		scheduler.dispose();
		const queuedResult = await queued;
		expect(queuedResult.isErr()).toBe(true);
		if (queuedResult.isErr()) {
			expect(queuedResult.error.code).toBe('disposed');
		}
		const after = await scheduler.request(async () => err('internal', 'should not run'));
		expect(after.isErr()).toBe(true);
		if (after.isErr()) {
			expect(after.error.code).toBe('disposed');
		}
	});
});
