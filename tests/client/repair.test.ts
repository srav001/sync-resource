import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRepairScheduler, needsRepair } from '../../src/client/repair.ts';
import type { PageState } from '../../src/client/types.ts';
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

describe('repair need', () => {
	it('is false for clean pages and true for a stale page', () => {
		expect(needsRepair([])).toBe(false);
		expect(needsRepair([page()])).toBe(false);
		expect(needsRepair([page(), page({ stale: true })])).toBe(true);
	});

	it('is true for any reset, including with clean pages', () => {
		expect(needsRepair([page()], { scope: 'notes:workspace-1', reason: 'manual' })).toBe(true);
	});
});

describe('repair scheduler', () => {
	it('coalesces queued requests and runs one callback', async () => {
		vi.useFakeTimers();
		const scheduler = createRepairScheduler();
		let calls = 0;
		const run = async () => {
			calls += 1;
			return ok(undefined);
		};
		const first = scheduler.request(run);
		const second = scheduler.request(run);
		expect(second).toBe(first);
		await vi.advanceTimersByTimeAsync(0);
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
		const scheduler = createRepairScheduler();
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
