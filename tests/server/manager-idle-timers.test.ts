import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	manager,
	resource,
	type ManagerRealtimeBus,
	type ManagerTelemetryContext,
	type SyncError
} from '../../src/server/index.ts';
import { releaseSharedStreamSubscriptions, resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { nextMacrotask, waitFor } from '../fixtures/async.ts';
import { connectRequest, openSharedStream, outboxReading } from '../fixtures/sharedStream.ts';

afterEach(() => {
	resetSharedStreamForTests();
	vi.useRealTimers();
});

interface IdleManagerOptions {
	realtimeBus?: ManagerRealtimeBus;
	handleError?(error: SyncError, context: ManagerTelemetryContext): void;
}

function createManager(
	key: string,
	stream?: { idleTtlMs?: number; onScopeIdle(scope: string): void },
	options: IdleManagerOptions = {}
) {
	return manager({
		key,
		resource: resource({ parse: (_value: unknown) => ({ workspaceId: 'ws' }) }, (method) => ({
			get: method.get({
				output: { parse: (_value: unknown) => ({ workspaceId: 'ws' }) },
				handler: ({ params, ctx }) => ctx.ok(params)
			})
		})),
		authorize: () => true,
		scope: (params) => params.workspaceId,
		persistence: outboxReading(),
		stream,
		...options
	});
}

describe('manager idle timers', () => {
	it('releases the realtime bus lease when a zero-TTL holder releases in the first microtask after registration', async () => {
		const idle = vi.fn();
		const unsubscribe = vi.fn();
		const notes = createManager(
			'notes',
			{ idleTtlMs: 0, onScopeIdle: idle },
			{
				realtimeBus: { publish() {}, subscribe: () => unsubscribe }
			}
		);
		const request = new Request('http://sync.test/connect?subscription=holder', {
			method: 'POST',
			headers: { 'x-sync-transport-id': 'fixture-transport' }
		});
		const connecting = notes.http.connect({ request, params: { workspaceId: 'ws' } });
		// Release in the first available microtask after registration, before connect necessarily resolves.
		await waitFor(() => {
			releaseSharedStreamSubscriptions(request, ['holder']);
			return idle.mock.calls.length > 0;
		});
		expect((await connecting).status).toBe(200);
		await nextMacrotask();
		expect(idle).toHaveBeenCalledExactlyOnceWith('notes:ws');
		expect(unsubscribe).toHaveBeenCalledTimes(1);
	});

	it.each([0, 10])(
		'contains idle hook failures and cleans both managers on one transport with TTL %i',
		async (idleTtlMs) => {
			vi.useFakeTimers();
			const failure = new Error('idle hook failed');
			const firstIdle = vi.fn(() => {
				throw failure;
			});
			const secondIdle = vi.fn();
			const report = vi.fn(async () => {
				throw new Error('error hook failed');
			});
			const first = createManager('first', { idleTtlMs, onScopeIdle: firstIdle }, { handleError: report });
			const second = createManager('second', { idleTtlMs, onScopeIdle: secondIdle });
			const reader = await openSharedStream(first.http, { workspaceId: 'ws' });
			expect(
				(
					await second.http.connect({
						request: connectRequest('fixture-transport'),
						params: { workspaceId: 'ws' }
					})
				).status
			).toBe(200);
			await reader.cancel();
			await vi.advanceTimersByTimeAsync(idleTtlMs);
			expect(firstIdle).toHaveBeenCalledExactlyOnceWith('first:ws');
			expect(secondIdle).toHaveBeenCalledExactlyOnceWith('second:ws');
			expect(report).toHaveBeenCalledExactlyOnceWith(
				expect.objectContaining({ message: failure.message }),
				expect.objectContaining({ manager: 'first', method: 'stream.onScopeIdle', scope: 'first:ws' })
			);
			expect(vi.getTimerCount()).toBe(0);
		}
	);

	it('cleans each manager scope exactly once after its own idle TTL', async () => {
		vi.useFakeTimers();
		const firstIdle = vi.fn();
		const secondIdle = vi.fn();
		const first = createManager('first', { idleTtlMs: 100, onScopeIdle: firstIdle });
		const second = createManager('second', { idleTtlMs: 200, onScopeIdle: secondIdle });
		const reader = await openSharedStream(first.http, { workspaceId: 'ws' });
		await second.http.connect({
			request: new Request('http://sync.test/connect', {
				method: 'POST',
				headers: { 'x-sync-transport-id': 'fixture-transport' }
			}),
			params: { workspaceId: 'ws' }
		});
		await reader.cancel();
		await vi.advanceTimersByTimeAsync(99);
		expect(firstIdle).not.toHaveBeenCalled();
		expect(secondIdle).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1);
		expect(firstIdle).toHaveBeenCalledExactlyOnceWith('first:ws');
		expect(secondIdle).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(100);
		expect(secondIdle).toHaveBeenCalledExactlyOnceWith('second:ws');
		await vi.advanceTimersByTimeAsync(1_000);
		expect(firstIdle).toHaveBeenCalledTimes(1);
		expect(secondIdle).toHaveBeenCalledTimes(1);
		expect(vi.getTimerCount()).toBe(0);
	});

	it('arms no idle timer when the manager has no idle work', async () => {
		vi.useFakeTimers();
		const baseline = vi.getTimerCount();
		const notes = createManager('notes');
		const reader = await openSharedStream(notes.http, { workspaceId: 'ws' });
		await reader.cancel();
		expect(vi.getTimerCount()).toBe(baseline);
	});

	it('cancels the pending idle timer when the scope re-subscribes', async () => {
		vi.useFakeTimers();
		const idle = vi.fn();
		const notes = createManager('notes', { idleTtlMs: 100, onScopeIdle: idle });
		const first = await openSharedStream(notes.http, { workspaceId: 'ws' });
		const activeTimers = vi.getTimerCount();
		await first.cancel();
		expect(vi.getTimerCount()).toBe(1);
		await vi.advanceTimersByTimeAsync(99);
		const second = await openSharedStream(notes.http, { workspaceId: 'ws' });
		expect(vi.getTimerCount()).toBe(activeTimers);
		await vi.advanceTimersByTimeAsync(101);
		expect(idle).not.toHaveBeenCalled();
		expect(vi.getTimerCount()).toBe(activeTimers);
		await second.cancel();
		await vi.advanceTimersByTimeAsync(100);
		expect(idle).toHaveBeenCalledExactlyOnceWith('notes:ws');
		expect(vi.getTimerCount()).toBe(0);
	});

	it.each([0, -1])('cleans up immediately without an idle timer for TTL %i', async (idleTtlMs) => {
		vi.useFakeTimers();
		const idle = vi.fn();
		const notes = createManager('notes', { idleTtlMs, onScopeIdle: idle });
		const reader = await openSharedStream(notes.http, { workspaceId: 'ws' });
		await reader.cancel();
		expect(idle).toHaveBeenCalledExactlyOnceWith('notes:ws');
		expect(vi.getTimerCount()).toBe(0);
	});
});
