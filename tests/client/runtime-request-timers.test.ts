import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime } from '../../src/client/core.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';

afterEach(() => {
	vi.useRealTimers();
});

function harness(batchConnect: boolean, outcome: 'success' | 'http-error' | 'reject' | 'headers' | 'body') {
	const signals: AbortSignal[] = [];
	const cancelled = vi.fn();
	const runtime = createRuntime({
		cache: noopCache,
		batchConnect,
		fetch: async (input, init) => {
			const path = new URL(input instanceof Request ? input.url : String(input), 'http://sync.test').pathname;
			if (path.endsWith('/stream')) {
				return new Response(new ReadableStream<Uint8Array>({ start() {} }));
			}
			const signal = init?.signal;
			if (!signal) {
				throw new Error('Missing request signal');
			}
			signals.push(signal);
			if (path.endsWith('/release')) {
				return new Response(new ReadableStream({ cancel: cancelled }));
			}
			if (outcome === 'reject') {
				throw new Error('Network failed');
			}
			if (outcome === 'headers') {
				return new Promise<Response>((_resolve, reject) => {
					signal.addEventListener('abort', () => reject(signal.reason), { once: true });
				});
			}
			if (outcome === 'body') {
				return new Response(
					new ReadableStream({
						start(controller) {
							signal.addEventListener('abort', () => controller.error(signal.reason), { once: true });
						}
					})
				);
			}
			if (outcome === 'http-error') {
				return new Response('unavailable', { status: 503 });
			}
			const body = { isOk: true, isError: false, value: { scope: 'notes:ws' } };
			return Response.json(batchConnect ? { results: [{ status: 200, body }] } : body);
		}
	});
	const controller = new AbortController();
	const subscribe = () =>
		runtime.transport.subscribe({
			connectUrl: '/notes/connect',
			managerKey: 'notes',
			scope: 'notes:ws',
			signal: controller.signal,
			onEnvelope() {}
		});
	return { runtime, controller, subscribe, signals, cancelled };
}

describe.each([true, false])('request timers with batchConnect=%s', (batchConnect) => {
	it.each(['success', 'http-error', 'reject'] as const)(
		'clears connect and release request timers after %s',
		async (outcome) => {
			vi.useFakeTimers();
			const h = harness(batchConnect, outcome);
			const baseline = vi.getTimerCount();
			try {
				const result = await h.subscribe();
				expect(result.isOk()).toBe(outcome === 'success');
				// Failed registrations own a retry timer, not a request timer.
				expect(vi.getTimerCount()).toBe(baseline + (outcome === 'success' ? 0 : 1));
				h.controller.abort();
				await vi.advanceTimersByTimeAsync(0);
				expect(h.cancelled).toHaveBeenCalledTimes(1);
				expect(vi.getTimerCount()).toBe(baseline);
				await vi.advanceTimersByTimeAsync(30_000);
				expect(h.signals.every((signal) => !signal.aborted)).toBe(true);
			} finally {
				h.controller.abort();
				h.runtime.dispose();
			}
		}
	);

	it.each(['headers', 'body'] as const)('aborts hung %s at 30 seconds', async (outcome) => {
		vi.useFakeTimers();
		const h = harness(batchConnect, outcome);
		try {
			const result = h.subscribe();
			await vi.advanceTimersByTimeAsync(0);
			expect(h.signals).toHaveLength(1);
			await vi.advanceTimersByTimeAsync(29_999);
			expect(h.signals[0]?.aborted).toBe(false);
			await vi.advanceTimersByTimeAsync(1);
			expect(h.signals[0]?.aborted).toBe(true);
			expect((await result).isErr()).toBe(true);
			h.controller.abort();
			await vi.advanceTimersByTimeAsync(0);
			expect(vi.getTimerCount()).toBe(0);
		} finally {
			h.controller.abort();
			h.runtime.dispose();
		}
	});
});
