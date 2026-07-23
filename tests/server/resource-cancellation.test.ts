import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { ok, resource } from '../../src/server/index.ts';

const params = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { scope?: unknown }).scope !== 'string') {
			throw new Error('bad params');
		}
		return value as { scope: string };
	}
};

const output = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('bad output');
		}
		return value as { id: string };
	}
};

afterEach(() => {
	vi.useRealTimers();
});

describe('resource operation cancellation', () => {
	it('rejects a pre-aborted caller signal without invoking the handler', async () => {
		const caller = new AbortController();
		caller.abort('caller stopped');
		let calls = 0;
		const notes = resource(params, (method) => ({
			get: method.get({
				output,
				handler({ ctx }) {
					calls += 1;
					return ctx.ok({ id: 'late' });
				}
			})
		}));

		const result = await notes.get({ params: { scope: 'scope-1' } }, { signal: caller.signal });

		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('aborted');
		}
		expect(calls).toBe(0);
	});

	it('maps caller cancellation during an abort-aware handler to aborted', async () => {
		const caller = new AbortController();
		let handlerStarted = false;
		const notes = resource(params, (method) => ({
			get: method.get({
				output,
				handler({ ctx }) {
					handlerStarted = true;
					return new Promise((_, reject) => {
						ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason), { once: true });
					});
				}
			})
		}));

		const operation = notes.get({ params: { scope: 'scope-1' } }, { signal: caller.signal });
		await waitFor(() => handlerStarted);
		caller.abort('caller stopped');
		const result = await operation;

		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('aborted');
		}
	});

	it('exposes an absolute deadline and distinguishes timeout from caller cancellation', async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date('2026-07-23T00:00:00.000Z'));
		let deadline: number | undefined;
		const notes = resource(params, (method) => ({
			get: method.get({
				output,
				handler({ ctx }) {
					deadline = ctx.deadline;
					return new Promise((_, reject) => {
						ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason), { once: true });
					});
				}
			})
		}));

		const operation = notes.get({ params: { scope: 'scope-1' } }, { timeoutMs: 25 });
		await waitFor(() => deadline !== undefined);
		expect(deadline).toBe(Date.now() + 25);
		await vi.advanceTimersByTimeAsync(25);
		const result = await operation;

		expect(result.isErr()).toBe(true);
		if (result.isErr()) {
			expect(result.error.code).toBe('timeout');
		}
	});

	it('characterizes a handler output that resolves after caller cancellation', async () => {
		const caller = new AbortController();
		const handlerOutput = deferred<ReturnType<typeof ok<{ readonly id: string }>>>();
		let handlerStarted = false;
		let handlerSignal: AbortSignal | undefined;
		const notes = resource(params, (method) => ({
			get: method.get({
				output,
				handler({ ctx }) {
					handlerStarted = true;
					handlerSignal = ctx.signal;
					return handlerOutput.promise;
				}
			})
		}));

		const operation = notes.get({ params: { scope: 'scope-1' } }, { signal: caller.signal });
		await waitFor(() => handlerStarted);
		caller.abort('caller stopped');
		expect(handlerSignal?.aborted).toBe(true);
		handlerOutput.resolve(ok({ id: 'late' }));
		const result = await operation;

		// DIVERGENCE: a handler that ignores its aborted signal can still publish a successful late output.
		expect(result.isOk()).toBe(true);
		if (result.isOk()) {
			expect(result.value).toEqual({ output: { id: 'late' } });
		}
	});
});

function deferred<TValue>() {
	let resolvePromise!: (value: TValue | PromiseLike<TValue>) => void;
	const promise = new Promise<TValue>((resolve) => {
		resolvePromise = resolve;
	});
	return {
		promise,
		resolve: resolvePromise
	};
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
