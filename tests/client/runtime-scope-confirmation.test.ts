import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime } from '../../src/client/core.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import { encodeSseFrame } from '../../src/shared/sse.ts';
import { waitFor } from '../fixtures/async.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	vi.restoreAllMocks();
});

function envelope(scope: string, cursor = 'cursor-1'): SyncEnvelope {
	return { managerKey: 'notes', scope, cursor, changes: [] };
}

function confirmed(scope: string): Response {
	return Response.json({ isOk: true, isError: false, value: { scope } });
}

function setup() {
	vi.useFakeTimers();
	vi.spyOn(Math, 'random').mockReturnValue(0.5);
	const connects: Deferred<Response>[] = [];
	const streams: ReadableStreamDefaultController<Uint8Array>[] = [];
	const fetchImpl: typeof fetch = async (input) => {
		const url = input instanceof Request ? input.url : String(input);
		if (url.endsWith('/stream')) {
			return new Response(new ReadableStream<Uint8Array>({ start: (controller) => streams.push(controller) }), {
				headers: { 'content-type': 'text/event-stream' }
			});
		}
		const response = new Deferred<Response>();
		connects.push(response);
		return response.promise;
	};
	const runtime = createRuntime({ cache: noopCache, batchConnect: false, fetch: fetchImpl });
	function subscribe(scope: string, received: SyncEnvelope[], signal = new AbortController().signal) {
		return runtime.transport.subscribe({
			connectUrl: `/notes/${scope}/connect`,
			managerKey: 'notes',
			scope,
			signal,
			onEnvelope: (value) => received.push(value)
		});
	}
	async function send(...values: SyncEnvelope[]) {
		for (const value of values) {
			streams.at(-1)?.enqueue(encodeSseFrame('sync', value));
		}
		await vi.advanceTimersByTimeAsync(0);
	}
	return { runtime, connects, streams, subscribe, send };
}

describe('server-confirmed subscription scopes', () => {
	it('buffers separately for two unconfirmed scopes of the same manager', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const first: SyncEnvelope[] = [];
			const second: SyncEnvelope[] = [];
			const a = subscribe('client-a', first);
			const b = subscribe('client-b', second);
			await waitFor(() => connects.length === 2);
			const value = envelope('server-x');
			await send(value);
			expect([first, second]).toEqual([[], []]);
			connects[0]?.resolve(confirmed('server-x'));
			connects[1]?.resolve(confirmed('server-y'));
			expect((await a).isOk()).toBe(true);
			expect((await b).isOk()).toBe(true);
			expect(first).toEqual([value]);
			expect(second).toEqual([]);
		} finally {
			runtime.dispose();
		}
	});

	it('never adopts an unrelated envelope scope while connect is pending', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('client-a', received);
			await waitFor(() => connects.length === 1);
			await send(envelope('other-organization'));
			expect(received).toEqual([]);
			connects[0]?.resolve(confirmed('own-organization'));
			expect((await subscription).isOk()).toBe(true);
			await send(envelope('other-organization', 'cursor-2'));
			expect(received).toEqual([]);
		} finally {
			runtime.dispose();
		}
	});

	it('delivers its own buffered envelopes exactly once in order only after confirmation', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('server-x', received);
			await waitFor(() => connects.length === 1);
			const first = envelope('server-x');
			const second = envelope('server-x', 'cursor-2');
			const values = [first, second];
			await send(first, envelope('other'), second);
			expect(received).toEqual([]);
			connects[0]?.resolve(confirmed('server-x'));
			expect((await subscription).isOk()).toBe(true);
			expect(received).toEqual(values);
			await send(envelope('server-x', 'cursor-3'));
			expect(received).toEqual([...values, envelope('server-x', 'cursor-3')]);
		} finally {
			runtime.dispose();
		}
	});

	it('buffers for an unconfirmed entry even when another confirmed entry receives the envelope', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const first: SyncEnvelope[] = [];
			const second: SyncEnvelope[] = [];
			const a = subscribe('client-a', first);
			await waitFor(() => connects.length === 1);
			connects[0]?.resolve(confirmed('server-x'));
			await a;
			const b = subscribe('client-b', second);
			await waitFor(() => connects.length === 2);
			const value = envelope('server-x');
			await send(value);
			expect(first).toEqual([value]);
			expect(second).toEqual([]);
			connects[1]?.resolve(confirmed('server-x'));
			await b;
			expect(second).toEqual([value]);
		} finally {
			runtime.dispose();
		}
	});

	it('discards envelopes from a failed registration before retrying', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('client-a', received);
			await waitFor(() => connects.length === 1);
			await send(envelope('server-x', 'failed-attempt'));
			connects[0]?.resolve(new Response('unavailable', { status: 503 }));
			expect((await subscription).isErr()).toBe(true);
			await vi.advanceTimersByTimeAsync(1000);
			expect(connects).toHaveLength(2);
			const value = envelope('server-x', 'retry-attempt');
			await send(value);
			connects[1]?.resolve(confirmed('server-x'));
			await vi.advanceTimersByTimeAsync(0);
			expect(received).toEqual([value]);
		} finally {
			runtime.dispose();
		}
	});

	it('discards buffered envelopes and ignores a late response when reconnect restarts registration', async () => {
		const { runtime, connects, streams, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('client-a', received);
			await waitFor(() => connects.length === 1);
			await send(envelope('server-x', 'old-attempt'));
			streams[0]?.close();
			await vi.advanceTimersByTimeAsync(1000);
			expect(connects).toHaveLength(2);
			connects[0]?.resolve(confirmed('wrong-scope'));
			const value = envelope('server-x', 'new-attempt');
			await send(value);
			connects[1]?.resolve(confirmed('server-x'));
			expect((await subscription).isOk()).toBe(true);
			expect(received).toEqual([value]);
		} finally {
			runtime.dispose();
		}
	});

	it('discards buffers when the last subscriber aborts', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const abort = new AbortController();
			const first = subscribe('client-a', received, abort.signal);
			await waitFor(() => connects.length === 1);
			await send(envelope('server-x', 'aborted-attempt'));
			abort.abort();
			expect((await first).isErr()).toBe(true);
			const second = subscribe('client-a', received);
			await waitFor(() => connects.length === 2);
			connects[0]?.resolve(confirmed('server-x'));
			connects[1]?.resolve(confirmed('server-x'));
			expect((await second).isOk()).toBe(true);
			expect(received).toEqual([]);
		} finally {
			runtime.dispose();
		}
	});

	it('rejects a successful response without an authoritative scope', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('client-a', received);
			await waitFor(() => connects.length === 1);
			await send(envelope('client-a'));
			connects[0]?.resolve(Response.json({ isOk: true, isError: false, value: {} }));
			expect((await subscription).isErr()).toBe(true);
			expect(received).toEqual([]);
		} finally {
			runtime.dispose();
		}
	});

	it('drops the oldest envelopes beyond the per-subscription cap', async () => {
		const { runtime, connects, subscribe, send } = setup();
		try {
			const received: SyncEnvelope[] = [];
			const subscription = subscribe('client-a', received);
			await waitFor(() => connects.length === 1);
			const values = Array.from({ length: 258 }, (_, i) => envelope('server-x', `cursor-${i}`));
			await send(...values);
			connects[0]?.resolve(confirmed('server-x'));
			await subscription;
			expect(received).toEqual(values.slice(2));
		} finally {
			runtime.dispose();
		}
	});
});
