import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime, type SyncRuntime } from '../../src/client/core.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import { encodeSseFrame } from '../../src/shared/sse.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';

interface ConnectResult {
	readonly status: number;
	readonly body: unknown;
}

interface ConnectRequest {
	readonly url: string;
	readonly headers: Headers;
	readonly connects?: readonly string[];
}

const forbidden = {
	isOk: false,
	isError: true,
	error: { code: 'forbidden', message: 'Not allowed.', retryable: false }
};

function registered(manager: string): ConnectResult {
	return { status: 200, body: { isOk: true, isError: false, value: { scope: `confirmed:${manager}` } } };
}

function managerOf(url: string): string {
	return new URL(url, 'http://sync.test').pathname.split('/')[1] ?? '';
}

function createHarness(
	resultFor: (url: string) => ConnectResult = (url) => registered(managerOf(url)),
	connectUrl?: string
) {
	const requests: ConnectRequest[] = [];
	let streamController!: ReadableStreamDefaultController<Uint8Array>;
	let batchStatus = 200;
	let streamRequests = 0;
	const fetchImpl: typeof fetch = async (input, init) => {
		const request = new Request(typeof input === 'string' ? new URL(input, 'http://sync.test') : input, init);
		const url = new URL(request.url);
		const headers = request.headers;
		if (url.pathname === '/api/sync-resource/stream') {
			streamRequests += 1;
			return new Response(
				new ReadableStream<Uint8Array>({
					start(controller) {
						streamController = controller;
					}
				}),
				{ headers: { 'content-type': 'text/event-stream' } }
			);
		}
		if (url.pathname === '/api/sync-resource/connect') {
			const { connects } = (await request.json()) as { connects: string[] };
			requests.push({ url: url.pathname, headers, connects });
			if (batchStatus !== 200) {
				return new Response('unavailable', { status: batchStatus });
			}
			return Response.json({ results: connects.map(resultFor) });
		}
		requests.push({ url: url.origin === 'http://sync.test' ? url.pathname + url.search : url.href, headers });
		const result = resultFor(url.pathname + url.search);
		return Response.json(result.body, { status: result.status });
	};
	const runtime = createRuntime({
		cache: noopCache,
		fetch: fetchImpl,
		connectUrl,
		createId: (prefix) => `${prefix}-batch`
	});
	return {
		runtime,
		requests,
		setBatchStatus(status: number) {
			batchStatus = status;
		},
		streamRequests: () => streamRequests,
		closeStream() {
			streamController.close();
		},
		push(envelope: SyncEnvelope) {
			streamController.enqueue(encodeSseFrame('sync', envelope));
		}
	};
}

function register(
	runtime: SyncRuntime,
	manager: string,
	signal: AbortSignal,
	received: SyncEnvelope[] = [],
	getCursor?: () => string | undefined,
	origin = ''
) {
	return runtime.transport.subscribe({
		url: `${origin}/${manager}/events`,
		connectUrl: `${origin}/${manager}/connect`,
		managerKey: manager,
		scope: `requested:${manager}`,
		signal,
		getCursor: getCursor ?? (() => `cursor-${manager}`),
		onEnvelope: (envelope) => received.push(envelope)
	});
}

function envelopeFor(manager: string): SyncEnvelope {
	return {
		managerKey: manager,
		scope: `confirmed:${manager}`,
		cursor: `next-${manager}`,
		changes: [{ type: 'itemAdded', id: `${manager}-1`, value: { id: `${manager}-1` } }]
	};
}

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 20; attempt += 1) {
		await Promise.resolve();
	}
}

afterEach(() => {
	vi.useRealTimers();
	vi.restoreAllMocks();
});

describe('batched realtime registration', () => {
	it('sends registrations queued in one tick as one request and confirms each scope from its own result', async () => {
		const harness = createHarness();
		const signal = new AbortController();
		const received: Record<string, SyncEnvelope[]> = { a: [], b: [], c: [] };
		try {
			const results = await Promise.all(
				['a', 'b', 'c'].map((manager) => register(harness.runtime, manager, signal.signal, received[manager]))
			);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(harness.requests).toHaveLength(1);
			expect(harness.requests[0]?.url).toBe('/api/sync-resource/connect');
			expect(harness.requests[0]?.connects).toEqual([
				'/a/connect?after=cursor-a',
				'/b/connect?after=cursor-b',
				'/c/connect?after=cursor-c'
			]);
			expect(harness.requests[0]?.headers.get('x-sync-transport-id')).toMatch(/^transport-/);
			expect(harness.requests[0]?.headers.get('x-client-id')).toBe(harness.runtime.clientId);

			for (const manager of ['a', 'b', 'c']) {
				harness.push(envelopeFor(manager));
			}
			await settle();
			expect(received.a).toEqual([envelopeFor('a')]);
			expect(received.b).toEqual([envelopeFor('b')]);
			expect(received.c).toEqual([envelopeFor('c')]);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('sends one direct request per registration when batchConnect is false', async () => {
		const harness = createHarness();
		const direct = createRuntime({
			cache: noopCache,
			batchConnect: false,
			fetch: (...args) => harness.runtime.fetch(...args),
			createId: (prefix) => `${prefix}-direct`
		});
		const signal = new AbortController();
		try {
			const results = await Promise.all(
				['a', 'b', 'c'].map((manager) => register(direct, manager, signal.signal))
			);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(harness.requests.map((request) => request.url)).toEqual([
				'/a/connect?after=cursor-a',
				'/b/connect?after=cursor-b',
				'/c/connect?after=cursor-c'
			]);
			expect(harness.requests.every((request) => request.connects === undefined)).toBe(true);
		} finally {
			signal.abort();
			direct.dispose();
			harness.runtime.dispose();
		}
	});

	it('fails only the item with an error result and retries it alone', async () => {
		vi.useFakeTimers();
		let attempts = 0;
		const harness = createHarness((url) => {
			if (managerOf(url) !== 'b') {
				return registered(managerOf(url));
			}
			attempts += 1;
			return attempts === 1 ? { status: 403, body: forbidden } : registered('b');
		});
		const signal = new AbortController();
		try {
			const results = await Promise.all(
				['a', 'b', 'c'].map((manager) => register(harness.runtime, manager, signal.signal))
			);
			expect(results.map((result) => result.isOk())).toEqual([true, false, true]);
			expect(harness.requests).toHaveLength(1);

			await vi.advanceTimersByTimeAsync(1300);
			expect(harness.requests).toHaveLength(2);
			expect(harness.requests[1]?.connects).toEqual(['/b/connect?after=cursor-b']);
			await vi.advanceTimersByTimeAsync(60_000);
			expect(harness.requests).toHaveLength(2);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('reports the error code the server sent for a failed item', async () => {
		vi.useFakeTimers();
		const harness = createHarness(() => ({ status: 403, body: forbidden }));
		const signal = new AbortController();
		try {
			const result = await register(harness.runtime, 'a', signal.signal);
			expect(result.isErr() && result.error.code).toBe('forbidden');
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it.each([
		[401, 'unauthorized'],
		[429, 'rate_limited'],
		[503, 'internal']
	])('classifies a bodyless %i batch response as %s', async (status, code) => {
		vi.useFakeTimers();
		const harness = createHarness();
		harness.setBatchStatus(status);
		const signal = new AbortController();
		try {
			const result = await register(harness.runtime, 'a', signal.signal);
			expect(result.isErr() && result.error.code).toBe(code);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('fails and retries every item when the batch response is not ok', async () => {
		vi.useFakeTimers();
		const harness = createHarness();
		harness.setBatchStatus(503);
		const signal = new AbortController();
		try {
			const results = await Promise.all(
				['a', 'b', 'c'].map((manager) => register(harness.runtime, manager, signal.signal))
			);
			expect(results.every((result) => result.isErr())).toBe(true);
			expect(harness.requests).toHaveLength(1);

			harness.setBatchStatus(200);
			await vi.advanceTimersByTimeAsync(1300);
			const retried = harness.requests.slice(1).flatMap((request) => request.connects ?? []);
			expect(retried.sort()).toEqual([
				'/a/connect?after=cursor-a',
				'/b/connect?after=cursor-b',
				'/c/connect?after=cursor-c'
			]);
			await vi.advanceTimersByTimeAsync(60_000);
			expect(harness.requests.slice(1).flatMap((request) => request.connects ?? [])).toHaveLength(3);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('leaves out a registration aborted before the flush', async () => {
		const harness = createHarness();
		const signal = new AbortController();
		const abortB = new AbortController();
		try {
			const results = await Promise.all([
				register(harness.runtime, 'a', signal.signal),
				register(harness.runtime, 'b', abortB.signal),
				register(harness.runtime, 'c', signal.signal, [], () => {
					abortB.abort();
					return 'cursor-c';
				})
			]);
			expect(results.map((result) => result.isOk())).toEqual([true, false, true]);
			expect(harness.requests).toHaveLength(1);
			expect(harness.requests[0]?.connects).toEqual(['/a/connect?after=cursor-a', '/c/connect?after=cursor-c']);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('splits more than 64 queued registrations into ceil(n / 64) requests', async () => {
		const harness = createHarness();
		const signal = new AbortController();
		try {
			const results = await Promise.all(
				Array.from({ length: 150 }, (_, index) => register(harness.runtime, `m${index}`, signal.signal))
			);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(harness.requests.map((request) => request.connects?.length)).toEqual([64, 64, 22]);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});
	it('batches absolute store URLs on the batch route origin as paths and connects other origins directly', async () => {
		const harness = createHarness(undefined, 'http://app.test/api/sync-resource/connect');
		const signal = new AbortController();
		try {
			const results = await Promise.all([
				register(harness.runtime, 'a', signal.signal, [], undefined, 'http://app.test'),
				register(harness.runtime, 'b', signal.signal, [], undefined, 'http://other.test')
			]);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(harness.requests.map((request) => request.connects ?? request.url)).toEqual([
				'http://other.test/b/connect?after=cursor-b',
				['/a/connect?after=cursor-a']
			]);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});

	it('re-registers every subscription with its latest cursor in one batch when the shared stream reconnects', async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, 'random').mockReturnValue(0.5);
		const harness = createHarness();
		const signal = new AbortController();
		const cursors: Record<string, string> = { a: 'a-1', b: 'b-1' };
		try {
			const results = await Promise.all(
				['a', 'b'].map((manager) =>
					register(harness.runtime, manager, signal.signal, [], () => cursors[manager])
				)
			);
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(harness.requests.map((request) => request.connects)).toEqual([
				['/a/connect?after=a-1', '/b/connect?after=b-1']
			]);

			cursors.a = 'a-2';
			cursors.b = 'b-2';
			harness.closeStream();
			await vi.advanceTimersByTimeAsync(2_000);
			expect(harness.streamRequests()).toBe(2);
			expect(harness.requests.map((request) => request.connects)).toEqual([
				['/a/connect?after=a-1', '/b/connect?after=b-1'],
				['/a/connect?after=a-2', '/b/connect?after=b-2']
			]);
		} finally {
			signal.abort();
			harness.runtime.dispose();
		}
	});
});
