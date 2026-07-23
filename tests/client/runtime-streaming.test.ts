import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime, type CacheAdapter } from '../../src/client/core.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import { MAX_CHUNKED_SYNC_BYTES, encodeSseFrame, encodeSyncEnvelopeSseFrames } from '../../src/shared/sse.ts';

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

afterEach(() => {
	vi.useRealTimers();
	vi.restoreAllMocks();
});

describe('browser runtime SSE transport', () => {
	it('adds a replay cursor and reads ping plus fragmented direct SSE frames', async () => {
		let request: Request | undefined;
		let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
		let cancelled = 0;
		const runtime = createRuntime({
			cache,
			fetch: async (input, init) => {
				request = toRequest(input, init);
				return new Response(
					new ReadableStream<Uint8Array>({
						start(nextController) {
							controller = nextController;
						},
						cancel() {
							cancelled += 1;
						}
					}),
					{ headers: { 'content-type': 'text/event-stream' } }
				);
			},
			createId: (prefix) => `${prefix}-direct`
		});
		const received: SyncEnvelope[] = [];
		const signal = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: '/events?existing=1',
				signal: signal.signal,
				getCursor: () => 'cursor one',
				onEnvelope: (envelope) => received.push(envelope)
			});
			expect(subscription.isOk()).toBe(true);
			expect(new URL(request?.url ?? '').searchParams.get('after')).toBe('cursor one');

			controller?.enqueue(encodeSseFrame('ping', { now: 1 }));
			const envelope: SyncEnvelope = {
				managerKey: 'notes',
				scope: 'notes:workspace-1',
				cursor: 'cursor-2',
				changes: [{ type: 'itemAdded', id: 'note-1', value: { id: 'note-1' } }]
			};
			const frame = encodeSseFrame('sync', envelope);
			controller?.enqueue(frame.slice(0, 11));
			controller?.enqueue(frame.slice(11));
			await waitFor(() => received.length === 1);
			expect(received).toEqual([envelope]);

			if (subscription.isOk()) {
				subscription.value();
			}
			await waitFor(() => cancelled === 1);
		} finally {
			runtime.dispose();
		}
	});

	it('reassembles duplicate out-of-order sync chunks exactly once', async () => {
		let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
		const runtime = createRuntime({
			cache,
			fetch: async () =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(nextController) {
							controller = nextController;
						}
					}),
					{ headers: { 'content-type': 'text/event-stream' } }
				),
			createId: (prefix) => `${prefix}-chunks`
		});
		const received: SyncEnvelope[] = [];
		const signal = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: 'http://sync.test/events',
				signal: signal.signal,
				onEnvelope: (envelope) => received.push(envelope)
			});
			expect(subscription.isOk()).toBe(true);
			const envelope: SyncEnvelope = {
				managerKey: 'notes',
				scope: 'notes:workspace-1',
				cursor: 'cursor-large',
				changes: [{ type: 'itemAdded', id: 'large', value: { text: 'x'.repeat(20_000) } }]
			};
			const frames = encodeSyncEnvelopeSseFrames(envelope, {
				maxEnvelopeBytes: MAX_CHUNKED_SYNC_BYTES,
				maxFrameBytes: 2_048
			});
			expect(frames?.length).toBeGreaterThan(2);
			const reversed = [...(frames ?? [])].reverse();
			controller?.enqueue(reversed[0]!);
			controller?.enqueue(reversed[0]!);
			for (const frame of reversed.slice(1)) {
				controller?.enqueue(frame);
			}
			await waitFor(() => received.length === 1);
			expect(received).toEqual([envelope]);

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});

	it('shares one physical stream and logical registration between duplicate stores', async () => {
		let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
		let streamRequests = 0;
		let connectRequests = 0;
		let streamCancelled = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = toRequest(input, init);
			if (request.url.endsWith('/api/sync-resource/stream')) {
				streamRequests += 1;
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							streamController = controller;
						},
						cancel() {
							streamCancelled += 1;
						}
					}),
					{ headers: { 'content-type': 'text/event-stream' } }
				);
			}
			connectRequests += 1;
			return Response.json({
				isOk: true,
				isError: false,
				value: { scope: 'notes:workspace-1' }
			});
		};
		const runtime = createRuntime({
			cache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-shared`
		});
		const first: SyncEnvelope[] = [];
		const second: SyncEnvelope[] = [];
		const firstAbort = new AbortController();
		const secondAbort = new AbortController();
		try {
			const firstSubscription = await runtime.transport.subscribe({
				url: '/notes/events',
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'client-scope',
				signal: firstAbort.signal,
				onEnvelope: (envelope) => first.push(envelope)
			});
			const registrationsAfterFirst = connectRequests;
			const secondSubscription = await runtime.transport.subscribe({
				url: '/notes/events',
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'client-scope',
				signal: secondAbort.signal,
				onEnvelope: (envelope) => second.push(envelope)
			});
			expect(firstSubscription.isOk()).toBe(true);
			expect(secondSubscription.isOk()).toBe(true);
			expect(streamRequests).toBe(1);
			expect(connectRequests).toBe(registrationsAfterFirst);

			const envelope: SyncEnvelope = {
				managerKey: 'notes',
				scope: 'notes:workspace-1',
				cursor: 'cursor-1',
				changes: []
			};
			streamController?.enqueue(encodeSseFrame('sync', envelope));
			await waitFor(() => first.length === 1 && second.length === 1);

			if (firstSubscription.isOk()) {
				firstSubscription.value();
			}
			expect(streamCancelled).toBe(0);
			if (secondSubscription.isOk()) {
				secondSubscription.value();
			}
			await waitFor(() => streamCancelled === 1);
		} finally {
			runtime.dispose();
		}
	});

	it('reopens the shared stream, re-registers the latest cursor, and requests catch-up hydration', async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, 'random').mockReturnValue(0.5);
		const streamControllers: ReadableStreamDefaultController<Uint8Array>[] = [];
		const connectRequests: Request[] = [];
		let streamRequests = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = toRequest(input, init);
			if (request.url.endsWith('/api/sync-resource/stream')) {
				streamRequests += 1;
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							streamControllers.push(controller);
						}
					}),
					{ headers: { 'content-type': 'text/event-stream' } }
				);
			}
			connectRequests.push(request.clone());
			return Response.json({
				isOk: true,
				isError: false,
				value: { scope: 'notes:workspace-1' }
			});
		};
		let cursor = 'cursor-1';
		let catchUpHydrations = 0;
		const runtime = createRuntime({
			cache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-reconnect`
		});
		const abort = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: '/notes/events',
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'client-scope',
				signal: abort.signal,
				getCursor: () => cursor,
				onEnvelope() {},
				onReconnect() {
					catchUpHydrations += 1;
				}
			});
			expect(subscription.isOk()).toBe(true);
			const initialRegistrations = connectRequests.length;
			cursor = 'cursor-2';
			streamControllers[0]?.close();
			await vi.advanceTimersByTimeAsync(1_000);
			await waitFor(() => streamRequests === 2 && connectRequests.length > initialRegistrations);
			expect(catchUpHydrations).toBe(1);
			expect(new URL(connectRequests.at(-1)?.url ?? '').searchParams.get('after')).toBe('cursor-2');

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});
});

function toRequest(input: URL | RequestInfo, init?: RequestInit): Request {
	return new Request(input instanceof Request ? input : new URL(String(input), 'http://sync.test'), init);
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
