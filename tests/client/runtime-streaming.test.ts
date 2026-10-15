import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime } from '../../src/client/core.ts';
import { readSseStream } from '../../src/client/runtime.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import { MAX_CHUNKED_SYNC_BYTES, encodeSseFrame, encodeSyncEnvelopeSseFrames } from '../../src/shared/sse.ts';
import { waitFor } from '../fixtures/async.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	vi.restoreAllMocks();
});

describe('browser runtime SSE transport', () => {
	it('reads ping plus fragmented SSE frames and cancels the stream on abort', async () => {
		let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
		let cancelled = 0;
		const reader = new ReadableStream<Uint8Array>({
			start(nextController) {
				controller = nextController;
			},
			cancel() {
				cancelled += 1;
			}
		}).getReader();
		const received: SyncEnvelope[] = [];
		const signal = new AbortController();
		const finished = readSseStream(
			reader,
			signal.signal,
			(envelope) => received.push(envelope),
			() => {}
		);

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

		signal.abort();
		await finished;
		expect(cancelled).toBe(1);
	});

	it('reassembles duplicate out-of-order sync chunks exactly once', async () => {
		let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
		const reader = new ReadableStream<Uint8Array>({
			start(nextController) {
				controller = nextController;
			}
		}).getReader();
		const received: SyncEnvelope[] = [];
		const signal = new AbortController();
		const finished = readSseStream(
			reader,
			signal.signal,
			(envelope) => received.push(envelope),
			() => {}
		);
		const envelope: SyncEnvelope = {
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			cursor: 'cursor-large',
			changes: [{ type: 'itemAdded', id: 'large', value: { text: 'x'.repeat(300_000) } }]
		};
		const frames = encodeSyncEnvelopeSseFrames(envelope, MAX_CHUNKED_SYNC_BYTES);
		expect(frames?.length).toBeGreaterThan(2);
		const reversed = [...(frames ?? [])].reverse();
		controller?.enqueue(reversed[0]!);
		controller?.enqueue(reversed[0]!);
		for (const frame of reversed.slice(1)) {
			controller?.enqueue(frame);
		}
		await waitFor(() => received.length === 1);
		expect(received).toEqual([envelope]);

		signal.abort();
		await finished;
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
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-shared`
		});
		const first: SyncEnvelope[] = [];
		const second: SyncEnvelope[] = [];
		const firstAbort = new AbortController();
		const secondAbort = new AbortController();
		try {
			const firstSubscription = await runtime.transport.subscribe({
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'client-scope',
				signal: firstAbort.signal,
				onEnvelope: (envelope) => first.push(envelope)
			});
			const registrationsAfterFirst = connectRequests;
			const secondSubscription = await runtime.transport.subscribe({
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
		const streamTransportIds: (string | null)[] = [];
		let streamRequests = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = toRequest(input, init);
			if (request.url.endsWith('/api/sync-resource/stream')) {
				streamRequests += 1;
				streamTransportIds.push(request.headers.get('x-sync-transport-id'));
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
		let idIndex = 0;
		const runtime = createRuntime({
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-${(idIndex += 1)}`
		});
		const abort = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
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
			expect(streamTransportIds[1]).toBe(streamTransportIds[0]);
			expect(new URL(connectRequests.at(-1)?.url ?? '').searchParams.get('after')).toBe('cursor-2');

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});
});

describe('browser runtime registration order', () => {
	function connectResponse(scope: string): Response {
		return Response.json({ isOk: true, isError: false, value: { scope } });
	}

	function sseResponse(onStart?: (controller: ReadableStreamDefaultController<Uint8Array>) => void): Response {
		return new Response(new ReadableStream<Uint8Array>({ start: (controller) => onStart?.(controller) }), {
			headers: { 'content-type': 'text/event-stream' }
		});
	}

	function subscribeTo(runtime: ReturnType<typeof createRuntime>, name: string, onReconnect?: () => void) {
		return runtime.transport.subscribe({
			connectUrl: `/${name}/connect`,
			managerKey: name,
			scope: `${name}-client-scope`,
			signal: new AbortController().signal,
			onEnvelope() {},
			onReconnect
		});
	}

	it('registers logical subscriptions only after the shared stream opens and does not re-send them', async () => {
		vi.useFakeTimers();
		const order: string[] = [];
		const streamOpened = new Deferred();
		let hydrations = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = toRequest(input, init);
			if (request.url.endsWith('/api/sync-resource/stream')) {
				order.push('stream');
				await streamOpened.promise;
				return sseResponse();
			}
			order.push(new URL(request.url).pathname);
			return connectResponse(`${new URL(request.url).pathname}:scope`);
		};
		const runtime = createRuntime({
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-gate`
		});
		try {
			const subscriptions = Promise.all([
				subscribeTo(runtime, 'notes', () => (hydrations += 1)),
				subscribeTo(runtime, 'tasks', () => (hydrations += 1))
			]);
			await waitFor(() => order.length > 0);
			await vi.advanceTimersByTimeAsync(1_000);
			expect(order).toEqual(['stream']);
			streamOpened.resolve();
			const results = await subscriptions;
			expect(results.every((result) => result.isOk())).toBe(true);
			expect(order).toEqual(['stream', '/notes/connect', '/tasks/connect']);
			expect(hydrations).toBe(0);
		} finally {
			runtime.dispose();
		}
	});

	it('still registers when the first stream attempt fails, then re-registers and hydrates once it opens', async () => {
		vi.useFakeTimers();
		vi.spyOn(Math, 'random').mockReturnValue(0.5);
		let streamRequests = 0;
		let connects = 0;
		let hydrations = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = toRequest(input, init);
			if (request.url.endsWith('/api/sync-resource/stream')) {
				streamRequests += 1;
				return streamRequests === 1 ? new Response('busy', { status: 429 }) : sseResponse();
			}
			connects += 1;
			return connectResponse('notes:scope');
		};
		const runtime = createRuntime({
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-failed`
		});
		try {
			const subscription = await subscribeTo(runtime, 'notes', () => (hydrations += 1));
			expect(subscription.isOk()).toBe(true);
			expect(connects).toBe(1);
			await vi.advanceTimersByTimeAsync(1_000);
			await waitFor(() => streamRequests === 2 && connects === 2);
			expect(hydrations).toBe(1);
		} finally {
			runtime.dispose();
		}
	});

	it('keeps refreshing the localStorage owner record while a tab without Web Locks owns the stream', async () => {
		const setItem = vi.fn();
		vi.stubGlobal('window', { localStorage: { getItem: () => null, setItem } });
		vi.stubGlobal('navigator', {});
		vi.useFakeTimers();
		const fetchImpl: typeof fetch = async (input, init) =>
			toRequest(input, init).url.endsWith('/api/sync-resource/stream')
				? sseResponse()
				: connectResponse('notes:scope');
		const runtime = createRuntime({
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId: (prefix) => `${prefix}-storage-owner`
		});
		try {
			expect((await subscribeTo(runtime, 'notes')).isOk()).toBe(true);
			const writes = () => setItem.mock.calls.filter(([key]) => key === 'sync-resource:owner').length;
			expect(writes()).toBe(1);
			await vi.advanceTimersByTimeAsync(3000);
			expect(writes()).toBe(2);
		} finally {
			runtime.dispose();
			vi.unstubAllGlobals();
		}
	});

	describe('transport id per stream owner', () => {
		const STORAGE_KEY = 'sync-resource:transport-id';

		afterEach(() => {
			vi.unstubAllGlobals();
		});

		function browserStorage(initialTransportId?: string) {
			const items: Record<string, string> = initialTransportId ? { [STORAGE_KEY]: initialTransportId } : {};
			return {
				items,
				window: {
					localStorage: {
						getItem: (key: string) => items[key] ?? null,
						setItem: (key: string, value: string) => {
							items[key] = value;
						}
					}
				}
			};
		}

		function recordingFetch(streamIds: string[], connectIds: string[]) {
			const fetchImpl: typeof fetch = async (input, init) => {
				const request = toRequest(input, init);
				const transportId = request.headers.get('x-sync-transport-id') ?? '';
				if (request.url.endsWith('/api/sync-resource/stream')) {
					streamIds.push(transportId);
					return sseResponse();
				}
				connectIds.push(transportId);
				return connectResponse('notes:scope');
			};
			return fetchImpl;
		}

		function idFactory() {
			let index = 0;
			return (prefix: string) => `${prefix}-${(index += 1)}`;
		}

		it('gives browsers that share stored state different stream ids', async () => {
			const browsers = [browserStorage('stored-shared'), browserStorage('stored-shared')];
			const streamIds: string[][] = [[], []];
			const runtimes: ReturnType<typeof createRuntime>[] = [];
			try {
				for (const [index, browser] of browsers.entries()) {
					vi.stubGlobal('window', browser.window);
					vi.stubGlobal('navigator', { locks: lockManager() });
					const runtime = createRuntime({
						batchConnect: false,
						cache: noopCache,
						fetch: recordingFetch(streamIds[index] ?? [], []),
						createId: (prefix) => `${prefix}-browser${index}`
					});
					runtimes.push(runtime);
					expect((await subscribeTo(runtime, 'notes')).isOk()).toBe(true);
				}
				expect(streamIds[0]).toEqual(['transport-browser0']);
				expect(streamIds[1]).toEqual(['transport-browser1']);
				expect(browsers.map((browser) => browser.items[STORAGE_KEY])).toEqual([
					'transport-browser0',
					'transport-browser1'
				]);
			} finally {
				for (const runtime of runtimes) {
					runtime.dispose();
				}
			}
		});

		it('mints a new id when a tab that lost stream ownership acquires it again', async () => {
			const browser = browserStorage();
			vi.stubGlobal('window', browser.window);
			vi.stubGlobal('navigator', { locks: lockManager() });
			const streamIds: string[] = [];
			const runtime = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: recordingFetch(streamIds, []),
				createId: idFactory()
			});
			try {
				const first = await subscribeTo(runtime, 'notes');
				if (first.isOk()) {
					first.value();
				}
				await waitFor(() => streamIds.length === 1);
				expect((await subscribeTo(runtime, 'notes')).isOk()).toBe(true);
				await waitFor(() => streamIds.length === 2);
				expect(new Set(streamIds).size).toBe(2);
				expect(browser.items[STORAGE_KEY]).toBe(streamIds[1]);
			} finally {
				runtime.dispose();
			}
		});

		it('registers followers with the current stream owner id and re-registers a successor that takes over without hydrating', async () => {
			const browser = browserStorage();
			vi.stubGlobal('window', browser.window);
			vi.stubGlobal('navigator', { locks: lockManager() });
			vi.stubGlobal('BroadcastChannel', LoopbackBroadcastChannel);
			const createId = idFactory();
			const ownerStreams: string[] = [];
			const successorStreams: string[] = [];
			const followerConnects: string[] = [];
			const successorConnects: string[] = [];
			let successorHydrations = 0;
			const owner = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: recordingFetch(ownerStreams, []),
				createId
			});
			const successor = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: recordingFetch(successorStreams, successorConnects),
				createId
			});
			const follower = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: recordingFetch([], followerConnects),
				createId
			});
			try {
				expect((await subscribeTo(owner, 'notes')).isOk()).toBe(true);
				expect((await subscribeTo(successor, 'notes', () => (successorHydrations += 1))).isOk()).toBe(true);
				expect((await subscribeTo(follower, 'notes')).isOk()).toBe(true);
				expect(followerConnects).toEqual([ownerStreams[0]]);

				owner.dispose();
				await waitFor(() => successorStreams.length === 1 && followerConnects.length === 2);
				expect(successorStreams[0]).not.toBe(ownerStreams[0]);
				expect(followerConnects[1]).toBe(successorStreams[0]);
				expect(successorConnects).toEqual([ownerStreams[0], successorStreams[0]]);
				expect(successorHydrations).toBe(0);
			} finally {
				owner.dispose();
				successor.dispose();
				follower.dispose();
			}
		});
	});

	describe('with Web Locks ownership', () => {
		afterEach(() => {
			vi.unstubAllGlobals();
		});

		function stubBrowser(options: { readonly granted: boolean }) {
			const request = vi.fn(
				(_name: string, ...rest: unknown[]) =>
					new Promise<void>((resolve) => {
						const callback = rest.at(-1) as (lock: object | null) => Promise<void>;
						const ifAvailable = typeof rest[0] === 'object';
						if (ifAvailable) {
							callback(options.granted ? {} : null).then(resolve);
						}
					})
			);
			vi.stubGlobal('window', { localStorage: undefined });
			vi.stubGlobal('navigator', { locks: { request } });
		}

		it('registers after the stream opens when the tab owns the Web Lock', async () => {
			vi.useFakeTimers();
			stubBrowser({ granted: true });
			const order: string[] = [];
			const streamOpened = new Deferred();
			const fetchImpl: typeof fetch = async (input, init) => {
				const request = toRequest(input, init);
				if (request.url.endsWith('/api/sync-resource/stream')) {
					order.push('stream');
					await streamOpened.promise;
					return sseResponse();
				}
				order.push('connect');
				return connectResponse('notes:scope');
			};
			const runtime = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: fetchImpl,
				createId: (prefix) => `${prefix}-owner`
			});
			try {
				const subscription = subscribeTo(runtime, 'notes');
				await waitFor(() => order.length > 0);
				await vi.advanceTimersByTimeAsync(1_000);
				expect(order).toEqual(['stream']);
				streamOpened.resolve();
				expect((await subscription).isOk()).toBe(true);
				expect(order).toEqual(['stream', 'connect']);
			} finally {
				runtime.dispose();
			}
		});

		it('does not write a localStorage owner record while the tab holds the Web Lock', async () => {
			stubBrowser({ granted: true });
			const setItem = vi.fn();
			vi.stubGlobal('window', { localStorage: { getItem: () => null, setItem } });
			vi.useFakeTimers();
			const fetchImpl: typeof fetch = async (input, init) =>
				toRequest(input, init).url.endsWith('/api/sync-resource/stream')
					? sseResponse()
					: connectResponse('notes:scope');
			const runtime = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: fetchImpl,
				createId: (prefix) => `${prefix}-lock-owner`
			});
			try {
				expect((await subscribeTo(runtime, 'notes')).isOk()).toBe(true);
				await vi.advanceTimersByTimeAsync(30_000);
				expect(setItem).not.toHaveBeenCalledWith('sync-resource:owner', expect.anything());
			} finally {
				runtime.dispose();
			}
		});

		it('does not wait for a stream the follower tab will never open', async () => {
			stubBrowser({ granted: false });
			let streamRequests = 0;
			let connects = 0;
			const fetchImpl: typeof fetch = async (input, init) => {
				const request = toRequest(input, init);
				if (request.url.endsWith('/api/sync-resource/stream')) {
					streamRequests += 1;
					return sseResponse();
				}
				connects += 1;
				return connectResponse('notes:scope');
			};
			const runtime = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: fetchImpl,
				createId: (prefix) => `${prefix}-follower`
			});
			try {
				const subscription = await subscribeTo(runtime, 'notes');
				expect(subscription.isOk()).toBe(true);
				expect(connects).toBe(1);
				expect(streamRequests).toBe(0);
			} finally {
				runtime.dispose();
			}
		});

		it('relays every envelope the owner tab receives, chunked or not, to a follower tab', async () => {
			vi.stubGlobal('window', { localStorage: undefined });
			vi.stubGlobal('navigator', { locks: lockManager() });
			vi.stubGlobal('BroadcastChannel', LoopbackBroadcastChannel);
			LoopbackBroadcastChannel.posted.length = 0;
			const streams: ReadableStreamDefaultController<Uint8Array>[] = [];
			const fetchImpl: typeof fetch = async (input, init) =>
				toRequest(input, init).url.endsWith('/api/sync-resource/stream')
					? sseResponse((controller) => streams.push(controller))
					: connectResponse('notes:scope');
			const owner = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: fetchImpl,
				createId: (p) => `${p}-owner`
			});
			const follower = createRuntime({
				batchConnect: false,
				cache: noopCache,
				fetch: fetchImpl,
				createId: (p) => `${p}-follower`
			});
			const ownerReceived: SyncEnvelope[] = [];
			const followerReceived: SyncEnvelope[] = [];
			const subscribe = (runtime: ReturnType<typeof createRuntime>, received: SyncEnvelope[]) =>
				runtime.transport.subscribe({
					connectUrl: '/notes/connect',
					managerKey: 'notes',
					scope: 'notes-client-scope',
					signal: new AbortController().signal,
					onEnvelope: (envelope) => received.push(envelope)
				});
			try {
				expect((await subscribe(owner, ownerReceived)).isOk()).toBe(true);
				expect((await subscribe(follower, followerReceived)).isOk()).toBe(true);
				expect(streams).toHaveLength(1);

				const sent = Array.from({ length: 4 }, (_, index): SyncEnvelope => ({
					managerKey: 'notes',
					scope: 'notes:scope',
					cursor: `cursor-${index}`,
					changes: [
						{
							type: 'itemAdded',
							id: `note-${index}`,
							value: { id: `note-${index}`, title: 'é😀'.repeat(index === 3 ? 50_000 : 1) }
						}
					]
				}));
				const chunked = encodeSyncEnvelopeSseFrames(sent[3] as SyncEnvelope, MAX_CHUNKED_SYNC_BYTES);
				expect(chunked?.length).toBeGreaterThan(1);
				for (const envelope of sent.slice(0, 3)) {
					streams[0]?.enqueue(encodeSseFrame('sync', envelope));
				}
				for (const frame of chunked ?? []) {
					streams[0]?.enqueue(frame);
				}
				await waitFor(() => followerReceived.length === sent.length);
				expect(ownerReceived).toEqual(sent);
				expect(followerReceived).toEqual(sent);
				const relayed = LoopbackBroadcastChannel.posted.filter(({ data }) => typeof data === 'string');
				expect(relayed.map(({ name, data }) => [name, data])).toEqual(
					sent.map((envelope) => ['sync-resource:v2:transport', JSON.stringify(envelope)])
				);
			} finally {
				owner.dispose();
				follower.dispose();
			}
		});
	});
});

// One lock universe per browser: the first requester owns it, later requesters queue until it is released.
function lockManager() {
	let held = false;
	const waiting: (() => void)[] = [];
	const run = (callback: (lock: object | null) => Promise<void>) => {
		held = true;
		return callback({}).finally(() => {
			held = false;
			waiting.shift()?.();
		});
	};
	return {
		request(_name: string, ...rest: unknown[]): Promise<void> {
			const callback = rest.at(-1) as (lock: object | null) => Promise<void>;
			if (!held) {
				return run(callback);
			}
			if (typeof rest[0] === 'object') {
				return callback(null);
			}
			return new Promise<void>((resolve) => waiting.push(() => resolve(run(callback))));
		}
	};
}

function toRequest(input: URL | RequestInfo, init?: RequestInit): Request {
	return new Request(input instanceof Request ? input : new URL(String(input), 'http://sync.test'), init);
}

class LoopbackBroadcastChannel {
	private static readonly open = new Set<LoopbackBroadcastChannel>();
	static readonly posted: { readonly name: string; readonly data: unknown }[] = [];
	onmessage: ((message: { readonly data: unknown }) => void) | null = null;

	private readonly name: string;

	constructor(name: string) {
		this.name = name;
		LoopbackBroadcastChannel.open.add(this);
	}

	postMessage(data: unknown): void {
		LoopbackBroadcastChannel.posted.push({ name: this.name, data });
		const copy = structuredClone(data);
		for (const peer of LoopbackBroadcastChannel.open) {
			if (peer !== this && peer.name === this.name) {
				queueMicrotask(() => peer.onmessage?.({ data: copy }));
			}
		}
	}

	close(): void {
		LoopbackBroadcastChannel.open.delete(this);
	}
}
