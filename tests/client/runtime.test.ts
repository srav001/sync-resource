import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	configureSync,
	createRuntime,
	getSyncRuntime,
	ok,
	resetSyncConfiguration,
	type RuntimeTransport
} from '../../src/client/core.ts';
import { encodeSseFrame } from '../../src/shared/sse.ts';
import { waitFor } from '../fixtures/async.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';

afterEach(() => {
	resetSyncConfiguration();
	vi.restoreAllMocks();
});

describe('client runtime configuration', () => {
	it('requires configuration, exposes one runtime, and disposes it on reset', () => {
		expect(() => getSyncRuntime()).toThrow('Sync runtime is not configured');
		let disposed = 0;
		const transport: RuntimeTransport & { dispose(): void } = {
			async subscribe() {
				return ok(() => {});
			},
			dispose() {
				disposed += 1;
			}
		};
		configureSync({ cache: { adapter: noopCache }, transport });
		const first = getSyncRuntime();
		expect(getSyncRuntime()).toBe(first);
		expect(first.transport).toBe(transport);
		resetSyncConfiguration();
		expect(disposed).toBe(1);
		expect(() => getSyncRuntime()).toThrow('Sync runtime is not configured');
	});

	it('opens the default shared stream, registers a cursor, and dispatches matching envelopes', async () => {
		const requests: Request[] = [];
		let streamController!: ReadableStreamDefaultController<Uint8Array>;
		let id = 0;
		const fetchImpl: typeof fetch = async (input, init) => {
			const request = new Request(
				input instanceof Request ? input : new URL(String(input), 'http://sync.test'),
				init
			);
			requests.push(request.clone());
			if (request.url.endsWith('/api/sync-resource/stream')) {
				const stream = new ReadableStream<Uint8Array>({
					start(controller) {
						streamController = controller;
						request.signal.addEventListener(
							'abort',
							() => {
								controller.close();
							},
							{ once: true }
						);
					}
				});
				return new Response(stream, {
					headers: { 'content-type': 'text/event-stream' }
				});
			}
			return Response.json({
				isOk: true,
				isError: false,
				value: {
					scope: 'notes:workspace-1'
				}
			});
		};
		const runtime = createRuntime({
			batchConnect: false,
			cache: noopCache,
			fetch: fetchImpl,
			createId(prefix) {
				id += 1;
				return `${prefix}-${id}`;
			}
		});
		const envelopes: unknown[] = [];
		const abort = new AbortController();
		const subscription = await runtime.transport.subscribe({
			connectUrl: 'http://sync.test/notes/connect',
			managerKey: 'notes',
			scope: 'sync-resource:notes:{"workspaceId":"workspace-1"}',
			signal: abort.signal,
			getCursor: () => 'cursor-1',
			onEnvelope: (envelope) => envelopes.push(envelope)
		});
		expect(subscription.isOk()).toBe(true);

		const streamRequest = requests.find((request) => request.url.endsWith('/api/sync-resource/stream'));
		const connectRequest = requests.find((request) => request.url.includes('/notes/connect'));
		expect(streamRequest?.method).toBe('POST');
		expect(streamRequest?.headers.get('accept')).toBe('text/event-stream');
		expect(streamRequest?.headers.get('x-sync-transport-id')).toMatch(/^transport-/);
		expect(connectRequest?.method).toBe('POST');
		expect(connectRequest?.headers.get('x-sync-transport-id')).toBe(
			streamRequest?.headers.get('x-sync-transport-id')
		);
		expect(new URL(connectRequest?.url ?? '').searchParams.get('after')).toBe('cursor-1');

		const envelope = {
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			cursor: 'cursor-2',
			changes: [{ type: 'itemAdded' as const, id: 'note-1', value: { id: 'note-1', title: 'One' } }]
		};
		streamController.enqueue(encodeSseFrame('sync', envelope));
		await waitFor(() => envelopes.length === 1);
		expect(envelopes).toEqual([envelope]);

		if (subscription.isOk()) {
			subscription.value();
		}
		runtime.dispose();
	});
	it('generates transport and subscription ids with independent cryptographic randomness from clientId', async () => {
		let calls = 0;
		const getRandomValues = vi.spyOn(crypto, 'getRandomValues').mockImplementation((array) => {
			(array as Uint8Array).fill(++calls * 0x11);
			return array;
		});
		const requests: Request[] = [];
		const runtime = createRuntime({
			cache: noopCache,
			batchConnect: false,
			fetch: async (input, init) => {
				const request = new Request(
					input instanceof Request ? input : new URL(input, 'http://sync.test'),
					init
				);
				requests.push(request);
				return request.url.endsWith('/stream')
					? new Response(new ReadableStream())
					: Response.json({ isOk: true, isError: false, value: { scope: 'notes:scope' } });
			}
		});
		try {
			const result = await runtime.transport.subscribe({
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'scope',
				signal: new AbortController().signal,
				onEnvelope() {}
			});
			expect(result.isOk()).toBe(true);
			expect(runtime.clientId).toBe(`client_${'11'.repeat(16)}`);
			expect(requests[0]!.headers.get('x-sync-transport-id')).toBe(`transport_${'33'.repeat(16)}`);
			expect(new URL(requests[1]!.url).searchParams.get('subscription')).toBe(`subscription_${'44'.repeat(16)}`);
			expect(getRandomValues).toHaveBeenCalledTimes(4);
		} finally {
			runtime.dispose();
			getRandomValues.mockRestore();
		}
	});
});
