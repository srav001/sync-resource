import { describe, expect, it } from 'vite-plus/test';

import { createRuntime, type CacheAdapter } from '../../src/client/core.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import {
	encodeSseFrame,
	MAX_CHUNKED_SYNC_BYTES,
	MAX_SSE_FRAME_BYTES,
	type SyncEnvelopeChunk
} from '../../src/shared/sse.ts';

const textEncoder = new TextEncoder();

const cache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

describe('browser runtime SSE safety', () => {
	it('rejects a pre-aborted direct subscription without starting fetch', async () => {
		const harness = createDirectStreamHarness();
		const runtime = createRuntime({ cache, fetch: harness.fetch, createId: (prefix) => `${prefix}-pre-aborted` });
		const abort = new AbortController();
		abort.abort();
		try {
			const result = await runtime.transport.subscribe({ url: '/events', signal: abort.signal, onEnvelope() {} });
			expect(result.isErr() && result.error.code).toBe('aborted');
			expect(harness.controllers).toHaveLength(0);
		} finally {
			runtime.dispose();
		}
	});

	it('ignores malformed and out-of-range chunk metadata, then delivers a valid envelope', async () => {
		const harness = createDirectStreamHarness();
		const runtime = createRuntime({
			cache,
			fetch: harness.fetch,
			createId: (prefix) => `${prefix}-invalid-chunks`
		});
		const received: SyncEnvelope[] = [];
		const abort = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: '/events',
				signal: abort.signal,
				onEnvelope: (envelope) => received.push(envelope)
			});
			expect(subscription.isOk()).toBe(true);
			const controller = harness.controllers[0];
			expect(controller).toBeDefined();

			controller?.enqueue(textEncoder.encode('event: sync-chunk\ndata: {\n\n'));
			controller?.enqueue(
				encodeSseFrame('sync-chunk', {
					type: 'not-a-sync-chunk',
					id: 'wrong-type',
					index: 0,
					total: 1,
					totalBytes: 1,
					data: 'x'
				})
			);
			controller?.enqueue(
				chunkFrame({
					type: 'sync-chunk',
					id: 'empty',
					index: 0,
					total: 0,
					totalBytes: 1,
					data: 'x'
				})
			);
			controller?.enqueue(
				chunkFrame({
					type: 'sync-chunk',
					id: 'bad-index',
					index: 2,
					total: 2,
					totalBytes: 1,
					data: 'x'
				})
			);
			controller?.enqueue(
				chunkFrame({
					type: 'sync-chunk',
					id: 'too-large',
					index: 0,
					total: 1,
					totalBytes: MAX_CHUNKED_SYNC_BYTES + 1,
					data: 'x'
				})
			);

			const valid = envelope('cursor-valid', 'valid');
			controller?.enqueue(encodeSseFrame('sync', valid));
			await waitFor(() => received.length === 1);
			expect(received).toEqual([valid]);

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});

	it('drops chunk groups with inconsistent metadata or byte totals and recovers for a valid group', async () => {
		const harness = createDirectStreamHarness();
		const runtime = createRuntime({
			cache,
			fetch: harness.fetch,
			createId: (prefix) => `${prefix}-chunk-integrity`
		});
		const received: SyncEnvelope[] = [];
		const abort = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: '/events',
				signal: abort.signal,
				onEnvelope: (envelopeValue) => received.push(envelopeValue)
			});
			expect(subscription.isOk()).toBe(true);
			const controller = harness.controllers[0];

			const mismatched = splitEnvelope(envelope('cursor-mismatch', 'mismatch'), 'mismatched');
			controller?.enqueue(chunkFrame(mismatched[0]));
			controller?.enqueue(chunkFrame({ ...mismatched[1], total: 3 }));

			const wrongBytes = splitEnvelope(envelope('cursor-wrong-bytes', 'wrong-bytes'), 'wrong-bytes');
			controller?.enqueue(chunkFrame({ ...wrongBytes[0], totalBytes: wrongBytes[0].totalBytes + 1 }));
			controller?.enqueue(chunkFrame({ ...wrongBytes[1], totalBytes: wrongBytes[1].totalBytes + 1 }));

			const validEnvelope = envelope('cursor-valid-chunks', 'valid-chunks');
			const valid = splitEnvelope(validEnvelope, 'valid');
			controller?.enqueue(chunkFrame(valid[1]));
			controller?.enqueue(chunkFrame(valid[0]));

			await waitFor(() => received.length === 1);
			expect(received).toEqual([validEnvelope]);

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});

	it('bounds incomplete chunk groups by pruning the oldest of more than 32 ids', async () => {
		const harness = createDirectStreamHarness();
		const runtime = createRuntime({
			cache,
			fetch: harness.fetch,
			createId: (prefix) => `${prefix}-chunk-pruning`
		});
		const received: SyncEnvelope[] = [];
		const abort = new AbortController();
		try {
			const subscription = await runtime.transport.subscribe({
				url: '/events',
				signal: abort.signal,
				onEnvelope: (envelopeValue) => received.push(envelopeValue)
			});
			expect(subscription.isOk()).toBe(true);
			const controller = harness.controllers[0];
			const groups = Array.from({ length: 33 }, (_, index) => {
				const id = `group-${index}`;
				return splitEnvelope(envelope(`cursor-${index}`, id), id);
			});

			for (const group of groups) {
				controller?.enqueue(chunkFrame(group[0]));
			}
			controller?.enqueue(chunkFrame(groups[0]![1]));
			controller?.enqueue(chunkFrame(groups[32]![1]));

			await waitFor(() => received.length === 1);
			expect(received).toEqual([envelope('cursor-32', 'group-32')]);

			if (subscription.isOk()) {
				subscription.value();
			}
		} finally {
			runtime.dispose();
		}
	});

	it('stops reading an oversized complete frame and a new subscription can recover', async () => {
		await expectBoundedStreamRecovery(
			`event: sync\ndata: ${'x'.repeat(MAX_SSE_FRAME_BYTES + 1)}\n\n`,
			'oversized-frame'
		);
	});

	it('stops reading an unterminated buffer above the frame bound and a new subscription can recover', async () => {
		await expectBoundedStreamRecovery('x'.repeat(MAX_SSE_FRAME_BYTES + 1), 'stalled-buffer');
	});

	it('counts unterminated SSE buffers by UTF-8 bytes', async () => {
		await expectBoundedStreamRecovery(
			'😀'.repeat(Math.floor(MAX_SSE_FRAME_BYTES / 4) + 1),
			'unicode-stalled-buffer'
		);
	});

	it('keeps stateful UTF-8 decoder state local to each concurrent stream', async () => {
		const harness = createDirectStreamHarness();
		const runtime = createRuntime({ cache, fetch: harness.fetch, createId: (prefix) => `${prefix}-local-decoder` });
		const firstReceived: SyncEnvelope[] = [];
		const secondReceived: SyncEnvelope[] = [];
		const firstAbort = new AbortController();
		const secondAbort = new AbortController();
		try {
			const first = await runtime.transport.subscribe({
				url: '/first',
				signal: firstAbort.signal,
				onEnvelope: (value) => firstReceived.push(value)
			});
			const second = await runtime.transport.subscribe({
				url: '/second',
				signal: secondAbort.signal,
				onEnvelope: (value) => secondReceived.push(value)
			});
			const firstEnvelope = envelope('cursor-first', 'first-😀');
			const secondEnvelope = envelope('cursor-second', 'second-😀');
			const firstFrame = encodeSseFrame('sync', firstEnvelope);
			const secondFrame = encodeSseFrame('sync', secondEnvelope);
			const firstSplit = firstFrame.indexOf(0xf0) + 2;
			const secondSplit = secondFrame.indexOf(0xf0) + 2;

			harness.controllers[0]?.enqueue(firstFrame.slice(0, firstSplit));
			harness.controllers[1]?.enqueue(secondFrame.slice(0, secondSplit));
			harness.controllers[0]?.enqueue(firstFrame.slice(firstSplit));
			harness.controllers[1]?.enqueue(secondFrame.slice(secondSplit));
			await waitFor(() => firstReceived.length === 1 && secondReceived.length === 1);
			expect(firstReceived).toEqual([firstEnvelope]);
			expect(secondReceived).toEqual([secondEnvelope]);

			if (first.isOk()) {
				first.value();
			}
			if (second.isOk()) {
				second.value();
			}
		} finally {
			runtime.dispose();
		}
	});
});

interface DirectStreamHarness {
	readonly controllers: ReadableStreamDefaultController<Uint8Array>[];
	readonly fetch: typeof fetch;
}

function createDirectStreamHarness(): DirectStreamHarness {
	const controllers: ReadableStreamDefaultController<Uint8Array>[] = [];
	return {
		controllers,
		fetch: async () =>
			new Response(
				new ReadableStream<Uint8Array>({
					start(controller) {
						controllers.push(controller);
					}
				}),
				{ headers: { 'content-type': 'text/event-stream' } }
			)
	};
}

function chunkFrame(chunk: SyncEnvelopeChunk): Uint8Array {
	return encodeSseFrame('sync-chunk', chunk);
}

function splitEnvelope(envelopeValue: SyncEnvelope, id: string): readonly [SyncEnvelopeChunk, SyncEnvelopeChunk] {
	const serialized = JSON.stringify(envelopeValue);
	const splitAt = Math.floor(serialized.length / 2);
	const chunks = [serialized.slice(0, splitAt), serialized.slice(splitAt)] as const;
	const totalBytes = textEncoder.encode(serialized).byteLength;
	return [
		{
			type: 'sync-chunk',
			id,
			index: 0,
			total: 2,
			totalBytes,
			data: chunks[0]
		},
		{
			type: 'sync-chunk',
			id,
			index: 1,
			total: 2,
			totalBytes,
			data: chunks[1]
		}
	];
}

function envelope(cursor: string, id: string): SyncEnvelope {
	return {
		managerKey: 'notes',
		scope: 'notes:workspace-1',
		cursor,
		changes: [{ type: 'itemAdded', id, value: { id } }]
	};
}

async function expectBoundedStreamRecovery(malformedInput: string, suffix: string): Promise<void> {
	const harness = createDirectStreamHarness();
	const runtime = createRuntime({
		cache,
		fetch: harness.fetch,
		createId: (prefix) => `${prefix}-${suffix}`
	});
	const received: SyncEnvelope[] = [];
	const firstAbort = new AbortController();
	const secondAbort = new AbortController();
	try {
		const first = await runtime.transport.subscribe({
			url: '/events',
			signal: firstAbort.signal,
			onEnvelope: (envelopeValue) => received.push(envelopeValue)
		});
		expect(first.isOk()).toBe(true);
		harness.controllers[0]?.enqueue(textEncoder.encode(malformedInput));
		await flushMicrotasks();
		harness.controllers[0]?.enqueue(encodeSseFrame('sync', envelope('cursor-ignored', 'ignored')));
		await flushMicrotasks();
		expect(received).toEqual([]);

		const second = await runtime.transport.subscribe({
			url: '/events',
			signal: secondAbort.signal,
			onEnvelope: (envelopeValue) => received.push(envelopeValue)
		});
		expect(second.isOk()).toBe(true);
		const valid = envelope('cursor-recovered', 'recovered');
		harness.controllers[1]?.enqueue(encodeSseFrame('sync', valid));
		await waitFor(() => received.length === 1);
		expect(received).toEqual([valid]);

		if (first.isOk()) {
			first.value();
		}
		if (second.isOk()) {
			second.value();
		}
	} finally {
		runtime.dispose();
	}
}

async function flushMicrotasks(): Promise<void> {
	for (let index = 0; index < 10; index += 1) {
		await Promise.resolve();
	}
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
