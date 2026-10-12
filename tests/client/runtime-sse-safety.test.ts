import { describe, expect, it } from 'vite-plus/test';

import { readSseStream } from '../../src/client/runtime.ts';
import type { SyncEnvelope } from '../../src/shared/protocol.ts';
import {
	encodeSseFrame,
	MAX_CHUNKED_SYNC_BYTES,
	MAX_SSE_FRAME_BYTES,
	type SyncEnvelopeChunk
} from '../../src/shared/sse.ts';
import { waitFor } from '../fixtures/async.ts';

const textEncoder = new TextEncoder();

describe('browser runtime SSE safety', () => {
	it('ignores malformed and out-of-range chunk metadata, then delivers a valid envelope', async () => {
		const stream = startStream();
		stream.enqueue(textEncoder.encode('event: sync-chunk\ndata: {\n\n'));
		stream.enqueue(
			encodeSseFrame('sync-chunk', {
				type: 'not-a-sync-chunk',
				id: 'wrong-type',
				index: 0,
				total: 1,
				totalBytes: 1,
				data: 'x'
			})
		);
		stream.enqueue(chunkFrame({ type: 'sync-chunk', id: 'empty', index: 0, total: 0, totalBytes: 1, data: 'x' }));
		stream.enqueue(
			chunkFrame({ type: 'sync-chunk', id: 'bad-index', index: 2, total: 2, totalBytes: 1, data: 'x' })
		);
		stream.enqueue(
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
		stream.enqueue(encodeSseFrame('sync', valid));
		await waitFor(() => stream.received.length === 1);
		expect(stream.received).toEqual([valid]);
		await stream.stop();
	});

	it('drops chunk groups with inconsistent metadata or byte totals and recovers for a valid group', async () => {
		const stream = startStream();
		const mismatched = splitEnvelope(envelope('cursor-mismatch', 'mismatch'), 'mismatched');
		stream.enqueue(chunkFrame(mismatched[0]));
		stream.enqueue(chunkFrame({ ...mismatched[1], total: 3 }));

		const wrongBytes = splitEnvelope(envelope('cursor-wrong-bytes', 'wrong-bytes'), 'wrong-bytes');
		stream.enqueue(chunkFrame({ ...wrongBytes[0], totalBytes: wrongBytes[0].totalBytes + 1 }));
		stream.enqueue(chunkFrame({ ...wrongBytes[1], totalBytes: wrongBytes[1].totalBytes + 1 }));

		const validEnvelope = envelope('cursor-valid-chunks', 'valid-chunks');
		const valid = splitEnvelope(validEnvelope, 'valid');
		stream.enqueue(chunkFrame(valid[1]));
		stream.enqueue(chunkFrame(valid[0]));

		await waitFor(() => stream.received.length === 1);
		expect(stream.received).toEqual([validEnvelope]);
		await stream.stop();
	});

	it('bounds incomplete chunk groups by pruning the oldest of more than 32 ids', async () => {
		const stream = startStream();
		const groups = Array.from({ length: 33 }, (_, index) => {
			const id = `group-${index}`;
			return splitEnvelope(envelope(`cursor-${index}`, id), id);
		});

		for (const group of groups) {
			stream.enqueue(chunkFrame(group[0]));
		}
		stream.enqueue(chunkFrame(groups[0]![1]));
		stream.enqueue(chunkFrame(groups[32]![1]));

		await waitFor(() => stream.received.length === 1);
		expect(stream.received).toEqual([envelope('cursor-32', 'group-32')]);
		await stream.stop();
	});

	it('stops reading an oversized complete frame and a new stream can recover', async () => {
		await expectBoundedStreamRecovery(`event: sync\ndata: ${'x'.repeat(MAX_SSE_FRAME_BYTES + 1)}\n\n`);
	});

	it('stops reading a complete frame that is within the bound by characters but above it by UTF-8 bytes', async () => {
		await expectBoundedStreamRecovery(
			`event: sync\ndata: ${'\u20ac'.repeat(Math.ceil(MAX_SSE_FRAME_BYTES / 3))}\n\n`
		);
	});

	it('delivers a complete frame above a third of the bound by characters but within it by bytes', async () => {
		const stream = startStream();
		const large = envelope('cursor-large', 'x'.repeat(MAX_SSE_FRAME_BYTES / 4));
		stream.enqueue(encodeSseFrame('sync', large));
		await waitFor(() => stream.received.length === 1);
		expect(stream.received).toEqual([large]);
		await stream.stop();
	});

	it('stops reading an unterminated buffer above the frame bound and a new stream can recover', async () => {
		await expectBoundedStreamRecovery('x'.repeat(MAX_SSE_FRAME_BYTES + 1));
	});

	it('counts unterminated SSE buffers by UTF-8 bytes', async () => {
		await expectBoundedStreamRecovery('😀'.repeat(Math.floor(MAX_SSE_FRAME_BYTES / 4) + 1));
	});

	it('keeps stateful UTF-8 decoder state local to each concurrent stream', async () => {
		const first = startStream();
		const second = startStream();
		const firstEnvelope = envelope('cursor-first', 'first-😀');
		const secondEnvelope = envelope('cursor-second', 'second-😀');
		const firstFrame = encodeSseFrame('sync', firstEnvelope);
		const secondFrame = encodeSseFrame('sync', secondEnvelope);
		const firstSplit = firstFrame.indexOf(0xf0) + 2;
		const secondSplit = secondFrame.indexOf(0xf0) + 2;

		first.enqueue(firstFrame.slice(0, firstSplit));
		second.enqueue(secondFrame.slice(0, secondSplit));
		first.enqueue(firstFrame.slice(firstSplit));
		second.enqueue(secondFrame.slice(secondSplit));
		await waitFor(() => first.received.length === 1 && second.received.length === 1);
		expect(first.received).toEqual([firstEnvelope]);
		expect(second.received).toEqual([secondEnvelope]);
		await first.stop();
		await second.stop();
	});
});

interface TestStream {
	readonly received: SyncEnvelope[];
	readonly terminalFailures: () => number;
	enqueue(chunk: Uint8Array): void;
	finished(): Promise<void>;
	stop(): Promise<void>;
}

function startStream(): TestStream {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	const reader = new ReadableStream<Uint8Array>({
		start(nextController) {
			controller = nextController;
		}
	}).getReader();
	const received: SyncEnvelope[] = [];
	const abort = new AbortController();
	let terminalFailures = 0;
	const finished = readSseStream(
		reader,
		abort.signal,
		(value) => received.push(value),
		() => {
			terminalFailures += 1;
		}
	);
	return {
		received,
		terminalFailures: () => terminalFailures,
		enqueue: (chunk) => controller.enqueue(chunk),
		finished: () => finished,
		async stop() {
			abort.abort();
			await finished;
		}
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

async function expectBoundedStreamRecovery(malformedInput: string): Promise<void> {
	const first = startStream();
	first.enqueue(textEncoder.encode(malformedInput));
	await first.finished();
	expect(first.terminalFailures()).toBe(1);
	first.enqueue(encodeSseFrame('sync', envelope('cursor-ignored', 'ignored')));
	await flushMicrotasks();
	expect(first.received).toEqual([]);

	const second = startStream();
	const valid = envelope('cursor-recovered', 'recovered');
	second.enqueue(encodeSseFrame('sync', valid));
	await waitFor(() => second.received.length === 1);
	expect(second.received).toEqual([valid]);
	expect(second.terminalFailures()).toBe(0);
	await second.stop();
}

async function flushMicrotasks(): Promise<void> {
	for (let index = 0; index < 10; index += 1) {
		await Promise.resolve();
	}
}
