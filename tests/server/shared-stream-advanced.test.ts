import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { MAX_CHUNKED_SYNC_BYTES, parseSseEvent, parseSyncEnvelopeChunkJson } from '../../src/shared/sse.ts';
import { outboxReading, registerStreamSubscription, streamRequest } from '../fixtures/sharedStream.ts';

afterEach(() => {
	vi.useRealTimers();
	resetSharedStreamForTests();
});

describe('shared stream replay and framing', () => {
	it('chunks a committed envelope above the physical frame limit and preserves its payload', async () => {
		registerSharedStreamManager({
			key: 'notes',
			maxEventBytes: MAX_CHUNKED_SYNC_BYTES
		});
		await registerStreamSubscription('transport-chunks', outboxReading());
		const response = await httpSharedSyncStream(streamRequest('transport-chunks'));
		const reader = response.body!.getReader();
		await reader.read();

		const envelope: SyncEnvelope = {
			managerKey: 'notes',
			scope: 'notes:ws',
			cursor: 'cursor-large',
			changes: [{ type: 'itemAdded', id: 'large', value: { text: 'x'.repeat(300_000) } }]
		};
		publishSharedStreamEnvelope(envelope);

		const chunks = new Map<number, string>();
		let total = 0;
		let totalBytes = 0;
		while (chunks.size === 0 || chunks.size < total) {
			const next = await reader.read();
			expect(next.done).toBe(false);
			const event = parseSseEvent(new TextDecoder().decode(next.value));
			expect(event.eventName).toBe('sync-chunk');
			const chunk = parseSyncEnvelopeChunkJson(event.data);
			expect(chunk).toBeDefined();
			if (chunk) {
				total = chunk.total;
				totalBytes = chunk.totalBytes;
				chunks.set(chunk.index, chunk.data);
			}
		}
		const serialized = [...chunks.entries()]
			.sort(([left], [right]) => left - right)
			.map(([, data]) => data)
			.join('');
		expect(new TextEncoder().encode(serialized)).toHaveLength(totalBytes);
		expect(JSON.parse(serialized)).toEqual(envelope);
		await reader.cancel();
	});

	it('reports replay failure, emits a repair reset, and closes the stream', async () => {
		const errors: string[] = [];
		registerSharedStreamManager({
			key: 'notes',
			handleError(error, context) {
				errors.push(`${context.method}:${error.message}`);
			}
		});
		await registerStreamSubscription(
			'transport-failure',
			outboxReading(async () => {
				throw new Error('outbox unavailable');
			}),
			'cursor-before'
		);
		const response = await httpSharedSyncStream(streamRequest('transport-failure'));
		const reader = response.body!.getReader();
		await reader.read();
		const resetFrame = await reader.read();
		const resetEvent = parseSseEvent(new TextDecoder().decode(resetFrame.value));
		expect(resetEvent.eventName).toBe('sync');
		expect(JSON.parse(resetEvent.data)).toMatchObject({
			cursor: 'reset',
			reset: {
				reason: 'replay_failed',
				previousCursor: 'cursor-before',
				nextCursor: 'reset'
			}
		});
		expect(errors).toEqual(['stream.replay:outbox unavailable']);
		expect((await reader.read()).done).toBe(true);
	});

	it('emits heartbeats at the smallest registered manager interval', async () => {
		vi.useFakeTimers();
		registerSharedStreamManager({ key: 'notes', heartbeatMs: 25 });
		const response = await httpSharedSyncStream(streamRequest('transport-heartbeat'));
		const reader = response.body!.getReader();
		await reader.read();
		const pendingPing = reader.read();
		await vi.advanceTimersByTimeAsync(25);
		const ping = parseSseEvent(new TextDecoder().decode((await pendingPing).value));
		expect(ping.eventName).toBe('ping');
		expect(JSON.parse(ping.data)).toMatchObject({
			type: 'ping',
			transportId: 'transport-heartbeat'
		});
		await reader.cancel();
	});
});
