import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	resetSharedStreamForTests,
	type ManagerOutboxRead,
	type ManagerSyncPersistence,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { MAX_CHUNKED_SYNC_BYTES, parseSseEvent, parseSyncEnvelopeChunkJson } from '../../src/shared/sse.ts';

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
		const persistence = createPersistence();
		await registerSharedStreamSubscription({
			request: connectRequest('transport-chunks'),
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			replayLimit: 100,
			persistence,
			nextCursor: () => 'reset-cursor'
		});
		const response = await httpSharedSyncStream(streamRequest('transport-chunks'));
		const reader = response.body!.getReader();
		await reader.read();

		const envelope: SyncEnvelope = {
			managerKey: 'notes',
			scope: 'notes:workspace-1',
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
			handleError(error) {
				errors.push(error.message);
			}
		});
		const persistence = createPersistence(async () => {
			throw new Error('outbox unavailable');
		});
		await registerSharedStreamSubscription({
			request: connectRequest('transport-failure'),
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			afterCursor: 'cursor-before',
			replayLimit: 100,
			persistence,
			nextCursor: () => 'cursor-reset'
		});
		const response = await httpSharedSyncStream(streamRequest('transport-failure'));
		const reader = response.body!.getReader();
		await reader.read();
		const resetFrame = await reader.read();
		const resetEvent = parseSseEvent(new TextDecoder().decode(resetFrame.value));
		expect(resetEvent.eventName).toBe('sync');
		expect(JSON.parse(resetEvent.data)).toMatchObject({
			cursor: 'cursor-reset',
			reset: {
				reason: 'replay_failed',
				previousCursor: 'cursor-before',
				nextCursor: 'cursor-reset'
			}
		});
		expect(errors).toEqual(['outbox unavailable']);
		expect((await reader.read()).done).toBe(true);
	});

	it('delivers replay before live envelopes committed while replay is in flight', async () => {
		let resolveReplay!: (value: ManagerOutboxRead) => void;
		const replayStarted = new Promise<void>((resolve) => {
			const persistence = createPersistence(
				() =>
					new Promise<ManagerOutboxRead>((resolveRead) => {
						resolveReplay = resolveRead;
						resolve();
					})
			);
			registerSharedStreamManager({ key: 'notes' });
			registerSharedStreamSubscription({
				request: connectRequest('transport-order'),
				managerKey: 'notes',
				scope: 'notes:workspace-1',
				afterCursor: 'cursor-before',
				replayLimit: 100,
				persistence,
				nextCursor: () => 'cursor-reset'
			});
		});
		const response = await httpSharedSyncStream(streamRequest('transport-order'));
		const reader = response.body!.getReader();
		await reader.read();
		await replayStarted;

		const replayed = envelope('cursor-replayed', 'replayed');
		const live = envelope('cursor-live', 'live');
		publishSharedStreamEnvelope(live);
		resolveReplay({
			cursorFound: true,
			retainedEnvelopeCount: 1,
			envelopes: [replayed]
		});

		const first = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
		const second = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
		expect([JSON.parse(first.data).cursor, JSON.parse(second.data).cursor]).toEqual([
			'cursor-replayed',
			'cursor-live'
		]);
		await reader.cancel();
	});

	it('characterizes live delivery racing deferred replay for an already-open stream', async () => {
		vi.useFakeTimers();
		let replayReads = 0;
		const replayed = envelope('cursor-replayed', 'replayed');
		const live = envelope('cursor-live', 'live');
		const persistence = createPersistence(() => {
			replayReads += 1;
			return {
				cursorFound: true,
				retainedEnvelopeCount: 1,
				envelopes: [replayed]
			};
		});
		registerSharedStreamManager({ key: 'notes' });
		const response = await httpSharedSyncStream(streamRequest('transport-deferred-order'));
		const reader = response.body!.getReader();
		await reader.read();

		const registered = await registerSharedStreamSubscription({
			request: connectRequest('transport-deferred-order'),
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			afterCursor: 'cursor-before',
			replayLimit: 100,
			persistence,
			nextCursor: () => 'cursor-reset'
		});
		expect(registered.isOk()).toBe(true);
		expect(replayReads).toBe(0);

		publishSharedStreamEnvelope(live);
		await vi.advanceTimersByTimeAsync(0);
		expect(replayReads).toBe(1);

		const first = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
		const second = parseSseEvent(new TextDecoder().decode((await reader.read()).value));
		// DIVERGENCE: live delivery can overtake deferred cursor replay after logical registration on an open stream.
		expect([JSON.parse(first.data).cursor, JSON.parse(second.data).cursor]).toEqual([
			'cursor-live',
			'cursor-replayed'
		]);
		await reader.cancel();
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

function createPersistence(
	readAfter: (
		scope: string,
		cursor: string,
		limit: number
	) => ManagerOutboxRead | Promise<ManagerOutboxRead> = () => ({
		cursorFound: true,
		retainedEnvelopeCount: 0,
		envelopes: []
	})
): ManagerSyncPersistence {
	return {
		async append() {},
		readAfter,
		async readMutation() {
			return undefined;
		},
		async recordMutation() {}
	};
}

function connectRequest(transportId: string): Request {
	return new Request('http://sync.test/connect', {
		method: 'POST',
		headers: { 'x-sync-transport-id': transportId }
	});
}

function streamRequest(transportId: string): Request {
	return new Request('http://sync.test/stream', {
		method: 'POST',
		headers: {
			'x-sync-transport-id': transportId,
			'x-forwarded-for': '203.0.113.1'
		}
	});
}

function envelope(cursor: string, id: string): SyncEnvelope {
	return {
		managerKey: 'notes',
		scope: 'notes:workspace-1',
		cursor,
		changes: [{ type: 'itemAdded', id, value: { id } }]
	};
}
