import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	type ManagerOutboxRead
} from '../../src/server/index.ts';
import { resetSharedStreamForTests, sharedStreamSentCursorCount } from '../../src/server/streamMultiplexer.ts';
import { nextMacrotask } from '../fixtures/async.ts';
import {
	openStream,
	outboxReading,
	readCursorsUntil,
	registerStreamSubscription,
	streamEnvelope
} from '../fixtures/sharedStream.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	resetSharedStreamForTests();
});

const caughtUp = (retainedEnvelopeCount: number, ...cursors: string[]): ManagerOutboxRead => ({
	cursorFound: true,
	retainedEnvelopeCount,
	envelopes: cursors.map((cursor) => streamEnvelope(cursor))
});

describe('shared stream replay ordering', () => {
	it('starts replay after the registration returns and keeps live envelopes behind it, dropping a replayed live copy', async () => {
		vi.useFakeTimers();
		registerSharedStreamManager({ key: 'notes' });
		const reads: string[] = [];
		const reader = await openStream('t1');
		await registerStreamSubscription(
			't1',
			outboxReading((_scope, cursor) => {
				reads.push(cursor);
				return caughtUp(2, 'c1', 'c2');
			}),
			'c0'
		);
		expect(reads).toEqual([]);

		publishSharedStreamEnvelope(streamEnvelope('c2'));
		publishSharedStreamEnvelope(streamEnvelope('c3'));
		await vi.advanceTimersByTimeAsync(0);
		expect(reads).toEqual(['c0']);
		expect(await readCursorsUntil(reader, 'c3')).toEqual(['c1', 'c2', 'c3']);
	});

	it('delivers replay before live envelopes committed while the replay read is in flight', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const readStarted = new Deferred();
		const replay = new Deferred<ManagerOutboxRead>();
		const reader = await openStream('t2');
		await registerStreamSubscription(
			't2',
			outboxReading(() => {
				readStarted.resolve();
				return replay.promise;
			}),
			'c0'
		);
		await readStarted.promise;

		publishSharedStreamEnvelope(streamEnvelope('live'));
		replay.resolve(caughtUp(1, 'replayed'));
		expect(await readCursorsUntil(reader, 'live')).toEqual(['replayed', 'live']);
	});

	it('keeps buffering live envelopes until the last overlapping replay of a scope ends', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const started = [new Deferred(), new Deferred()];
		const gates = [new Deferred<ManagerOutboxRead>(), new Deferred<ManagerOutboxRead>()];
		let reads = 0;
		const persistence = outboxReading(() => {
			const index = reads++;
			started[index]!.resolve();
			return gates[index]!.promise;
		});
		await registerStreamSubscription('t3', persistence, 'c0');
		const reader = await openStream('t3');
		await started[0]!.promise;
		await registerStreamSubscription('t3', persistence, 'c0');
		await started[1]!.promise;

		gates[0]!.resolve(caughtUp(1));
		await nextMacrotask();
		publishSharedStreamEnvelope(streamEnvelope('c9'));
		publishSharedStreamEnvelope(streamEnvelope('c10'));
		gates[1]!.resolve(caughtUp(2, 'c5', 'c9'));
		expect(await readCursorsUntil(reader, 'c10')).toEqual(['c5', 'c9', 'c10']);
	});
});

describe('shared stream sent-cursor window', () => {
	it('holds no cursor for live envelopes however many are sent', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const reader = await openStream('t4');
		await registerStreamSubscription('t4', outboxReading());
		for (let index = 0; index < 50; index += 1) {
			publishSharedStreamEnvelope(streamEnvelope(`c${index}`));
		}
		expect(await readCursorsUntil(reader, 'c49')).toHaveLength(50);
		expect(sharedStreamSentCursorCount('t4')).toBe(0);
	});

	it('records replayed cursors only, which still drop a later live copy', async () => {
		vi.useFakeTimers();
		registerSharedStreamManager({ key: 'notes' });
		const reader = await openStream('t5');
		await registerStreamSubscription(
			't5',
			outboxReading(() => caughtUp(2, 'c1', 'c2')),
			'c0'
		);
		await vi.advanceTimersByTimeAsync(0);
		expect(await readCursorsUntil(reader, 'c2')).toEqual(['c1', 'c2']);

		publishSharedStreamEnvelope(streamEnvelope('c2'));
		publishSharedStreamEnvelope(streamEnvelope('c3'));
		expect(await readCursorsUntil(reader, 'c3')).toEqual(['c3']);
		expect(sharedStreamSentCursorCount('t5')).toBe(2);
	});
});
