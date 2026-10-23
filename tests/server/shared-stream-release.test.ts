import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import type { ManagerOutboxRead } from '../../src/server/index.ts';
import {
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	releaseSharedStreamSubscriptions,
	resetSharedStreamForTests
} from '../../src/server/streamMultiplexer.ts';
import {
	connectRequest,
	openStream,
	outboxReading,
	readSyncEnvelope,
	streamEnvelope
} from '../fixtures/sharedStream.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

const scope = 'notes:workspace';
const transport = 'transport-release';

function register(holder: string, transportId = transport, afterCursor?: string, persistence = outboxReading()) {
	const url = new URL('http://sync.test/notes/connect');
	url.searchParams.set('subscription', holder);
	return registerSharedStreamSubscription({
		request: new Request(url, { headers: { 'x-sync-transport-id': transportId } }),
		managerKey: 'notes',
		scope,
		afterCursor,
		persistence,
		replayLimit: 100,
		nextCursor: () => 'reset'
	});
}

function release(...ids: string[]) {
	return releaseSharedStreamSubscriptions(connectRequest(transport), ids);
}

// A frame from the sentinel scope proves no released-scope frame preceded it, without waiting on a timer.
async function probe(reader: ReadableStreamDefaultReader<Uint8Array>) {
	await registerSharedStreamSubscription({
		request: connectRequest(transport),
		managerKey: 'notes',
		scope: 'notes:sentinel',
		persistence: outboxReading(),
		replayLimit: 100,
		nextCursor: () => 'reset'
	});
	publishSharedStreamEnvelope(streamEnvelope('released-probe', scope));
	publishSharedStreamEnvelope(streamEnvelope('sentinel', 'notes:sentinel'));
	return (await readSyncEnvelope(reader)).cursor;
}

afterEach(() => {
	resetSharedStreamForTests();
	vi.useRealTimers();
});

describe('shared stream holder release', () => {
	it('keeps a shared scope until both holders release and then runs idle cleanup after the TTL', async () => {
		vi.useFakeTimers();
		const idle = vi.fn();
		registerSharedStreamManager({ key: 'notes', onScopeIdle: idle });
		const reader = await openStream(transport);
		await register('tab-a');
		await register('tab-b');
		expect(release('tab-a').isOk()).toBe(true);
		publishSharedStreamEnvelope(streamEnvelope('still-held', scope));
		expect((await readSyncEnvelope(reader)).cursor).toBe('still-held');
		await vi.advanceTimersByTimeAsync(300_000);
		expect(idle).not.toHaveBeenCalled();
		release('tab-b');
		expect(await probe(reader)).toBe('sentinel');
		await vi.advanceTimersByTimeAsync(299_999);
		expect(idle).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1);
		expect(idle).toHaveBeenCalledExactlyOnceWith(scope);
		await reader.cancel();
	});

	it('ignores unknown holders and holders belonging to another transport', async () => {
		const reader = await openStream(transport);
		await register('owned');
		await register('other-holder', 'other-transport');
		release('unknown', 'other-holder');
		releaseSharedStreamSubscriptions(connectRequest('other-transport'), ['owned']);
		publishSharedStreamEnvelope(streamEnvelope('still-held', scope));
		expect((await readSyncEnvelope(reader)).cursor).toBe('still-held');
		release('owned');
		expect(await probe(reader)).toBe('sentinel');
		await reader.cancel();
	});

	it('re-registers the same holder without requiring multiple releases', async () => {
		const reader = await openStream(transport);
		await register('same');
		await register('same');
		release('same');
		expect(await probe(reader)).toBe('sentinel');
		await reader.cancel();
	});

	it('keeps anonymous registrations until transport teardown and clears every holder on close', async () => {
		vi.useFakeTimers();
		const idle = vi.fn();
		registerSharedStreamManager({ key: 'notes', onScopeIdle: idle });
		const reader = await openStream(transport);
		await register('');
		await register('named');
		release('', 'named');
		publishSharedStreamEnvelope(streamEnvelope('anonymous', scope));
		expect((await readSyncEnvelope(reader)).cursor).toBe('anonymous');
		await register('second');
		await reader.cancel();
		const replacement = await openStream(transport);
		expect(await probe(replacement)).toBe('sentinel');
		await vi.advanceTimersByTimeAsync(300_000);
		expect(idle).toHaveBeenCalledExactlyOnceWith(scope);
		await replacement.cancel();
	});

	it.each(['success', 'failure'])('suppresses a late replay %s after its last holder releases', async (outcome) => {
		vi.useFakeTimers();
		const replay = new Deferred<ManagerOutboxRead>();
		const errors = vi.fn();
		registerSharedStreamManager({ key: 'notes', handleError: errors });
		const reader = await openStream(transport);
		await register(
			'replaying',
			transport,
			'before',
			outboxReading(() => replay.promise)
		);
		await vi.advanceTimersByTimeAsync(0);
		publishSharedStreamEnvelope(streamEnvelope('buffered', scope));
		release('replaying');
		if (outcome === 'success') {
			replay.resolve({
				cursorFound: true,
				envelopes: [streamEnvelope('replayed', scope)],
				retainedEnvelopeCount: 1
			});
		} else {
			replay.reject(new Error('late failure'));
		}
		await vi.advanceTimersByTimeAsync(0);
		expect(await probe(reader)).toBe('sentinel');
		expect(errors).not.toHaveBeenCalled();
		await reader.cancel();
	});

	it('starts scope idle cleanup only after the last transport releases', async () => {
		vi.useFakeTimers();
		const idle = vi.fn();
		registerSharedStreamManager({ key: 'notes', onScopeIdle: idle });
		const first = await openStream(transport);
		const second = await openStream('other-transport');
		await register('first');
		await register('second', 'other-transport');
		release('first');
		await vi.advanceTimersByTimeAsync(300_000);
		expect(idle).not.toHaveBeenCalled();
		publishSharedStreamEnvelope(streamEnvelope('other-still-held', scope));
		expect((await readSyncEnvelope(second)).cursor).toBe('other-still-held');
		releaseSharedStreamSubscriptions(connectRequest('other-transport'), ['second']);
		await vi.advanceTimersByTimeAsync(300_000);
		expect(idle).toHaveBeenCalledExactlyOnceWith(scope);
		await first.cancel();
		await second.cancel();
	});

	it('keeps the latest replay cursor when a holder re-registers before its stream opens', async () => {
		const readAfter = vi.fn(() => ({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 }));
		const persistence = outboxReading(readAfter);
		await register('same', transport, 'old-cursor', persistence);
		await register('same', transport, 'latest-cursor', persistence);
		const reader = await openStream(transport);
		expect(readAfter).toHaveBeenCalledTimes(1);
		expect(readAfter.mock.calls[0]).toEqual([scope, 'latest-cursor', 101, { signal: expect.any(AbortSignal) }]);
		release('same');
		expect(await probe(reader)).toBe('sentinel');
		await reader.cancel();
	});

	it('does not let a released replay block or flush into a reopened scope', async () => {
		vi.useFakeTimers();
		const replay = new Deferred<ManagerOutboxRead>();
		await register(
			'old',
			transport,
			'before',
			outboxReading(() => replay.promise)
		);
		const reader = await openStream(transport);
		release('old');
		await register('new');
		publishSharedStreamEnvelope(streamEnvelope('new-live', scope));
		expect(await probe(reader)).toBe('new-live');
		expect((await readSyncEnvelope(reader)).cursor).toBe('released-probe');
		expect((await readSyncEnvelope(reader)).cursor).toBe('sentinel');
		replay.resolve({
			cursorFound: true,
			envelopes: [streamEnvelope('old-replay', scope)],
			retainedEnvelopeCount: 1
		});
		await vi.advanceTimersByTimeAsync(0);
		expect(await probe(reader)).toBe('released-probe');
		expect((await readSyncEnvelope(reader)).cursor).toBe('sentinel');
		await reader.cancel();
	});

	it('does not close the shared stream when its holder releases during a replay error hook', async () => {
		vi.useFakeTimers();
		const reported = new Deferred();
		const finishHook = new Deferred();
		registerSharedStreamManager({
			key: 'notes',
			handleError: () => {
				reported.resolve();
				return finishHook.promise;
			}
		});
		await register(
			'failing',
			transport,
			'before',
			outboxReading(() => Promise.reject(new Error('replay failed')))
		);
		const reader = await openStream(transport);
		await reported.promise;
		release('failing');
		finishHook.resolve();
		await vi.advanceTimersByTimeAsync(0);
		expect(await probe(reader)).toBe('sentinel');
		await reader.cancel();
	});

	it('evicts a registration whose transport never opens a stream after the transport TTL', async () => {
		vi.useFakeTimers();
		await register('orphan');
		await vi.advanceTimersByTimeAsync(180_000);
		const reader = await openStream(transport);
		expect(await probe(reader)).toBe('sentinel');
		await reader.cancel();
	});

	it('indexes transport and holder ids separately even when their colon-joined keys collide', async () => {
		const first = await openStream('t:extra');
		const second = await openStream('t');
		await register('h', 't:extra');
		await register('extra:h', 't');
		releaseSharedStreamSubscriptions(connectRequest('t:extra'), ['h']);
		await registerSharedStreamSubscription({
			request: connectRequest('t:extra'),
			managerKey: 'notes',
			scope: 'notes:sentinel',
			persistence: outboxReading(),
			replayLimit: 100,
			nextCursor: () => 'reset'
		});
		publishSharedStreamEnvelope(streamEnvelope('held-by-second', scope));
		publishSharedStreamEnvelope(streamEnvelope('sentinel', 'notes:sentinel'));
		expect((await readSyncEnvelope(first)).cursor).toBe('sentinel');
		expect((await readSyncEnvelope(second)).cursor).toBe('held-by-second');
		releaseSharedStreamSubscriptions(connectRequest('t'), ['extra:h']);
		await registerSharedStreamSubscription({
			request: connectRequest('t'),
			managerKey: 'notes',
			scope: 'notes:sentinel',
			persistence: outboxReading(),
			replayLimit: 100,
			nextCursor: () => 'reset'
		});
		publishSharedStreamEnvelope(streamEnvelope('released', scope));
		publishSharedStreamEnvelope(streamEnvelope('sentinel-2', 'notes:sentinel'));
		expect((await readSyncEnvelope(second)).cursor).toBe('sentinel-2');
		await first.cancel();
		await second.cancel();
	});

	it('requires the transport header for releases', () => {
		const result = releaseSharedStreamSubscriptions(new Request('http://sync.test/connect'), ['holder']);
		expect(result.isErr() && result.error.code).toBe('bad_request');
	});
});
