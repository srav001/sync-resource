import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	publishSharedStreamEnvelope,
	registerSharedStreamManager
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import {
	outboxReading,
	readSyncEnvelope,
	registerStreamSubscription,
	streamEnvelope,
	streamRequest
} from '../fixtures/sharedStream.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

const FIVE_MINUTES_MS = 5 * 60_000;

afterEach(() => {
	vi.useRealTimers();
	resetSharedStreamForTests();
});

describe('shared stream lifecycle', () => {
	it('replaces duplicate physical connections without leaking the per-transport connection count', async () => {
		registerSharedStreamManager({ key: 'notes', maxConnectionsPerIp: 2 });
		const firstResponse = await httpSharedSyncStream(streamRequest('transport-replaced'));
		expect(firstResponse.status).toBe(200);
		const firstReader = firstResponse.body!.getReader();
		expect(parseSseEvent(decode((await firstReader.read()).value)).eventName).toBe('hello');

		const secondResponse = await httpSharedSyncStream(streamRequest('transport-replaced'));
		expect(secondResponse.status).toBe(200);
		expect((await firstReader.read()).done).toBe(true);
		const secondReader = secondResponse.body!.getReader();
		expect(parseSseEvent(decode((await secondReader.read()).value)).eventName).toBe('hello');

		const thirdResponse = await httpSharedSyncStream(streamRequest('transport-replaced'));
		expect(thirdResponse.status).toBe(200);
		expect((await secondReader.read()).done).toBe(true);
		const thirdReader = thirdResponse.body!.getReader();
		expect(parseSseEvent(decode((await thirdReader.read()).value)).eventName).toBe('hello');
		await thirdReader.cancel();
	});

	it('delays scope cleanup until the idle TTL after transport cleanup', async () => {
		vi.useFakeTimers();
		const idleScopes: string[] = [];
		registerSharedStreamManager({
			key: 'notes',
			onScopeIdle(scope) {
				idleScopes.push(scope);
			}
		});
		await registerSubscription('transport-idle', 'notes:workspace-1');
		const response = await httpSharedSyncStream(streamRequest('transport-idle'));
		const reader = response.body!.getReader();
		await reader.read();
		await reader.cancel();

		await vi.advanceTimersByTimeAsync(FIVE_MINUTES_MS - 1);
		expect(idleScopes).toEqual([]);
		await vi.advanceTimersByTimeAsync(1);
		expect(idleScopes).toEqual(['notes:workspace-1']);
	});

	it('cancels stale idle cleanup when the scope reconnects and starts a fresh TTL after its next cleanup', async () => {
		vi.useFakeTimers();
		const idleScopes: string[] = [];
		registerSharedStreamManager({
			key: 'notes',
			onScopeIdle(scope) {
				idleScopes.push(scope);
			}
		});
		await registerSubscription('transport-first', 'notes:workspace-1');
		const firstResponse = await httpSharedSyncStream(streamRequest('transport-first'));
		const firstReader = firstResponse.body!.getReader();
		await firstReader.read();
		await firstReader.cancel();

		await vi.advanceTimersByTimeAsync(FIVE_MINUTES_MS - 1);
		await registerSubscription('transport-second', 'notes:workspace-1');
		await vi.advanceTimersByTimeAsync(1);
		expect(idleScopes).toEqual([]);

		const secondResponse = await httpSharedSyncStream(streamRequest('transport-second'));
		const secondReader = secondResponse.body!.getReader();
		await secondReader.read();
		await secondReader.cancel();
		await vi.advanceTimersByTimeAsync(FIVE_MINUTES_MS);
		expect(idleScopes).toEqual(['notes:workspace-1']);
	});

	it('routes idle cleanup only to the manager that owns the encoded scope', async () => {
		vi.useFakeTimers();
		const notesIdle: string[] = [];
		const tasksIdle: string[] = [];
		registerSharedStreamManager({ key: 'notes', onScopeIdle: (scope) => notesIdle.push(scope) });
		registerSharedStreamManager({ key: 'tasks', onScopeIdle: (scope) => tasksIdle.push(scope) });
		await registerSubscription('transport-owned', 'notes:workspace-1');
		const response = await httpSharedSyncStream(streamRequest('transport-owned'));
		const reader = response.body!.getReader();
		await reader.read();
		await reader.cancel();

		await vi.advanceTimersByTimeAsync(FIVE_MINUTES_MS);
		expect(notesIdle).toEqual(['notes:workspace-1']);
		expect(tasksIdle).toEqual([]);
	});

	describe('logical registrations and the physical stream of one transport id', () => {
		const scope = 'notes:workspace-1';

		async function openStream(transportId: string): Promise<ReadableStreamDefaultReader<Uint8Array>> {
			const reader = (await httpSharedSyncStream(streamRequest(transportId))).body!.getReader();
			expect(parseSseEvent(decode((await reader.read()).value)).eventName).toBe('hello');
			return reader;
		}

		// The sentinel scope is registered on the transport last, so the probe can only come first if its registration survived.
		async function receivesLiveEnvelope(
			transportId: string,
			reader: ReadableStreamDefaultReader<Uint8Array>
		): Promise<boolean> {
			await registerSubscription(transportId, 'notes:sentinel');
			publishSharedStreamEnvelope(streamEnvelope('probe', scope));
			publishSharedStreamEnvelope(streamEnvelope('sentinel', 'notes:sentinel'));
			return (await readSyncEnvelope(reader)).cursor === 'probe';
		}

		it('attaches a registration made before the stream opens', async () => {
			registerSharedStreamManager({ key: 'notes' });
			await registerSubscription('transport-pre-stream', scope);
			const reader = await openStream('transport-pre-stream');
			expect(await receivesLiveEnvelope('transport-pre-stream', reader)).toBe(true);
			await reader.cancel();
		});

		it('drops a registration made before the stream opens when it replaces an older stream', async () => {
			registerSharedStreamManager({ key: 'notes' });
			const older = await openStream('transport-replace-drop');
			await registerSubscription('transport-replace-drop', scope);
			const replacement = await openStream('transport-replace-drop');
			expect(await receivesLiveEnvelope('transport-replace-drop', replacement)).toBe(false);
			await older.cancel();
			await replacement.cancel();
		});

		it('keeps a registration made after the replacing stream opened when the older stream closes late', async () => {
			registerSharedStreamManager({ key: 'notes' });
			const older = await openStream('transport-replace-keep');
			const replacement = await openStream('transport-replace-keep');
			await registerSubscription('transport-replace-keep', scope);
			await older.cancel();
			expect(await receivesLiveEnvelope('transport-replace-keep', replacement)).toBe(true);
			await replacement.cancel();
		});
	});

	it('cancels pending replay with the physical stream and treats disconnect as normal termination', async () => {
		const replayStarted = new Deferred();
		const replayAborted = new Deferred();
		const errors: string[] = [];
		registerSharedStreamManager({
			key: 'notes',
			handleError(error) {
				errors.push(error.message);
			}
		});
		const registered = await registerStreamSubscription(
			'transport-replay-cancel',
			outboxReading(
				(_scope, _cursor, _limit, execution) =>
					new Promise((resolve) => {
						replayStarted.resolve();
						execution?.signal.addEventListener(
							'abort',
							() => {
								replayAborted.resolve();
								resolve({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 });
							},
							{ once: true }
						);
					})
			),
			'cursor-before',
			'notes:workspace-1'
		);
		expect(registered.isOk()).toBe(true);
		const response = await httpSharedSyncStream(streamRequest('transport-replay-cancel'));
		const reader = response.body!.getReader();
		await reader.read();
		await replayStarted.promise;
		await reader.cancel();
		await replayAborted.promise;

		expect(errors).toEqual([]);
	});
});

async function registerSubscription(transportId: string, scope: string): Promise<void> {
	const result = await registerStreamSubscription(transportId, outboxReading(), undefined, scope);
	expect(result.isOk()).toBe(true);
}

function decode(value: Uint8Array | undefined): string {
	return new TextDecoder().decode(value);
}
