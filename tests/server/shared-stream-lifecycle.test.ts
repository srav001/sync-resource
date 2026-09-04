import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	type ManagerSyncPersistence
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';

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

	it('cancels pending replay with the physical stream and treats disconnect as normal termination', async () => {
		let replayStarted!: () => void;
		const didStartReplay = new Promise<void>((resolve) => {
			replayStarted = resolve;
		});
		let replayFinalized = false;
		const errors: string[] = [];
		const replayPersistence: ManagerSyncPersistence = {
			...persistence,
			readAfter(_scope, _cursor, _limit, execution) {
				return new Promise((resolve) => {
					replayStarted();
					execution?.signal.addEventListener(
						'abort',
						() => {
							replayFinalized = true;
							resolve({ cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 });
						},
						{ once: true }
					);
				});
			}
		};
		registerSharedStreamManager({
			key: 'notes',
			handleError(error) {
				errors.push(error.message);
			}
		});
		const registered = await registerSharedStreamSubscription({
			request: new Request('http://sync.test/connect', {
				method: 'POST',
				headers: { 'x-sync-transport-id': 'transport-replay-cancel' }
			}),
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			afterCursor: 'cursor-before',
			replayLimit: 100,
			persistence: replayPersistence,
			nextCursor: () => 'cursor-reset'
		});
		expect(registered.isOk()).toBe(true);
		const response = await httpSharedSyncStream(streamRequest('transport-replay-cancel'));
		const reader = response.body!.getReader();
		await reader.read();
		await didStartReplay;
		await reader.cancel();
		for (let index = 0; index < 10 && !replayFinalized; index += 1) {
			await Promise.resolve();
		}

		expect(replayFinalized).toBe(true);
		expect(errors).toEqual([]);
	});
});

const persistence: ManagerSyncPersistence = {
	async append() {},
	async readAfter() {
		return {
			cursorFound: true,
			retainedEnvelopeCount: 0,
			envelopes: []
		};
	},
	async readMutation() {
		return undefined;
	},
	async recordMutation() {}
};

async function registerSubscription(transportId: string, scope: string): Promise<void> {
	const result = await registerSharedStreamSubscription({
		request: new Request('http://sync.test/connect', {
			method: 'POST',
			headers: { 'x-sync-transport-id': transportId }
		}),
		managerKey: 'notes',
		scope,
		replayLimit: 100,
		persistence,
		nextCursor: () => 'cursor-reset'
	});
	expect(result.isOk()).toBe(true);
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

function decode(value: Uint8Array | undefined): string {
	return new TextDecoder().decode(value);
}
