import { afterEach, describe, expect, it } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import { getSharedStreamConfig, httpSharedSyncStream, resetSharedStreamForTests } from '../../src/server/index.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

describe('shared physical SSE stream', () => {
	it('rejects a stream without the transport id header', async () => {
		const response = await httpSharedSyncStream(new Request('http://sync.test/stream'));
		expect(response.status).toBe(400);
		expect(await response.json()).toMatchObject({ isError: true, error: { code: 'bad_request' } });
	});

	it('opens with protocol hello version 1 and registered manager keys', async () => {
		const system = createSyncTestSystem();
		const transportId = 'transport-1';
		const connected = await system.manager.http.connect?.({
			request: new Request('http://sync.test/connect', {
				method: 'POST',
				headers: { 'x-sync-transport-id': transportId }
			}),
			params: { workspaceId: 'workspace-1' }
		});
		expect(connected?.ok).toBe(true);
		const response = await httpSharedSyncStream(
			new Request('http://sync.test/stream', {
				method: 'POST',
				headers: { 'x-sync-transport-id': transportId }
			})
		);
		const reader = response.body?.getReader();
		const first = await reader?.read();
		const event = parseSseEvent(new TextDecoder().decode(first?.value));
		expect(event.eventName).toBe('hello');
		expect(JSON.parse(event.data)).toMatchObject({ type: 'hello', protocol: 1, managers: ['notes'] });
		await reader?.cancel();
	});

	it('routes a committed envelope to the registered scope', async () => {
		const system = createSyncTestSystem();
		const transportId = 'transport-routing';
		await system.manager.http.connect?.({
			request: new Request('http://sync.test/connect', {
				method: 'POST',
				headers: { 'x-sync-transport-id': transportId }
			}),
			params: { workspaceId: 'workspace-1' }
		});
		const response = await httpSharedSyncStream(
			new Request('http://sync.test/stream', {
				method: 'POST',
				headers: { 'x-sync-transport-id': transportId }
			})
		);
		const reader = response.body?.getReader();
		await reader?.read();
		await system.manager.bind({ workspaceId: 'workspace-1' }).add({ input: { id: 'shared-1', title: 'Shared' } });
		const next = await reader?.read();
		const event = parseSseEvent(new TextDecoder().decode(next?.value));
		expect(event.eventName).toBe('sync');
		expect(JSON.parse(event.data)).toMatchObject({ scope: 'notes:workspace-1', changes: [{ id: 'shared-1' }] });
		await reader?.cancel();
	});

	it('exposes the merged default stream limits', () => {
		createSyncTestSystem();
		expect(getSharedStreamConfig()).toMatchObject({
			heartbeatMs: 60_000,
			idleTtlMs: 5 * 60_000,
			maxConnectionsPerIp: 2,
			maxEventBytes: 256 * 1024
		});
	});
});
