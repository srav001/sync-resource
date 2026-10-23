import { afterEach, describe, expect, it } from 'vite-plus/test';

import { httpSharedSyncRelease } from '../../src/server/index.ts';
import {
	publishSharedStreamEnvelope,
	registerSharedStreamSubscription,
	resetSharedStreamForTests
} from '../../src/server/streamMultiplexer.ts';
import {
	connectRequest,
	openStream,
	outboxReading,
	readSyncEnvelope,
	streamEnvelope
} from '../fixtures/sharedStream.ts';

function request(body: unknown, transportId: string | undefined = 'release-http') {
	const headers = new Headers({ 'content-type': 'application/json' });
	if (transportId) {
		headers.set('x-sync-transport-id', transportId);
	}
	return new Request('http://sync.test/release', {
		method: 'POST',
		headers,
		body: typeof body === 'string' ? body : JSON.stringify(body)
	});
}

afterEach(() => resetSharedStreamForTests());

describe('httpSharedSyncRelease', () => {
	it('releases a registered holder through the public HTTP route', async () => {
		const reader = await openStream('release-http');
		const register = (scope: string, holder: string) =>
			registerSharedStreamSubscription({
				request: new Request(`http://sync.test/connect?subscription=${holder}`, {
					headers: connectRequest('release-http').headers
				}),
				managerKey: 'notes',
				scope,
				persistence: outboxReading(),
				replayLimit: 100,
				nextCursor: () => 'reset'
			});
		await register('notes:released', 'holder');
		await register('notes:sentinel', 'sentinel');
		const response = await httpSharedSyncRelease(request({ releases: ['holder'] }));
		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({ isOk: true, isError: false, value: null });
		publishSharedStreamEnvelope(streamEnvelope('released', 'notes:released'));
		publishSharedStreamEnvelope(streamEnvelope('sentinel', 'notes:sentinel'));
		expect((await readSyncEnvelope(reader)).cursor).toBe('sentinel');
		await reader.cancel();
	});

	it.each([
		['missing releases', {}],
		['non-array releases', { releases: 'holder' }],
		['null releases', { releases: null }],
		['empty releases', { releases: [] }],
		['empty holder ids', { releases: [''] }],
		['non-string holder ids', { releases: [1] }],
		['more than 64 ids', { releases: Array.from({ length: 65 }, (_, i) => `holder-${i}`) }],
		['invalid JSON', 'not json']
	])('rejects %s with 400', async (_name, body) => {
		const response = await httpSharedSyncRelease(request(body));
		expect(response.status).toBe(400);
		expect(await response.json()).toMatchObject({ isError: true, error: { code: 'bad_request' } });
	});

	it('requires the transport header', async () => {
		const response = await httpSharedSyncRelease(request({ releases: ['holder'] }, ''));
		expect(response.status).toBe(400);
	});

	it('accepts exactly 64 unknown holder ids as idempotent no-ops', async () => {
		const response = await httpSharedSyncRelease(
			request({ releases: Array.from({ length: 64 }, (_, i) => `holder-${i}`) })
		);
		expect(response.status).toBe(200);
	});
});
