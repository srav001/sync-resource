import { afterEach, expect, it } from 'vite-plus/test';

import { configureSync, createStore, resetSyncConfiguration } from '../../src/client/core.ts';
import { httpSharedSyncConnect, httpSharedSyncRelease, httpSharedSyncStream } from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import { nextMacrotask, waitFor } from '../fixtures/async.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	resetSyncConfiguration();
	resetSharedStreamForTests();
});

it.each([false, true])(
	'releases the last store holder while another scope keeps streaming (namespaced middleware: %s)',
	async (namespaced) => {
		const system = createSyncTestSystem();
		const deliveredScopes: string[] = [];
		let releases = 0;
		let connects = 0;
		let streams = 0;
		const transportIds: string[] = [];
		async function dispatch(request: Request): Promise<Response> {
			const url = new URL(request.url);
			// The batch request is not namespaced: its items re-enter this pipeline individually.
			if (url.pathname === '/api/sync-resource/connect') {
				return httpSharedSyncConnect(request, dispatch);
			}
			const transportId = request.headers.get('x-sync-transport-id');
			if (transportId) {
				const scopedId = namespaced ? `user:alice:${transportId}` : transportId;
				request.headers.set('x-sync-transport-id', scopedId);
				transportIds.push(scopedId);
			}
			if (url.pathname === '/api/sync-resource/stream') {
				streams += 1;
				const response = await httpSharedSyncStream(request);
				return new Response(
					response.body!.pipeThrough(
						new TransformStream<Uint8Array, Uint8Array>({
							transform(chunk, controller) {
								const event = parseSseEvent(new TextDecoder().decode(chunk));
								if (event.eventName === 'sync') {
									const value = JSON.parse(event.data) as { scope: string };
									deliveredScopes.push(value.scope);
								}
								controller.enqueue(chunk);
							}
						})
					),
					{ headers: response.headers }
				);
			}
			if (url.pathname === '/api/sync-resource/release') {
				const body = (await request.clone().json()) as { releases: string[] };
				const response = await httpSharedSyncRelease(request);
				expect(response.status).toBe(200);
				releases += body.releases.length;
				return response;
			}
			const workspaceId = url.pathname.split('/')[2]!;
			const params = { workspaceId };
			if (url.pathname.endsWith('/connect')) {
				connects += 1;
				return system.manager.http.connect({ request, params });
			}
			return system.manager.http.list({ request, params });
		}
		configureSync({
			cache: { adapter: noopCache },
			fetch: async (input, init) =>
				dispatch(new Request(input instanceof Request ? input : new URL(input, 'http://sync.test'), init))
		});
		const store = (workspaceId: string) =>
			createStore<typeof system.manager.type>({
				key: 'notes',
				params: { workspaceId },
				getUrl: (params) => `/workspaces/${params.workspaceId}/notes`,
				query: () => ({ limit: 20 })
			});
		const first = store('released');
		const duplicate = store('released');
		const sentinel = store('sentinel');
		try {
			expect((await first.connect()).isOk()).toBe(true);
			expect((await duplicate.connect()).isOk()).toBe(true);
			expect((await sentinel.connect()).isOk()).toBe(true);
			expect(connects).toBe(2);
			const bound = system.manager.bind({ workspaceId: 'released' });
			await bound.add({ input: { id: 'before-release', title: 'Visible' } });
			await waitFor(() => deliveredScopes.length === 1);
			first.dispose();
			await nextMacrotask();
			expect(releases).toBe(0);
			duplicate.dispose();
			await waitFor(() => releases === 1);
			await bound.add({ input: { id: 'after-release', title: 'Not streamed' } });
			await system.manager
				.bind({ workspaceId: 'sentinel' })
				.add({ input: { id: 'sentinel', title: 'Still connected' } });
			await waitFor(() => deliveredScopes.length === 2);
			expect(deliveredScopes).toEqual(['notes:released', 'notes:sentinel']);
			expect(streams).toBe(1);
			expect(new Set(transportIds).size).toBe(1);
			expect(transportIds[0]).toMatch(namespaced ? /^user:alice:transport_/ : /^transport_/);
		} finally {
			first.dispose();
			duplicate.dispose();
			sentinel.dispose();
		}
	}
);
