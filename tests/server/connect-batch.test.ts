import { describe, expect, it, vi } from 'vite-plus/test';

import { httpSharedSyncConnect } from '../../src/server/index.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

function batchRequest(
	body: unknown,
	init?: { readonly headers?: Record<string, string>; readonly signal?: AbortSignal }
): Request {
	return new Request('http://sync.test/api/sync-resource/connect', {
		method: 'POST',
		headers: {
			'content-type': 'application/json',
			'x-sync-transport-id': 'transport-1',
			'x-client-id': 'client-1',
			...init?.headers
		},
		body: typeof body === 'string' ? body : JSON.stringify(body),
		signal: init?.signal
	});
}

describe('httpSharedSyncConnect', () => {
	it('dispatches each connect as a header-forwarding POST and returns results in request order', async () => {
		const seen: Request[] = [];
		const bAnswered = new Deferred();
		const dispatch = vi.fn(async (request: Request) => {
			seen.push(request);
			const path = new URL(request.url).pathname;
			if (path === '/a/connect') {
				await bAnswered.promise;
				return Response.json({ isOk: true, isError: false, value: { scope: 'a' } });
			}
			bAnswered.resolve();
			return Response.json({ isOk: false, isError: true, error: { code: 'forbidden' } }, { status: 403 });
		});

		const response = await httpSharedSyncConnect(
			batchRequest(
				{ connects: ['/a/connect?after=c1', '/b/connect'] },
				{ headers: { 'content-length': '2', 'transfer-encoding': 'identity', cookie: 'session=1' } }
			),
			dispatch
		);

		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({
			results: [
				{ status: 200, body: { isOk: true, isError: false, value: { scope: 'a' } } },
				{ status: 403, body: { isOk: false, isError: true, error: { code: 'forbidden' } } }
			]
		});
		expect(seen.map((request) => [request.method, request.url])).toEqual([
			['POST', 'http://sync.test/a/connect?after=c1'],
			['POST', 'http://sync.test/b/connect']
		]);
		expect(seen[0]?.headers.get('x-sync-transport-id')).toBe('transport-1');
		expect(seen[0]?.headers.get('x-client-id')).toBe('client-1');
		expect(seen[0]?.headers.has('content-type')).toBe(false);
		expect(seen[0]?.headers.get('cookie')).toBe('session=1');
		expect(seen[0]?.headers.has('content-length')).toBe(false);
		expect(seen[0]?.headers.has('transfer-encoding')).toBe(false);
	});

	it('gives each dispatched connect the signal of the batch request', async () => {
		const abort = new AbortController();
		const seen: Request[] = [];
		await httpSharedSyncConnect(
			batchRequest({ connects: ['/a/connect', '/b/connect'] }, { signal: abort.signal }),
			async (request) => {
				seen.push(request);
				return Response.json(null);
			}
		);
		expect(seen.map((request) => request.signal.aborted)).toEqual([false, false]);
		abort.abort();
		expect(seen.map((request) => request.signal.aborted)).toEqual([true, true]);
	});

	it('turns a throwing dispatch into a 500 for that item only', async () => {
		const response = await httpSharedSyncConnect(
			batchRequest({ connects: ['/a/connect', '/b/connect'] }),
			async (request) => {
				if (new URL(request.url).pathname === '/a/connect') {
					throw new Error('boom');
				}
				return Response.json({ isOk: true, isError: false, value: {} });
			}
		);

		const { results } = (await response.json()) as { results: { status: number; body: unknown }[] };
		expect(results[0]).toMatchObject({ status: 500, body: { isError: true, error: { code: 'internal' } } });
		expect(results[1]).toEqual({ status: 200, body: { isOk: true, isError: false, value: {} } });
	});

	it.each([
		['an absolute URL', { connects: ['https://evil.test/a/connect'] }],
		['a protocol-relative URL', { connects: ['//evil.test/a/connect'] }],
		['a protocol-relative URL of the same host', { connects: ['//sync.test/a/connect'] }],
		['a backslash host', { connects: ['/\\evil.test/a/connect'] }],
		['a non-connect path', { connects: ['/a/get'] }],
		['a non-string entry', { connects: [1] }],
		['no connects', { connects: [] }],
		['connects that is not an array', { connects: '/a/connect' }],
		['more than 64 connects', { connects: Array.from({ length: 65 }, (_, index) => `/m${index}/connect`) }],
		['a body that is not JSON', 'not json']
	])('rejects %s with 400 and dispatches nothing', async (_name, body) => {
		const dispatch = vi.fn(async () => Response.json(null));
		const response = await httpSharedSyncConnect(batchRequest(body), dispatch);
		expect(response.status).toBe(400);
		expect(await response.json()).toMatchObject({ isError: true, error: { code: 'bad_request' } });
		expect(dispatch).not.toHaveBeenCalled();
	});

	it('accepts exactly 64 connects', async () => {
		const dispatch = vi.fn(async () => Response.json({ isOk: true, isError: false, value: {} }));
		const response = await httpSharedSyncConnect(
			batchRequest({ connects: Array.from({ length: 64 }, (_, index) => `/m${index}/connect`) }),
			dispatch
		);
		expect(response.status).toBe(200);
		expect(dispatch).toHaveBeenCalledTimes(64);
	});
});
