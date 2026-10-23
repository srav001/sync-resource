import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { createRuntime } from '../../src/client/core.ts';
import { waitFor } from '../fixtures/async.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

interface BatchBody {
	readonly connects?: readonly string[];
	readonly releases?: readonly string[];
}

function confirmed() {
	return { isOk: true, isError: false, value: { scope: 'notes:server' } };
}

function harness(batchConnect = true, pending = false, releaseUrl?: string) {
	const requests: { request: Request; body: BatchBody; response: Deferred<Response> }[] = [];
	const streams: ReadableStreamDefaultController<Uint8Array>[] = [];
	const fetchImpl: typeof fetch = async (input, init) => {
		const request = new Request(input instanceof Request ? input : new URL(input, 'http://sync.test'), init);
		if (new URL(request.url).pathname.endsWith('/stream')) {
			return new Response(new ReadableStream<Uint8Array>({ start: (controller) => streams.push(controller) }));
		}
		const body: BatchBody = request.body ? ((await request.json()) as BatchBody) : {};
		const response = new Deferred<Response>();
		requests.push({ request, body, response });
		if (pending && (body.connects || new URL(request.url).pathname === '/notes/connect')) {
			return response.promise;
		}
		return Response.json({ results: (body.connects ?? []).map(() => ({ status: 200, body: confirmed() })) });
	};
	const runtime = createRuntime({ cache: noopCache, fetch: fetchImpl, batchConnect, releaseUrl });
	const subscribe = (abort = new AbortController(), scope = 'client') => ({
		abort,
		result: runtime.transport.subscribe({
			connectUrl: '/notes/connect',
			managerKey: 'notes',
			scope,
			signal: abort.signal,
			onEnvelope() {}
		})
	});
	const answer = (index = 0) => {
		const item = requests[index]!;
		item.response.resolve(
			Response.json(
				batchConnect
					? {
							results: (item.body.connects ?? []).map(() => ({ status: 200, body: confirmed() }))
						}
					: confirmed()
			)
		);
	};
	const holder = (index = 0) => {
		const item = requests[index]!;
		return new URL(item.body.connects?.[0] ?? item.request.url, 'http://sync.test').searchParams.get(
			'subscription'
		);
	};
	return { runtime, subscribe, requests, answer, holder, streams };
}

function browser(follower = false) {
	const window = Object.assign(new EventTarget(), {
		localStorage: {
			getItem(key: string) {
				if (!follower) {
					return null;
				}
				return key === 'sync-resource:transport-id'
					? 'owner-transport'
					: JSON.stringify({ ownerId: 'another-tab', expires: Date.now() + 60_000 });
			},
			setItem() {}
		}
	});
	vi.stubGlobal('window', window);
	vi.stubGlobal('navigator', {});
	vi.stubGlobal('BroadcastChannel', undefined);
	return (persisted: boolean) => window.dispatchEvent(Object.assign(new Event('pagehide'), { persisted }));
}

afterEach(() => {
	vi.useRealTimers();
	vi.unstubAllGlobals();
});

describe('runtime holder release', () => {
	it('sends one release after the last duplicate subscriber closes', async () => {
		const h = harness();
		try {
			const first = h.subscribe();
			const second = h.subscribe();
			expect((await first.result).isOk()).toBe(true);
			expect((await second.result).isOk()).toBe(true);
			expect(h.requests).toHaveLength(1);
			const id = h.holder();
			expect(id).toBeTruthy();
			first.abort.abort();
			await Promise.resolve();
			expect(h.requests).toHaveLength(1);
			second.abort.abort();
			await waitFor(() => h.requests.length === 2);
			expect(h.requests[1]?.body).toEqual({ releases: [id] });
			second.abort.abort();
			await Promise.resolve();
			expect(h.requests).toHaveLength(2);
		} finally {
			h.runtime.dispose();
		}
	});

	it.each(['response', 'failure'])(
		'waits for an in-flight batch network %s before releasing a closed entry',
		async (outcome) => {
			const h = harness(true, true);
			try {
				const subscription = h.subscribe();
				await waitFor(() => h.requests.length === 1);
				subscription.abort.abort();
				expect((await subscription.result).isErr()).toBe(true);
				expect(h.requests[0]?.request.signal.aborted).toBe(false);
				expect(h.requests).toHaveLength(1);
				if (outcome === 'response') {
					h.answer();
				} else {
					h.requests[0]?.response.reject(new Error('offline'));
				}
				await waitFor(() => h.requests.length === 2);
				expect(h.requests[1]?.body).toEqual({ releases: [h.holder()] });
			} finally {
				h.runtime.dispose();
			}
		}
	);

	it('sends neither a connect nor a release when closed before the held flush', async () => {
		vi.useFakeTimers();
		const h = harness();
		try {
			const releaseHold = h.runtime.transport.holdRegistrations?.();
			const subscription = h.subscribe();
			await vi.advanceTimersByTimeAsync(1_999);
			subscription.abort.abort();
			expect((await subscription.result).isErr()).toBe(true);
			releaseHold?.();
			await vi.advanceTimersByTimeAsync(2_000);
			expect(h.requests).toEqual([]);
		} finally {
			h.runtime.dispose();
		}
	});

	it('gives a reopened key a new holder and releases the old id separately in the same flush', async () => {
		vi.useFakeTimers();
		const h = harness();
		try {
			const first = h.subscribe();
			await first.result;
			const oldId = h.holder();
			const releaseHold = h.runtime.transport.holdRegistrations?.();
			first.abort.abort();
			const reopened = h.subscribe();
			releaseHold?.();
			await vi.advanceTimersByTimeAsync(0);
			expect((await reopened.result).isOk()).toBe(true);
			expect(h.requests).toHaveLength(3);
			expect(h.holder(1)).not.toBe(oldId);
			expect(h.requests[1]?.body.releases).toBeUndefined();
			expect(h.requests[2]?.body).toEqual({ releases: [oldId] });
			expect(new URL(h.requests[2]!.request.url).pathname).toBe('/api/sync-resource/release');
		} finally {
			h.runtime.dispose();
		}
	});

	it('does not abort a direct connect and posts its release to the configured releaseUrl after the response', async () => {
		const h = harness(false, true, '/internal/release');
		try {
			const subscription = h.subscribe();
			await waitFor(() => h.requests.length === 1);
			subscription.abort.abort();
			expect((await subscription.result).isErr()).toBe(true);
			expect(h.requests[0]?.request.signal.aborted).toBe(false);
			expect(h.requests).toHaveLength(1);
			h.answer();
			await waitFor(() => h.requests.length === 2);
			expect(new URL(h.requests[1]!.request.url).pathname).toBe('/internal/release');
			expect(h.requests[1]?.body).toEqual({ releases: [h.holder()] });
		} finally {
			h.runtime.dispose();
		}
	});

	it('waits for every overlapping registration request before releasing its holder', async () => {
		vi.useFakeTimers();
		const h = harness(true, true);
		try {
			const subscription = h.subscribe();
			await waitFor(() => h.requests.length === 1);
			h.streams[0]?.close();
			await vi.advanceTimersByTimeAsync(1_300);
			expect(h.requests).toHaveLength(2);
			expect(h.holder(1)).toBe(h.holder());
			subscription.abort.abort();
			await subscription.result;
			h.answer(1);
			await vi.advanceTimersByTimeAsync(0);
			expect(h.requests).toHaveLength(2);
			h.answer(0);
			await waitFor(() => h.requests.length === 3);
			expect(h.requests[2]?.body).toEqual({ releases: [h.holder()] });
		} finally {
			h.runtime.dispose();
		}
	});

	it('flushes queued releases and all held ids in one immediate keepalive on non-persisted pagehide', async () => {
		vi.useFakeTimers();
		const pagehide = browser();
		const h = harness();
		try {
			const first = h.subscribe();
			const second = h.subscribe(new AbortController(), 'another-scope');
			await Promise.all([first.result, second.result]);
			const ids = h.requests[0]!.body.connects!.map((url) =>
				new URL(url, 'http://sync.test').searchParams.get('subscription')
			);
			h.runtime.transport.holdRegistrations?.();
			first.abort.abort();
			pagehide(true);
			expect(h.requests).toHaveLength(1);
			pagehide(false);
			await waitFor(() => h.requests.length === 2);
			expect(h.requests[1]?.request.keepalive).toBe(true);
			expect(h.requests[1]?.body).toEqual({ releases: ids });
			await vi.advanceTimersByTimeAsync(2_000);
			expect(h.requests).toHaveLength(2);
		} finally {
			h.runtime.dispose();
		}
	});

	it('releases on dispose and removes the pagehide listener without a duplicate release', async () => {
		const pagehide = browser();
		const h = harness();
		await h.subscribe().result;
		h.runtime.dispose();
		pagehide(false);
		await waitFor(() => h.requests.length === 2);
		expect(h.requests[1]?.body).toEqual({ releases: [h.holder()] });
		expect(h.requests[1]?.request.keepalive).toBe(true);
		h.runtime.dispose();
		await Promise.resolve();
		expect(h.requests).toHaveLength(2);
	});

	it('chunks more than 64 queued releases into valid batches', async () => {
		const h = harness();
		try {
			const subscriptions = Array.from({ length: 150 }, (_, i) =>
				h.subscribe(new AbortController(), `scope-${i}`)
			);
			await Promise.all(subscriptions.map((item) => item.result));
			expect(h.requests).toHaveLength(3);
			for (const item of subscriptions) {
				item.abort.abort();
			}
			await waitFor(() => h.requests.length === 6);
			expect(h.requests.slice(3).map((item) => item.body.releases?.length)).toEqual([64, 64, 22]);
			expect(new Set(h.requests.slice(3).flatMap((item) => item.body.releases ?? [])).size).toBe(150);
		} finally {
			h.runtime.dispose();
		}
	});
	it('chunks pagehide keepalive releases to the server limit', async () => {
		const pagehide = browser();
		const h = harness();
		try {
			await Promise.all(
				Array.from({ length: 65 }, (_, i) => h.subscribe(new AbortController(), `scope-${i}`).result)
			);
			pagehide(false);
			await waitFor(() => h.requests.length === 4);
			expect(h.requests.slice(2).map((item) => item.body.releases?.length)).toEqual([64, 1]);
			expect(h.requests.slice(2).every((item) => item.request.keepalive)).toBe(true);
		} finally {
			h.runtime.dispose();
		}
	});
	it('releases again after an in-flight connect settles following disposal', async () => {
		const h = harness(true, true);
		const subscription = h.subscribe();
		await waitFor(() => h.requests.length === 1);
		h.runtime.dispose();
		expect((await subscription.result).isErr()).toBe(true);
		await waitFor(() => h.requests.length === 2);
		expect(h.requests[1]!.request.keepalive).toBe(true);
		h.answer(0);
		await waitFor(() => h.requests.length === 3);
		expect(h.requests[2]!.body).toEqual({ releases: [h.holder()] });
		expect(h.requests[2]!.request.keepalive).toBe(true);
	});
	it('includes a retired follower holder in pagehide releases while its connect is in flight', async () => {
		const pagehide = browser(true);
		const h = harness(true, true);
		try {
			const subscription = h.subscribe();
			await waitFor(() => h.requests.length === 1);
			expect(h.streams).toEqual([]);
			subscription.abort.abort();
			expect((await subscription.result).isErr()).toBe(true);
			expect(h.requests).toHaveLength(1);
			pagehide(false);
			await waitFor(() => h.requests.length === 2);
			expect(h.requests[1]!.body).toEqual({ releases: [h.holder()] });
			expect(h.requests[1]!.request.keepalive).toBe(true);
			expect(h.requests[1]!.request.headers.get('x-sync-transport-id')).toBe('owner-transport');
			expect(new URL(h.requests[1]!.request.url).pathname).toBe('/api/sync-resource/release');
			h.answer(0);
			await waitFor(() => h.requests.length === 3);
			pagehide(false);
			await Promise.resolve();
			expect(h.requests).toHaveLength(3);
		} finally {
			h.runtime.dispose();
		}
	});
	it('keeps the exit listener until a last closed holder has left the held release queue', async () => {
		const pagehide = browser(true);
		const h = harness();
		try {
			const subscription = h.subscribe();
			await subscription.result;
			h.runtime.transport.holdRegistrations?.();
			subscription.abort.abort();
			pagehide(false);
			await waitFor(() => h.requests.length === 2);
			expect(h.requests[1]!.body).toEqual({ releases: [h.holder()] });
			expect(h.requests[1]!.request.keepalive).toBe(true);
		} finally {
			h.runtime.dispose();
		}
	});
});
