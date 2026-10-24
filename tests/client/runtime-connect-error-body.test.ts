import { describe, expect, it } from 'vite-plus/test';

import { createRuntime } from '../../src/client/core.ts';
import { noopCache } from '../fixtures/clientRuntime.ts';

describe('direct connect error responses', () => {
	it('keeps the status-based error when the error body stream fails', async () => {
		const runtime = createRuntime({
			cache: noopCache,
			batchConnect: false,
			fetch: async (input) => {
				const path = new URL(input instanceof Request ? input.url : String(input), 'http://sync.test').pathname;
				if (path.endsWith('/stream')) {
					return new Response(new ReadableStream<Uint8Array>({ start() {} }));
				}
				return new Response(
					new ReadableStream({ start: (controller) => controller.error(new Error('body failed')) }),
					{
						status: 401
					}
				);
			}
		});
		const controller = new AbortController();
		try {
			const result = await runtime.transport.subscribe({
				connectUrl: '/notes/connect',
				managerKey: 'notes',
				scope: 'notes:ws',
				signal: controller.signal,
				onEnvelope() {}
			});
			expect(result.isErr() && result.error.code).toBe('unauthorized');
		} finally {
			controller.abort();
			runtime.dispose();
		}
	});
});
